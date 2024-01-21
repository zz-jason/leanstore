#include "Config.hpp"
#include "KVInterface.hpp"
#include "LeanStore.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "concurrency-recovery/Transaction.hpp"
#include "concurrency-recovery/Worker.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "utils/Defer.hpp"
#include "utils/RandomGenerator.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <unordered_map>

using namespace leanstore::utils;
using namespace leanstore::storage::btree;

namespace leanstore::test {

static Slice ToSlice(const std::string& src) {
  return Slice((const u8*)src.data(), src.size());
}

class LongRunningTxTest : public ::testing::Test {
protected:
  std::string mTreeName;
  TransactionKV* mKv;

protected:
  LongRunningTxTest() = default;

  ~LongRunningTxTest() = default;

  void SetUp() override {
    // init the leanstore
    auto* leanstore = GetLeanStore();

    mTreeName = RandomGenerator::RandAlphString(10);
    auto config = BTreeGeneric::Config{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create a btree for test
    cr::CRManager::sInstance->ScheduleJobSync(0, [&]() {
      cr::Worker::my().StartTx();
      SCOPED_DEFER(cr::Worker::my().CommitTx());
      leanstore->RegisterTransactionKV(mTreeName, config, &mKv);
      ASSERT_NE(mKv, nullptr);
    });

    // do extra insert and remove transactions in worker 0 to make it have more
    // than one entries in the commit log, which helps to advance the global
    // lower watermarks for garbage collection
    cr::CRManager::sInstance->ScheduleJobSync(0, [&]() {
      cr::Worker::my().StartTx();
      ASSERT_EQ(mKv->Insert(ToSlice("0"), ToSlice("0")), OpCode::kOK);
      cr::Worker::my().CommitTx();

      cr::Worker::my().StartTx();
      ASSERT_EQ(mKv->Remove(ToSlice("0")), OpCode::kOK);
      cr::Worker::my().CommitTx();
    });
  }

  void TearDown() override {
    TXID lastTxId = 0;
    cr::CRManager::sInstance->ScheduleJobSync(0, [&]() {
      cr::Worker::my().StartTx();
      SCOPED_DEFER(cr::Worker::my().CommitTx());
      lastTxId = cr::Worker::my().mActiveTx.mStartTs;
      GetLeanStore()->UnRegisterTransactionKV(mTreeName);
    });
  }

public:
  inline static auto CreateLeanStore() {
    FLAGS_enable_eager_garbage_collection = true;
    FLAGS_worker_threads = 3;
    FLAGS_recover = false;
    FLAGS_data_dir = "/tmp/MVCCTest";

    std::filesystem::path dirPath = FLAGS_data_dir;
    std::filesystem::remove_all(dirPath);
    std::filesystem::create_directories(dirPath);
    return std::make_unique<leanstore::LeanStore>();
  }

  inline static leanstore::LeanStore* GetLeanStore() {
    static auto sLeanStore = CreateLeanStore();
    return sLeanStore.get();
  }
};

// TODO(lookup from graveyard)
TEST_F(LongRunningTxTest, LookupFromGraveyard) {
  std::string key1("1"), val1("10");
  std::string key2("2"), val2("20");
  std::string res;

  std::string copiedVal;
  auto copyValue = [&](Slice val) {
    copiedVal = std::string((const char*)val.data(), val.size());
  };

  // Insert 2 key-values as the test base.
  cr::CRManager::sInstance->ScheduleJobSync(1, [&]() {
    cr::Worker::my().StartTx();
    EXPECT_EQ(mKv->Insert(ToSlice(key1), ToSlice(val1)), OpCode::kOK);
    cr::Worker::my().CommitTx();

    cr::Worker::my().StartTx();
    EXPECT_EQ(mKv->Insert(ToSlice(key2), ToSlice(val2)), OpCode::kOK);
    cr::Worker::my().CommitTx();
  });

  cr::CRManager::sInstance->ScheduleJobSync(
      1, [&]() { cr::Worker::my().StartTx(); });

  cr::CRManager::sInstance->ScheduleJobSync(2, [&]() {
    cr::Worker::my().StartTx(TxMode::kLongRunning);

    // got the old value in worker 2
    EXPECT_EQ(mKv->Lookup(ToSlice(key1), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val1);

    EXPECT_EQ(mKv->Lookup(ToSlice(key2), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);
  });

  // remove the key in worker 1
  cr::CRManager::sInstance->ScheduleJobSync(1, [&]() {
    EXPECT_EQ(mKv->Remove(ToSlice(key1)), OpCode::kOK);
    EXPECT_EQ(mKv->Remove(ToSlice(key2)), OpCode::kOK);
  });

  // got the old value in worker 2
  cr::CRManager::sInstance->ScheduleJobAsync(2, [&]() {
    EXPECT_EQ(mKv->Lookup(ToSlice(key1), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val1);

    EXPECT_EQ(mKv->Lookup(ToSlice(key2), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);
  });

  // commit the transaction in worker 1, after garbage collection when
  // committing the transaction, tombstones should be moved to the graveyard.
  cr::CRManager::sInstance->ScheduleJobSync(1, [&]() {
    cr::Worker::my().CommitTx();
    EXPECT_EQ(mKv->mGraveyard->CountEntries(), 2u);
  });

  // lookup from graveyard, still got the old value in worker 2
  cr::CRManager::sInstance->ScheduleJobSync(2, [&]() {
    EXPECT_EQ(mKv->Lookup(ToSlice(key1), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val1);

    EXPECT_EQ(mKv->Lookup(ToSlice(key2), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);

    // commit the transaction in worker 2
    cr::Worker::my().CommitTx();
  });

  // now worker 2 can not get the old value
  cr::CRManager::sInstance->ScheduleJobSync(2, [&]() {
    cr::Worker::my().StartTx(TxMode::kLongRunning,
                             IsolationLevel::kSnapshotIsolation, false);
    SCOPED_DEFER(cr::Worker::my().CommitTx());

    EXPECT_EQ(mKv->Lookup(ToSlice(key1), copyValue), OpCode::kNotFound);
    EXPECT_EQ(mKv->Lookup(ToSlice(key2), copyValue), OpCode::kNotFound);
  });
}

TEST_F(LongRunningTxTest, ScanAsc) {
  // randomly generate 100 unique key-values for s1 to insert
  size_t numKV = 100;
  std::unordered_map<std::string, std::string> kvToTest;
  std::string smallestKey;
  for (size_t i = 0; i < numKV; ++i) {
    std::string key = RandomGenerator::RandAlphString(10);
    std::string val = RandomGenerator::RandAlphString(10);
    if (kvToTest.find(key) != kvToTest.end()) {
      --i;
      continue;
    }

    // update the smallest key
    kvToTest[key] = val;
    if (smallestKey.empty() || smallestKey > key) {
      smallestKey = key;
    }
  }

  // insert the key-values in worker 0
  cr::CRManager::sInstance->ScheduleJobSync(0, [&]() {
    for (const auto& [key, val] : kvToTest) {
      cr::Worker::my().StartTx();
      SCOPED_DEFER(cr::Worker::my().CommitTx());
      EXPECT_EQ(mKv->Insert(ToSlice(key), ToSlice(val)), OpCode::kOK);
    }
  });

  // start transaction on worker 2, got the inserted values
  std::string copiedKey, copiedVal;
  auto copyKeyVal = [&](Slice key, Slice val) {
    copiedKey = std::string((const char*)key.data(), key.size());
    copiedVal = std::string((const char*)val.data(), val.size());
    EXPECT_EQ(copiedVal, kvToTest[copiedKey]);
    return true;
  };
  cr::CRManager::sInstance->ScheduleJobSync(2, [&]() {
    cr::Worker::my().StartTx(TxMode::kLongRunning,
                             IsolationLevel::kSnapshotIsolation, false);
    EXPECT_EQ(mKv->ScanAsc(ToSlice(smallestKey), copyKeyVal), OpCode::kOK);
  });

  // remove the key-values in worker 1
  cr::CRManager::sInstance->ScheduleJobSync(1, [&]() {
    cr::Worker::my().StartTx();
    for (const auto& [key, val] : kvToTest) {
      EXPECT_EQ(mKv->Remove(ToSlice(key)), OpCode::kOK);
    }
  });

  // got the old values in worker 2
  cr::CRManager::sInstance->ScheduleJobSync(2, [&]() {
    EXPECT_EQ(mKv->ScanAsc(ToSlice(smallestKey), copyKeyVal), OpCode::kOK);
  });

  // commit the transaction in worker 1
  cr::CRManager::sInstance->ScheduleJobSync(
      1, [&]() { cr::Worker::my().CommitTx(); });

  // still got the old values in worker 2
  cr::CRManager::sInstance->ScheduleJobSync(2, [&]() {
    EXPECT_EQ(mKv->ScanAsc(ToSlice(smallestKey), copyKeyVal), OpCode::kOK);

    // commit the transaction in worker 2
    cr::Worker::my().CommitTx();
  });

  // now worker 2 can not get the old values
  cr::CRManager::sInstance->ScheduleJobSync(2, [&]() {
    cr::Worker::my().StartTx(TxMode::kLongRunning,
                             IsolationLevel::kSnapshotIsolation, false);
    SCOPED_DEFER(cr::Worker::my().CommitTx());
    EXPECT_EQ(mKv->ScanAsc(ToSlice(smallestKey), copyKeyVal), OpCode::kOK);
  });
}

TEST_F(LongRunningTxTest, ScanAscFromGraveyard) {
  // randomly generate 100 unique key-values for s1 to insert
  size_t numKV = 100;
  std::unordered_map<std::string, std::string> kvToTest;
  std::string smallestKey;
  for (size_t i = 0; i < numKV; ++i) {
    std::string key = RandomGenerator::RandAlphString(10);
    std::string val = RandomGenerator::RandAlphString(10);
    if (kvToTest.find(key) != kvToTest.end()) {
      --i;
      continue;
    }

    // update the smallest key
    kvToTest[key] = val;
    if (smallestKey.empty() || smallestKey > key) {
      smallestKey = key;
    }
  }

  // insert the key-values in worker 0
  cr::CRManager::sInstance->ScheduleJobSync(0, [&]() {
    for (const auto& [key, val] : kvToTest) {
      cr::Worker::my().StartTx();
      SCOPED_DEFER(cr::Worker::my().CommitTx());
      EXPECT_EQ(mKv->Insert(ToSlice(key), ToSlice(val)), OpCode::kOK);
    }
  });

  // start transaction on worker 2, got the inserted values
  std::string copiedKey, copiedVal;
  auto copyKeyVal = [&](Slice key, Slice val) {
    copiedKey = std::string((const char*)key.data(), key.size());
    copiedVal = std::string((const char*)val.data(), val.size());
    EXPECT_EQ(copiedVal, kvToTest[copiedKey]);
    return true;
  };
  cr::CRManager::sInstance->ScheduleJobSync(2, [&]() {
    cr::Worker::my().StartTx(TxMode::kLongRunning,
                             IsolationLevel::kSnapshotIsolation, false);
    EXPECT_EQ(mKv->ScanAsc(ToSlice(smallestKey), copyKeyVal), OpCode::kOK);
  });

  // remove the key-values in worker 1 in several transactions, so that the
  // old tombstones are moved to graveyard
  cr::CRManager::sInstance->ScheduleJobSync(1, [&]() {
    for (const auto& [key, val] : kvToTest) {
      cr::Worker::my().StartTx();
      EXPECT_EQ(mKv->Remove(ToSlice(key)), OpCode::kOK);
      cr::Worker::my().CommitTx();
    }
  });

  // verify the two watermarks
  EXPECT_TRUE(cr::Worker::sGlobalOldestTxId <
              cr::Worker::sGlobalOldestShortTxId);
  EXPECT_TRUE(cr::Worker::sGlobalWmkOfAllTx < cr::Worker::sGlobalWmkOfShortTx);

  // scan and got the old values in worker 2's long-running transaction
  cr::CRManager::sInstance->ScheduleJobSync(2, [&]() {
    EXPECT_EQ(mKv->ScanAsc(ToSlice(smallestKey), copyKeyVal), OpCode::kOK);
  });

  // commit the long-running transaction in worker 2
  cr::CRManager::sInstance->ScheduleJobSync(
      2, [&]() { cr::Worker::my().CommitTx(); });

  // now worker 2 can not get the old values
  cr::CRManager::sInstance->ScheduleJobSync(2, [&]() {
    cr::Worker::my().StartTx(TxMode::kLongRunning,
                             IsolationLevel::kSnapshotIsolation, false);
    SCOPED_DEFER(cr::Worker::my().CommitTx());
    EXPECT_EQ(mKv->ScanAsc(ToSlice(smallestKey), copyKeyVal), OpCode::kOK);
  });
}

} // namespace leanstore::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}