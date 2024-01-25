#include "Config.hpp"
#include "KVInterface.hpp"
#include "LeanStore.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "concurrency-recovery/HistoryTree.hpp"
#include "concurrency-recovery/Transaction.hpp"
#include "concurrency-recovery/Worker.hpp"
#include "storage/btree/BasicKV.hpp"
#include "storage/btree/TransactionKV.hpp"
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
    auto config = BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // Worker 0, create a btree for test
    GetLeanStore()->mCRManager->ScheduleJobSync(0, [&]() {
      cr::Worker::My().StartTx();
      SCOPED_DEFER(cr::Worker::My().CommitTx());
      leanstore->RegisterTransactionKV(mTreeName, config, &mKv);
      ASSERT_NE(mKv, nullptr);
    });

    // Worker 0, do extra insert and remove transactions in worker 0 to make it
    // have more than one entries in the commit log, which helps to advance the
    // global lower watermarks for garbage collection
    GetLeanStore()->mCRManager->ScheduleJobSync(0, [&]() {
      cr::Worker::My().StartTx();
      ASSERT_EQ(mKv->Insert(ToSlice("0"), ToSlice("0")), OpCode::kOK);
      cr::Worker::My().CommitTx();

      cr::Worker::My().StartTx();
      ASSERT_EQ(mKv->Remove(ToSlice("0")), OpCode::kOK);
      cr::Worker::My().CommitTx();
    });
  }

  void TearDown() override {
    // Worker 0, remove the btree
    GetLeanStore()->mCRManager->ScheduleJobSync(0, [&]() {
      cr::Worker::My().StartTx();
      SCOPED_DEFER(cr::Worker::My().CommitTx());
      GetLeanStore()->UnRegisterTransactionKV(mTreeName);
    });
  }

public:
  inline static auto CreateLeanStore() {
    FLAGS_enable_eager_garbage_collection = true;
    FLAGS_worker_threads = 3;
    FLAGS_init = true;
    FLAGS_data_dir = "/tmp/MVCCTest";
    FLAGS_alsologtostderr = 1;
    FLAGS_colorlogtostderr = 1;

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
  GetLeanStore()->mCRManager->ScheduleJobSync(1, [&]() {
    cr::Worker::My().StartTx();
    EXPECT_EQ(mKv->Insert(ToSlice(key1), ToSlice(val1)), OpCode::kOK);
    cr::Worker::My().CommitTx();

    cr::Worker::My().StartTx();
    EXPECT_EQ(mKv->Insert(ToSlice(key2), ToSlice(val2)), OpCode::kOK);
    cr::Worker::My().CommitTx();
  });

  GetLeanStore()->mCRManager->ScheduleJobSync(
      1, [&]() { cr::Worker::My().StartTx(); });

  GetLeanStore()->mCRManager->ScheduleJobSync(2, [&]() {
    cr::Worker::My().StartTx(TxMode::kLongRunning);

    // get the old value in worker 2
    EXPECT_EQ(mKv->Lookup(ToSlice(key1), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val1);

    EXPECT_EQ(mKv->Lookup(ToSlice(key2), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);
  });

  // remove the key in worker 1
  GetLeanStore()->mCRManager->ScheduleJobSync(1, [&]() {
    EXPECT_EQ(mKv->Remove(ToSlice(key1)), OpCode::kOK);
    EXPECT_EQ(mKv->Remove(ToSlice(key2)), OpCode::kOK);
  });

  // get the old value in worker 2
  GetLeanStore()->mCRManager->ScheduleJobAsync(2, [&]() {
    EXPECT_EQ(mKv->Lookup(ToSlice(key1), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val1);

    EXPECT_EQ(mKv->Lookup(ToSlice(key2), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);
  });

  // commit the transaction in worker 1, after garbage collection when
  // committing the transaction, tombstones should be moved to the graveyard.
  GetLeanStore()->mCRManager->ScheduleJobSync(1, [&]() {
    cr::Worker::My().CommitTx();
    EXPECT_EQ(mKv->mGraveyard->CountEntries(), 2u);
  });

  // lookup from graveyard, still get the old value in worker 2
  GetLeanStore()->mCRManager->ScheduleJobSync(2, [&]() {
    EXPECT_EQ(mKv->Lookup(ToSlice(key1), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val1);

    EXPECT_EQ(mKv->Lookup(ToSlice(key2), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);

    // commit the transaction in worker 2
    cr::Worker::My().CommitTx();
  });

  // now worker 2 can not get the old value
  GetLeanStore()->mCRManager->ScheduleJobSync(2, [&]() {
    cr::Worker::My().StartTx(TxMode::kLongRunning,
                             IsolationLevel::kSnapshotIsolation, false);
    SCOPED_DEFER(cr::Worker::My().CommitTx());

    EXPECT_EQ(mKv->Lookup(ToSlice(key1), copyValue), OpCode::kNotFound);
    EXPECT_EQ(mKv->Lookup(ToSlice(key2), copyValue), OpCode::kNotFound);
  });
}

TEST_F(LongRunningTxTest, LookupAfterUpdate100Times) {
  std::string key1("1"), val1("10");
  std::string key2("2"), val2("20");
  std::string res;

  std::string copiedVal;
  auto copyValue = [&](Slice val) {
    copiedVal = std::string((const char*)val.data(), val.size());
  };

  // Work 1, insert 2 key-values as the test base
  GetLeanStore()->mCRManager->ScheduleJobSync(1, [&]() {
    cr::Worker::My().StartTx();
    EXPECT_EQ(mKv->Insert(ToSlice(key1), ToSlice(val1)), OpCode::kOK);
    cr::Worker::My().CommitTx();

    cr::Worker::My().StartTx();
    EXPECT_EQ(mKv->Insert(ToSlice(key2), ToSlice(val2)), OpCode::kOK);
    cr::Worker::My().CommitTx();
  });

  // Worker 1, start a short-running transaction
  GetLeanStore()->mCRManager->ScheduleJobSync(
      1, [&]() { cr::Worker::My().StartTx(); });

  // Worker 2, start a long-running transaction, lookup, get the old value
  GetLeanStore()->mCRManager->ScheduleJobSync(2, [&]() {
    cr::Worker::My().StartTx(TxMode::kLongRunning);

    EXPECT_EQ(mKv->Lookup(ToSlice(key1), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val1);

    EXPECT_EQ(mKv->Lookup(ToSlice(key2), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);
  });

  // Worker 1, update key1 100 times with random values
  std::string newVal;
  GetLeanStore()->mCRManager->ScheduleJobSync(1, [&]() {
    auto updateDescBufSize = UpdateDesc::Size(1);
    u8 updateDescBuf[updateDescBufSize];
    auto* updateDesc = UpdateDesc::CreateFrom(updateDescBuf);
    updateDesc->mNumSlots = 1;
    updateDesc->mUpdateSlots[0].mOffset = 0;
    updateDesc->mUpdateSlots[0].mSize = val1.size();

    auto updateCallBack = [&](MutableSlice toUpdate) {
      auto newValSize = updateDesc->mUpdateSlots[0].mSize;
      newVal = RandomGenerator::RandAlphString(newValSize);
      std::memcpy(toUpdate.Data(), newVal.data(), newVal.size());
    };

    for (size_t i = 0; i < 100; ++i) {
      EXPECT_EQ(mKv->UpdatePartial(ToSlice(key1), updateCallBack, *updateDesc),
                OpCode::kOK);
    }
  });

  // Worker 2, lookup, get the old value
  GetLeanStore()->mCRManager->ScheduleJobAsync(2, [&]() {
    EXPECT_EQ(mKv->Lookup(ToSlice(key1), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val1);

    EXPECT_EQ(mKv->Lookup(ToSlice(key2), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);
  });

  // Worker 1, commit the transaction, graveyard should be empty, update history
  // trees should have 100 versions
  GetLeanStore()->mCRManager->ScheduleJobSync(1, [&]() {
    cr::Worker::My().CommitTx();

    EXPECT_EQ(mKv->mGraveyard->CountEntries(), 0u);
    auto* historyTree = static_cast<cr::HistoryTree*>(
        GetLeanStore()->mCRManager->mHistoryTreePtr.get());
    auto* updateTree = historyTree->mUpdateBTrees[1];
    auto* removeTree = historyTree->mRemoveBTrees[1];
    EXPECT_EQ(updateTree->CountEntries(), 100u);
    EXPECT_EQ(removeTree->CountEntries(), 0u);
  });

  // Worker 2, lookup, skip the update versions, still get old values, commit
  GetLeanStore()->mCRManager->ScheduleJobSync(2, [&]() {
    EXPECT_EQ(mKv->Lookup(ToSlice(key1), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val1);

    EXPECT_EQ(mKv->Lookup(ToSlice(key2), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);

    // commit the transaction in worker 2
    cr::Worker::My().CommitTx();
  });

  // Worker 2, now get the updated new value
  GetLeanStore()->mCRManager->ScheduleJobSync(2, [&]() {
    cr::Worker::My().StartTx(TxMode::kLongRunning,
                             IsolationLevel::kSnapshotIsolation, false);
    SCOPED_DEFER(cr::Worker::My().CommitTx());

    EXPECT_EQ(mKv->Lookup(ToSlice(key1), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, newVal);

    EXPECT_EQ(mKv->Lookup(ToSlice(key2), copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);
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
  GetLeanStore()->mCRManager->ScheduleJobSync(0, [&]() {
    for (const auto& [key, val] : kvToTest) {
      cr::Worker::My().StartTx();
      SCOPED_DEFER(cr::Worker::My().CommitTx());
      EXPECT_EQ(mKv->Insert(ToSlice(key), ToSlice(val)), OpCode::kOK);
    }
  });

  // start transaction on worker 2, get the inserted values
  std::string copiedKey, copiedVal;
  auto copyKeyVal = [&](Slice key, Slice val) {
    copiedKey = std::string((const char*)key.data(), key.size());
    copiedVal = std::string((const char*)val.data(), val.size());
    EXPECT_EQ(copiedVal, kvToTest[copiedKey]);
    return true;
  };
  GetLeanStore()->mCRManager->ScheduleJobSync(2, [&]() {
    cr::Worker::My().StartTx(TxMode::kLongRunning,
                             IsolationLevel::kSnapshotIsolation, false);
    EXPECT_EQ(mKv->ScanAsc(ToSlice(smallestKey), copyKeyVal), OpCode::kOK);
  });

  // remove the key-values in worker 1
  GetLeanStore()->mCRManager->ScheduleJobSync(1, [&]() {
    cr::Worker::My().StartTx();
    for (const auto& [key, val] : kvToTest) {
      EXPECT_EQ(mKv->Remove(ToSlice(key)), OpCode::kOK);
    }
  });

  // get the old values in worker 2
  GetLeanStore()->mCRManager->ScheduleJobSync(2, [&]() {
    EXPECT_EQ(mKv->ScanAsc(ToSlice(smallestKey), copyKeyVal), OpCode::kOK);
  });

  // commit the transaction in worker 1, all the removed key-values should be
  // moved to graveyard
  GetLeanStore()->mCRManager->ScheduleJobSync(1, [&]() {
    cr::Worker::My().CommitTx();
    EXPECT_EQ(mKv->mGraveyard->CountEntries(), kvToTest.size());
  });

  // still get the old values in worker 2
  GetLeanStore()->mCRManager->ScheduleJobSync(2, [&]() {
    EXPECT_EQ(mKv->ScanAsc(ToSlice(smallestKey), copyKeyVal), OpCode::kOK);

    // commit the transaction in worker 2
    cr::Worker::My().CommitTx();
  });

  // now worker 2 can not get the old values
  GetLeanStore()->mCRManager->ScheduleJobSync(2, [&]() {
    cr::Worker::My().StartTx(TxMode::kLongRunning,
                             IsolationLevel::kSnapshotIsolation, false);
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    EXPECT_EQ(mKv->ScanAsc(ToSlice(smallestKey), copyKeyVal), OpCode::kOK);
  });
}

} // namespace leanstore::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}