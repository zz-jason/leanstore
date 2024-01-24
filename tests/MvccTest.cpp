#include "LeanStore.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "utils/Defer.hpp"
#include "utils/RandomGenerator.hpp"

#include <gtest/gtest.h>

#include <filesystem>

using namespace leanstore::utils;
using namespace leanstore::storage::btree;

namespace leanstore {

class MvccTest : public ::testing::Test {
protected:
  std::string mTreeName;
  TransactionKV* mBTree;

protected:
  MvccTest() = default;

  ~MvccTest() = default;

  void SetUp() override {
    // init the leanstore
    auto* leanstore = GetLeanStore();

    mTreeName = RandomGenerator::RandAlphString(10);
    auto config = BTreeGeneric::Config{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create a btree for test
    GetLeanStore()->mCRManager->ScheduleJobSync(0, [&]() {
      cr::Worker::My().StartTx();
      SCOPED_DEFER(cr::Worker::My().CommitTx());
      leanstore->RegisterTransactionKV(mTreeName, config, &mBTree);
      ASSERT_NE(mBTree, nullptr);
    });
  }

  void TearDown() override {
    GetLeanStore()->mCRManager->ScheduleJobSync(1, [&]() {
      cr::Worker::My().StartTx();
      SCOPED_DEFER(cr::Worker::My().CommitTx());
      GetLeanStore()->UnRegisterTransactionKV(mTreeName);
    });
  }

public:
  inline static auto CreateLeanStore() {
    FLAGS_bulk_insert = false;
    FLAGS_worker_threads = 3;
    FLAGS_init = true;
    FLAGS_data_dir = "/tmp/MvccTest";

    std::filesystem::path dirPath = FLAGS_data_dir;
    std::filesystem::remove_all(dirPath);
    std::filesystem::create_directories(dirPath);
    return std::make_unique<leanstore::LeanStore>();
  }

  inline static leanstore::LeanStore* GetLeanStore() {
    static auto sLeanStore = MvccTest::CreateLeanStore();
    return sLeanStore.get();
  }
};

TEST_F(MvccTest, LookupWhileInsert) {
  // insert a base record
  auto key0 = RandomGenerator::RandAlphString(42);
  auto val0 = RandomGenerator::RandAlphString(151);
  GetLeanStore()->mCRManager->ScheduleJobSync(0, [&]() {
    cr::Worker::My().StartTx();
    auto res = mBTree->Insert(Slice((const u8*)key0.data(), key0.size()),
                              Slice((const u8*)val0.data(), val0.size()));
    cr::Worker::My().CommitTx();
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start a transaction to insert another record, don't commit
  auto key1 = RandomGenerator::RandAlphString(17);
  auto val1 = RandomGenerator::RandAlphString(131);
  GetLeanStore()->mCRManager->ScheduleJobSync(1, [&]() {
    cr::Worker::My().StartTx();
    auto res = mBTree->Insert(Slice((const u8*)key1.data(), key1.size()),
                              Slice((const u8*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start a transaction to lookup the base record
  // the lookup should not be blocked
  GetLeanStore()->mCRManager->ScheduleJobSync(2, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    cr::Worker::My().StartTx(TxMode::kShortRunning,
                             IsolationLevel::kSnapshotIsolation, true);
    EXPECT_EQ(mBTree->Lookup(Slice((const u8*)key0.data(), key0.size()),
                             copyValueOut),
              OpCode::kOK);
    EXPECT_EQ(copiedValue, val0);
    cr::Worker::My().CommitTx();
  });

  // commit the transaction
  GetLeanStore()->mCRManager->ScheduleJobSync(1, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };

    EXPECT_EQ(mBTree->Lookup(Slice((const u8*)key1.data(), key1.size()),
                             copyValueOut),
              OpCode::kOK);
    EXPECT_EQ(copiedValue, val1);
    cr::Worker::My().CommitTx();
  });

  // now we can see the latest record
  GetLeanStore()->mCRManager->ScheduleJobSync(2, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    cr::Worker::My().StartTx(TxMode::kShortRunning,
                             IsolationLevel::kSnapshotIsolation, true);
    EXPECT_EQ(mBTree->Lookup(Slice((const u8*)key1.data(), key1.size()),
                             copyValueOut),
              OpCode::kOK);
    EXPECT_EQ(copiedValue, val1);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(MvccTest, InsertConflict) {
  // insert a base record
  auto key0 = RandomGenerator::RandAlphString(42);
  auto val0 = RandomGenerator::RandAlphString(151);
  GetLeanStore()->mCRManager->ScheduleJobSync(0, [&]() {
    cr::Worker::My().StartTx();
    auto res = mBTree->Insert(Slice((const u8*)key0.data(), key0.size()),
                              Slice((const u8*)val0.data(), val0.size()));
    cr::Worker::My().CommitTx();
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start a transaction to insert a bigger key, don't commit
  auto key1 = key0 + "a";
  auto val1 = val0;
  GetLeanStore()->mCRManager->ScheduleJobSync(1, [&]() {
    cr::Worker::My().StartTx();
    auto res = mBTree->Insert(Slice((const u8*)key1.data(), key1.size()),
                              Slice((const u8*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start another transaction to insert the same key
  GetLeanStore()->mCRManager->ScheduleJobSync(2, [&]() {
    cr::Worker::My().StartTx();
    auto res = mBTree->Insert(Slice((const u8*)key1.data(), key1.size()),
                              Slice((const u8*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kAbortTx);
    cr::Worker::My().AbortTx();
  });

  // start another transaction to insert a smaller key
  auto key2 = std::string(key0.data(), key0.size() - 1);
  auto val2 = val0;
  GetLeanStore()->mCRManager->ScheduleJobSync(2, [&]() {
    cr::Worker::My().StartTx();
    auto res = mBTree->Insert(Slice((const u8*)key1.data(), key1.size()),
                              Slice((const u8*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kAbortTx);
    cr::Worker::My().AbortTx();
  });

  // commit the transaction
  GetLeanStore()->mCRManager->ScheduleJobSync(1, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };

    EXPECT_EQ(mBTree->Lookup(Slice((const u8*)key1.data(), key1.size()),
                             copyValueOut),
              OpCode::kOK);
    EXPECT_EQ(copiedValue, val1);
    cr::Worker::My().CommitTx();
  });

  // now we can see the latest record
  GetLeanStore()->mCRManager->ScheduleJobSync(2, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    cr::Worker::My().StartTx(TxMode::kShortRunning,
                             IsolationLevel::kSnapshotIsolation, true);
    EXPECT_EQ(mBTree->Lookup(Slice((const u8*)key1.data(), key1.size()),
                             copyValueOut),
              OpCode::kOK);
    EXPECT_EQ(copiedValue, val1);
    cr::Worker::My().CommitTx();
  });
}

} // namespace leanstore

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}