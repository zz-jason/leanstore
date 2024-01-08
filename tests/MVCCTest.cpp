#include "LeanStore.hpp"
#include "storage/buffer-manager/BufferFrame.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "utils/DebugFlags.hpp"
#include "utils/Defer.hpp"
#include "utils/RandomGenerator.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <iostream>
#include <mutex>
#include <shared_mutex>

using namespace leanstore::utils;
using namespace leanstore::storage::btree;

namespace leanstore {

class MVCCTest : public ::testing::Test {
protected:
  std::string mTreeName;
  BTreeVI* mBTree;

protected:
  MVCCTest() = default;

  ~MVCCTest() = default;

  void SetUp() override {
    // init the leanstore
    auto leanstore = GetLeanStore();

    mTreeName = RandomGenerator::RandomAlphString(10);
    auto config = BTreeGeneric::Config{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create a btree for test
    cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
      cr::Worker::my().startTX();
      SCOPED_DEFER(cr::Worker::my().commitTX());
      leanstore->RegisterBTreeVI(mTreeName, config, &mBTree);
      ASSERT_NE(mBTree, nullptr);
    });
  }

  void TearDown() override {
    cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
      cr::Worker::my().startTX();
      SCOPED_DEFER(cr::Worker::my().commitTX());
      GetLeanStore()->UnRegisterBTreeVI(mTreeName);
    });
  }

public:
  inline static auto CreateLeanStore() {
    FLAGS_enable_print_btree_stats_on_exit = true;
    FLAGS_wal = true;
    FLAGS_bulk_insert = false;
    FLAGS_worker_threads = 3;
    FLAGS_recover = false;
    FLAGS_data_dir = "/tmp/MVCCTest";

    std::filesystem::path dirPath = FLAGS_data_dir;
    std::filesystem::remove_all(dirPath);
    std::filesystem::create_directories(dirPath);
    return std::make_unique<leanstore::LeanStore>();
  }

  inline static leanstore::LeanStore* GetLeanStore() {
    static auto leanStore = MVCCTest::CreateLeanStore();
    return leanStore.get();
  }
};

TEST_F(MVCCTest, LookupWhileInsert) {
  // insert a base record
  auto key0 = RandomGenerator::RandomAlphString(42);
  auto val0 = RandomGenerator::RandomAlphString(151);
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().startTX();
    auto res = mBTree->insert(Slice((const u8*)key0.data(), key0.size()),
                              Slice((const u8*)val0.data(), val0.size()));
    cr::Worker::my().commitTX();
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start a transaction to insert another record, don't commit
  auto key1 = RandomGenerator::RandomAlphString(17);
  auto val1 = RandomGenerator::RandomAlphString(131);
  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    cr::Worker::my().startTX();
    auto res = mBTree->insert(Slice((const u8*)key1.data(), key1.size()),
                              Slice((const u8*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start a transaction to lookup the base record
  // the lookup should not be blocked
  cr::CRManager::sInstance->scheduleJobSync(2, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    cr::Worker::my().startTX(TX_MODE::OLTP, IsolationLevel::kSnapshotIsolation,
                             true);
    EXPECT_EQ(mBTree->Lookup(Slice((const u8*)key0.data(), key0.size()),
                             copyValueOut),
              OpCode::kOK);
    EXPECT_EQ(copiedValue, val0);
    cr::Worker::my().commitTX();
  });

  // commit the transaction
  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };

    EXPECT_EQ(mBTree->Lookup(Slice((const u8*)key1.data(), key1.size()),
                             copyValueOut),
              OpCode::kOK);
    EXPECT_EQ(copiedValue, val1);
    cr::Worker::my().commitTX();
  });

  // now we can see the latest record
  cr::CRManager::sInstance->scheduleJobSync(2, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    cr::Worker::my().startTX(TX_MODE::OLTP, IsolationLevel::kSnapshotIsolation,
                             true);
    EXPECT_EQ(mBTree->Lookup(Slice((const u8*)key1.data(), key1.size()),
                             copyValueOut),
              OpCode::kOK);
    EXPECT_EQ(copiedValue, val1);
    cr::Worker::my().commitTX();
  });
}

TEST_F(MVCCTest, InsertConflict) {
  // insert a base record
  auto key0 = RandomGenerator::RandomAlphString(42);
  auto val0 = RandomGenerator::RandomAlphString(151);
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().startTX();
    auto res = mBTree->insert(Slice((const u8*)key0.data(), key0.size()),
                              Slice((const u8*)val0.data(), val0.size()));
    cr::Worker::my().commitTX();
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start a transaction to insert a bigger key, don't commit
  auto key1 = key0 + "a";
  auto val1 = val0;
  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    cr::Worker::my().startTX();
    auto res = mBTree->insert(Slice((const u8*)key1.data(), key1.size()),
                              Slice((const u8*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start another transaction to insert the same key
  cr::CRManager::sInstance->scheduleJobSync(2, [&]() {
    cr::Worker::my().startTX();
    auto res = mBTree->insert(Slice((const u8*)key1.data(), key1.size()),
                              Slice((const u8*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kAbortTx);
    cr::Worker::my().abortTX();
  });

  // start another transaction to insert a smaller key
  auto key2 = std::string(key0.data(), key0.size() - 1);
  auto val2 = val0;
  cr::CRManager::sInstance->scheduleJobSync(2, [&]() {
    cr::Worker::my().startTX();
    auto res = mBTree->insert(Slice((const u8*)key1.data(), key1.size()),
                              Slice((const u8*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kAbortTx);
    cr::Worker::my().abortTX();
  });

  // commit the transaction
  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };

    EXPECT_EQ(mBTree->Lookup(Slice((const u8*)key1.data(), key1.size()),
                             copyValueOut),
              OpCode::kOK);
    EXPECT_EQ(copiedValue, val1);
    cr::Worker::my().commitTX();
  });

  // now we can see the latest record
  cr::CRManager::sInstance->scheduleJobSync(2, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    cr::Worker::my().startTX(TX_MODE::OLTP, IsolationLevel::kSnapshotIsolation,
                             true);
    EXPECT_EQ(mBTree->Lookup(Slice((const u8*)key1.data(), key1.size()),
                             copyValueOut),
              OpCode::kOK);
    EXPECT_EQ(copiedValue, val1);
    cr::Worker::my().commitTX();
  });
}

} // namespace leanstore

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}