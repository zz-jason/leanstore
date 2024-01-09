#include "LeanStore.hpp"
#include "storage/buffer-manager/BufferFrame.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "utils/DebugFlags.hpp"
#include "utils/Defer.hpp"
#include "utils/RandomGenerator.hpp"

#include <gtest/gtest.h>

#include <filesystem>

namespace leanstore {

class BTreeLLTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> mLeanStore;

  BTreeLLTest() {
    FLAGS_enable_print_btree_stats_on_exit = true;
    FLAGS_wal = true;
    FLAGS_bulk_insert = false;
  }

  ~BTreeLLTest() = default;
};

TEST_F(BTreeLLTest, BTreeLLCreate) {
  FLAGS_data_dir = "/tmp/BTreeLLTest/BTreeLLCreate";
  std::filesystem::path dirPath = FLAGS_data_dir;
  std::filesystem::remove_all(dirPath);
  std::filesystem::create_directories(dirPath);

  FLAGS_worker_threads = 2;
  FLAGS_recover = false;
  mLeanStore = std::make_unique<leanstore::LeanStore>();
  storage::btree::BTreeLL* btree;
  storage::btree::BTreeLL* another;

  // create leanstore btree for table records
  const auto* btreeName = "testTree1";
  auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };

  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().StartTx();
    SCOPED_DEFER(cr::Worker::my().CommitTx());
    mLeanStore->RegisterBTreeLL(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
  });

  // create btree with same should fail in the same worker
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().StartTx();
    SCOPED_DEFER(cr::Worker::my().CommitTx());
    mLeanStore->RegisterBTreeLL(btreeName, btreeConfig, &another);
    EXPECT_EQ(another, nullptr);
  });

  // create btree with same should also fail in other workers
  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    cr::Worker::my().StartTx();
    SCOPED_DEFER(cr::Worker::my().CommitTx());
    mLeanStore->RegisterBTreeLL(btreeName, btreeConfig, &another);
    EXPECT_EQ(another, nullptr);
  });

  // create btree with another different name should success
  btreeName = "testTree2";
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().StartTx();
    SCOPED_DEFER(cr::Worker::my().CommitTx());
    mLeanStore->RegisterBTreeLL(btreeName, btreeConfig, &another);
    EXPECT_NE(btree, nullptr);
  });
}

TEST_F(BTreeLLTest, BTreeLLInsertAndLookup) {
  FLAGS_data_dir = "/tmp/BTreeLLTest/BTreeLLInsertAndLookup";
  std::filesystem::path dirPath = FLAGS_data_dir;
  std::filesystem::remove_all(dirPath);
  std::filesystem::create_directories(dirPath);
  FLAGS_worker_threads = 2;
  FLAGS_recover = false;
  mLeanStore = std::make_unique<leanstore::LeanStore>();
  storage::btree::BTreeLL* btree;

  // prepare key-value pairs to insert
  size_t numKVs(10);
  std::vector<std::tuple<std::string, std::string>> kvToTest;
  for (size_t i = 0; i < numKVs; ++i) {
    std::string key("key_btree_LL_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val("VAL_BTREE_LL_YYYYYYYYYYYY_" + std::to_string(i));
    kvToTest.push_back(std::make_tuple(key, val));
  }

  // create leanstore btree for table records
  const auto* btreeName = "testTree1";
  auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().StartTx();
    mLeanStore->RegisterBTreeLL(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::my().CommitTx();

    // insert some values
    cr::Worker::my().StartTx();
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
    }
    cr::Worker::my().CommitTx();
  });

  // query on the created btree in the same worker
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().StartTx();
    SCOPED_DEFER(cr::Worker::my().CommitTx());
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, expectedVal] = kvToTest[i];
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), copyValueOut),
          OpCode::kOK);
      EXPECT_EQ(copiedValue, expectedVal);
    }
  });

  // query on the created btree in another worker
  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    cr::Worker::my().StartTx();
    SCOPED_DEFER(cr::Worker::my().CommitTx());
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, expectedVal] = kvToTest[i];
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), copyValueOut),
          OpCode::kOK);
      EXPECT_EQ(copiedValue, expectedVal);
    }
  });
}

} // namespace leanstore