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

namespace leanstore {

static std::shared_mutex sMutex;
static std::unique_ptr<LeanStore> sLeanStore = nullptr;

leanstore::LeanStore* GetLeanStore() {
  std::shared_lock sharedLock(sMutex);
  if (sLeanStore != nullptr) {
    return sLeanStore.get();
  }

  sharedLock.unlock();
  std::unique_lock uniqueLock(sMutex);

  if (sLeanStore != nullptr) {
    return sLeanStore.get();
  }

  FLAGS_vi = true;
  FLAGS_enable_print_btree_stats_on_exit = true;
  FLAGS_wal = true;
  FLAGS_bulk_insert = false;
  FLAGS_worker_threads = 2;
  FLAGS_recover = false;
  FLAGS_data_dir = "/tmp/BTreeVITest";

  std::filesystem::path dirPath = FLAGS_data_dir;
  std::filesystem::remove_all(dirPath);
  std::filesystem::create_directories(dirPath);
  sLeanStore = std::make_unique<leanstore::LeanStore>();
  return sLeanStore.get();
}

class BTreeVITest : public ::testing::Test {
protected:
  BTreeVITest() = default;

  ~BTreeVITest() = default;
};

TEST_F(BTreeVITest, BTreeVICreate) {
  GetLeanStore();
  storage::btree::BTreeVI* btree;
  storage::btree::BTreeVI* another;

  // create leanstore btree for table records
  const auto* btreeName = "testTree1";
  auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };

  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    EXPECT_TRUE(
        GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &btree));
    EXPECT_NE(btree, nullptr);
  });

  // create btree with same should fail in the same worker
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    EXPECT_FALSE(
        GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &another));
    EXPECT_EQ(another, nullptr);
  });

  // create btree with same should also fail in other workers
  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    EXPECT_FALSE(
        GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &another));
    EXPECT_EQ(another, nullptr);
  });

  // create btree with another different name should success
  const auto* btreeName2 = "testTree2";
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    EXPECT_TRUE(
        GetLeanStore()->RegisterBTreeVI(btreeName2, btreeConfig, &another));
    EXPECT_NE(btree, nullptr);
  });

  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    GetLeanStore()->UnRegisterBTreeVI(btreeName);
    GetLeanStore()->UnRegisterBTreeVI(btreeName2);
  });
}

TEST_F(BTreeVITest, BTreeVIInsertAndLookup) {
  GetLeanStore();
  storage::btree::BTreeVI* btree;

  // prepare key-value pairs to insert
  size_t numKVs(10);
  std::vector<std::tuple<std::string, std::string>> kvToTest;
  for (size_t i = 0; i < numKVs; ++i) {
    std::string key("key_btree_VI_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val("VAL_BTREE_VI_YYYYYYYYYYYY_" + std::to_string(i));
    kvToTest.push_back(std::make_tuple(key, val));
  }

  // create leanstore btree for table records
  const auto* btreeName = "testTree1";
  auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    EXPECT_TRUE(
        GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &btree));
    EXPECT_NE(btree, nullptr);

    // insert some values
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OP_RESULT::OK);
    }
  });

  // query on the created btree in the same worker
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, expectedVal] = kvToTest[i];
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), copyValueOut),
          OP_RESULT::OK);
      EXPECT_EQ(copiedValue, expectedVal);
    }
  });

  // query on the created btree in another worker
  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, expectedVal] = kvToTest[i];
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), copyValueOut),
          OP_RESULT::OK);
      EXPECT_EQ(copiedValue, expectedVal);
    }
  });

  cr::CRManager::sInstance->scheduleJobSync(
      1, [&]() { GetLeanStore()->UnRegisterBTreeVI(btreeName); });
}

TEST_F(BTreeVITest, BTreeVIToJSON) {
  GetLeanStore();
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    storage::btree::BTreeVI* btree;

    // prepare key-value pairs to insert
    size_t numKVs(10);
    std::vector<std::tuple<std::string, std::string>> kvToTest;
    for (size_t i = 0; i < numKVs; ++i) {
      std::string key("key_btree_VI_xxxxxxxxxxxx_" + std::to_string(i));
      std::string val("VAL_BTREE_VI_YYYYYYYYYYYY_" + std::to_string(i));
      kvToTest.push_back(std::make_tuple(key, val));
    }
    // create leanstore btree for table records
    const auto* btreeName = "testTree1";
    auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };
    EXPECT_TRUE(
        GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &btree));
    EXPECT_NE(btree, nullptr);

    // insert some values
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OP_RESULT::OK);
    }

    // auto doc = leanstore::storage::btree::BTreeGeneric::ToJSON(*btree);
    // std::cout << leanstore::utils::JsonToStr(&doc);

    GetLeanStore()->UnRegisterBTreeVI(btreeName);
  });
}

} // namespace leanstore