#include "LeanStore.hpp"
#include "storage/buffer-manager/BufferFrame.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "utils/DebugFlags.hpp"
#include "utils/Defer.hpp"
#include "utils/RandomGenerator.hpp"

#include <gtest/gtest.h>

#include <filesystem>

namespace leanstore {

class LeanStoreTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> mLeanStore;

  LeanStoreTest() {
    FLAGS_vi = true;
    FLAGS_enable_print_btree_stats_on_exit = true;
    FLAGS_wal = true;
    FLAGS_bulk_insert = false;
  }

  ~LeanStoreTest() = default;

  static u64 RandomWorkerId() {
    auto numWorkers = cr::CRManager::sInstance->NumWorkerThreads();
    return utils::RandomGenerator::getRand<u64>(0, numWorkers);
  }
};

TEST_F(LeanStoreTest, BTreeVICreateDuplicatedTree) {
  FLAGS_data_dir = "/tmp/LeanStoreTest/BTreeVICreateDuplicatedTree";
  std::filesystem::path dir_path = FLAGS_data_dir;
  std::filesystem::remove_all(dir_path);
  std::filesystem::create_directories(dir_path);

  FLAGS_worker_threads = 2;
  FLAGS_recover = false;
  mLeanStore = std::make_unique<leanstore::LeanStore>();
  storage::btree::BTreeVI* btree;
  storage::btree::BTreeVI* another;

  // create leanstore btree for table records
  auto btreeName = "testTree1";
  auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };

  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    EXPECT_TRUE(mLeanStore->RegisterBTreeVI(btreeName, btreeConfig, &btree));
    EXPECT_NE(btree, nullptr);
  });

  // create btree with same should fail in the same worker
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    EXPECT_FALSE(mLeanStore->RegisterBTreeVI(btreeName, btreeConfig, &another));
    EXPECT_EQ(another, nullptr);
  });

  // create btree with same should also fail in other workers
  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    EXPECT_FALSE(mLeanStore->RegisterBTreeVI(btreeName, btreeConfig, &another));
    EXPECT_EQ(another, nullptr);
  });

  // create btree with another different name should success
  btreeName = "testTree2";
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    EXPECT_TRUE(mLeanStore->RegisterBTreeVI(btreeName, btreeConfig, &another));
    EXPECT_NE(btree, nullptr);
  });
}

TEST_F(LeanStoreTest, BTreeLLInsertAndLookup) {
  FLAGS_data_dir = "/tmp/LeanStoreTest/BTreeLLInsertAndLookup";
  std::filesystem::path dir_path = FLAGS_data_dir;
  std::filesystem::remove_all(dir_path);
  std::filesystem::create_directories(dir_path);
  FLAGS_worker_threads = 2;
  FLAGS_recover = false;
  mLeanStore = std::make_unique<leanstore::LeanStore>();
  storage::btree::BTreeLL* btree;

  // prepare key-value pairs to insert
  size_t numKVs(10);
  std::vector<std::tuple<std::string, std::string>> kvToTest;
  for (size_t i = 0; i < numKVs; ++i) {
    std::string key("key_btree_ll_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val("VAL_BTREE_LL_YYYYYYYYYYYY_" + std::to_string(i));
    kvToTest.push_back(std::make_tuple(key, val));
  }

  // create leanstore btree for table records
  auto btreeName = "testTree1";
  auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    EXPECT_TRUE(mLeanStore->RegisterBTreeLL(btreeName, btreeConfig, &btree));
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
}

TEST_F(LeanStoreTest, Serde) {
  FLAGS_data_dir = "/tmp/LeanStoreTest/Serde";
  std::filesystem::path dir_path = FLAGS_data_dir;
  std::filesystem::remove_all(dir_path);
  std::filesystem::create_directories(dir_path);
  FLAGS_worker_threads = 2;
  FLAGS_recover = false;
  mLeanStore = std::make_unique<leanstore::LeanStore>();
  storage::btree::BTreeVI* btree;

  // prepare key-value pairs to insert
  size_t numKVs(10);
  std::vector<std::tuple<std::string, std::string>> kvToTest;
  for (size_t i = 0; i < numKVs; ++i) {
    std::string key("key_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val("VAL_YYYYYYYYYYYY_" + std::to_string(i));
    kvToTest.push_back(std::make_tuple(key, val));
  }

  // create btree for table records
  auto btreeName = "testTree1";
  auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };

  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    EXPECT_TRUE(mLeanStore->RegisterBTreeVI(btreeName, btreeConfig, &btree));
    EXPECT_NE(btree, nullptr);
  });

  // insert some values
  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OP_RESULT::OK);
    }
  });

  // meta file should be serialized during destructor.
  mLeanStore.reset(nullptr);
  FLAGS_recover = true;

  // recreate the store, it's expected that all the meta and pages are rebult.
  mLeanStore = std::make_unique<leanstore::LeanStore>();
  EXPECT_TRUE(mLeanStore->GetBTreeVI(btreeName, &btree));
  EXPECT_NE(btree, nullptr);

  // lookup the restored btree
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
}

TEST_F(LeanStoreTest, RecoverAfterInsert) {
  FLAGS_data_dir = "/tmp/LeanStoreTest/RecoverAfterInsert";
  std::filesystem::path dir_path = FLAGS_data_dir;
  std::filesystem::remove_all(dir_path);
  std::filesystem::create_directories(dir_path);
  FLAGS_worker_threads = 2;
  FLAGS_recover = false;
  mLeanStore = std::make_unique<leanstore::LeanStore>();
  storage::btree::BTreeVI* btree;

  // prepare key-value pairs to insert
  size_t numKVs(10);
  std::vector<std::tuple<std::string, std::string>> kvToTest;
  for (size_t i = 0; i < numKVs; ++i) {
    std::string key("key_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val("VAL_YYYYYYYYYYYY_" + std::to_string(i));
    kvToTest.push_back(std::make_tuple(key, val));
  }

  // create leanstore btree for table records
  auto btreeName = "testTree1";
  auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };

  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    EXPECT_TRUE(mLeanStore->RegisterBTreeVI(btreeName, btreeConfig, &btree));
    EXPECT_NE(btree, nullptr);

    // insert some values
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OP_RESULT::OK);
    }
  });

  // skip dumpping buffer frames on exit
  LS_DEBUG_ENABLE("skip_writeAllBufferFrames");
  SCOPED_DEFER(LS_DEBUG_DISABLE("skip_writeAllBufferFrames"));

  mLeanStore.reset(nullptr);

  // recreate the store, it's expected that all the meta and pages are rebult
  // based on the WAL entries
  FLAGS_recover = true;
  mLeanStore = std::make_unique<leanstore::LeanStore>();
  EXPECT_TRUE(mLeanStore->GetBTreeVI(btreeName, &btree));
  EXPECT_NE(btree, nullptr);

  // lookup the restored btree
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
}

} // namespace leanstore