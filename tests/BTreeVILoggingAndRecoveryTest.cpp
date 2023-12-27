#include "LeanStore.hpp"
#include "storage/buffer-manager/BufferFrame.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "utils/DebugFlags.hpp"
#include "utils/Defer.hpp"
#include "utils/RandomGenerator.hpp"

#include <gtest/gtest.h>

#include <filesystem>

namespace leanstore {

using namespace leanstore::storage::btree;

class BTreeVILoggingAndRecoveryTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> mLeanStore;

  BTreeVILoggingAndRecoveryTest() {
    FLAGS_vi = true;
    FLAGS_enable_print_btree_stats_on_exit = true;
    FLAGS_wal = true;
    FLAGS_bulk_insert = false;
  }

  ~BTreeVILoggingAndRecoveryTest() = default;

  static u64 RandomWorkerId() {
    auto numWorkers = cr::CRManager::sInstance->NumWorkerThreads();
    return utils::RandomGenerator::getRand<u64>(0, numWorkers);
  }
};

TEST_F(BTreeVILoggingAndRecoveryTest, SerializeAndDeserialize) {
  FLAGS_data_dir = "/tmp/BTreeVILoggingAndRecoveryTest/SerializeAndDeserialize";
  std::filesystem::path dirPath = FLAGS_data_dir;
  std::filesystem::remove_all(dirPath);
  std::filesystem::create_directories(dirPath);

  dirPath = GetLogDir();
  std::filesystem::remove_all(dirPath);
  std::filesystem::create_directories(dirPath);

  FLAGS_worker_threads = 2;
  FLAGS_recover = false;
  mLeanStore = std::make_unique<LeanStore>();
  BTreeVI* btree;

  // prepare key-value pairs to insert
  size_t numKVs(10);
  std::vector<std::tuple<std::string, std::string>> kvToTest;
  for (size_t i = 0; i < numKVs; ++i) {
    std::string key("key_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val("VAL_YYYYYYYYYYYY_" + std::to_string(i));
    kvToTest.push_back(std::make_tuple(key, val));
  }

  // create btree for table records
  const auto* btreeName = "testTree1";
  auto btreeConfig = BTreeGeneric::Config{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };

  // TODO(jian.z): need to create btree within a transaction, otherwise
  // transactions depend on the btree creator worker may hang on commit.
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().startTX();
    SCOPED_DEFER(cr::Worker::my().commitTX());
    mLeanStore->RegisterBTreeVI(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
  });

  // insert some values
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().startTX();
    SCOPED_DEFER(cr::Worker::my().commitTX());
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OP_RESULT::OK);
    }
  });

  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    rapidjson::Document doc(rapidjson::kObjectType);
    BTreeGeneric::ToJSON(*btree, &doc);
    LOG(INFO) << "btree before destroy: " << utils::JsonToStr(&doc);
  });

  // meta file should be serialized during destructor.
  mLeanStore.reset(nullptr);
  FLAGS_recover = true;

  // recreate the store, it's expected that all the meta and pages are rebult.
  mLeanStore = std::make_unique<LeanStore>();
  mLeanStore->GetBTreeVI(btreeName, &btree);
  EXPECT_NE(btree, nullptr);

  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    rapidjson::Document doc(rapidjson::kObjectType);
    BTreeGeneric::ToJSON(*btree, &doc);
    LOG(INFO) << "btree after recovery: " << utils::JsonToStr(&doc);
  });

  // lookup the restored btree
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().startTX();
    SCOPED_DEFER(cr::Worker::my().commitTX());
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

  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    cr::Worker::my().startTX();
    SCOPED_DEFER(cr::Worker::my().commitTX());
    mLeanStore->UnRegisterBTreeVI(btreeName);
  });
}

TEST_F(BTreeVILoggingAndRecoveryTest, RecoverAfterInsert) {
  FLAGS_data_dir = "/tmp/BTreeVILoggingAndRecoveryTest/RecoverAfterInsert";
  std::filesystem::path dirPath = FLAGS_data_dir;
  std::filesystem::remove_all(dirPath);
  std::filesystem::create_directories(dirPath);

  dirPath = GetLogDir();
  std::filesystem::remove_all(dirPath);
  std::filesystem::create_directories(dirPath);

  FLAGS_worker_threads = 2;
  FLAGS_recover = false;
  mLeanStore = std::make_unique<LeanStore>();
  BTreeVI* btree;

  // prepare key-value pairs to insert
  size_t numKVs(10);
  std::vector<std::tuple<std::string, std::string>> kvToTest;
  for (size_t i = 0; i < numKVs; ++i) {
    std::string key("key_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val("VAL_YYYYYYYYYYYY_" + std::to_string(i));
    kvToTest.push_back(std::make_tuple(key, val));
  }

  // create leanstore btree for table records
  const auto* btreeName = "testTree1";
  auto btreeConfig = BTreeGeneric::Config{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };

  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().startTX();
    mLeanStore->RegisterBTreeVI(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::my().commitTX();

    // insert some values
    cr::Worker::my().startTX();
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OP_RESULT::OK);
    }
    cr::Worker::my().commitTX();
  });

  // skip dumpping buffer frames on exit
  LS_DEBUG_ENABLE("skip_CheckpointAllBufferFrames");
  SCOPED_DEFER(LS_DEBUG_DISABLE("skip_CheckpointAllBufferFrames"));
  mLeanStore.reset(nullptr);
  FLAGS_recover = true;

  // recreate the store, it's expected that all the meta and pages are rebult
  // based on the WAL entries
  mLeanStore = std::make_unique<LeanStore>();
  mLeanStore->GetBTreeVI(btreeName, &btree);
  EXPECT_NE(btree, nullptr);
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().startTX();
    SCOPED_DEFER(cr::Worker::my().commitTX());
    rapidjson::Document doc(rapidjson::kObjectType);
    BTreeGeneric::ToJSON(*static_cast<BTreeGeneric*>(btree), &doc);
    DLOG(INFO) << "BTreeVI after recovery: " << utils::JsonToStr(&doc);
  });

  // lookup the restored btree
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().startTX();
    SCOPED_DEFER(cr::Worker::my().commitTX());
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