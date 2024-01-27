#include "LeanStore.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "storage/btree/BasicKV.hpp"
#include "storage/btree/TransactionKV.hpp"
#include "storage/btree/core/BTreeGeneric.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "utils/DebugFlags.hpp"
#include "utils/Defer.hpp"
#include "utils/JsonUtil.hpp"
#include "utils/RandomGenerator.hpp"

#include "glog/logging.h"
#include <gtest/gtest.h>

#include <filesystem>

namespace leanstore {

using namespace leanstore::storage::btree;

class RecoveringTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> mLeanStore;

  RecoveringTest() {
    FLAGS_bulk_insert = false;
  }

  ~RecoveringTest() = default;

  static u64 randomWorkerId() {
    auto numWorkers = FLAGS_worker_threads;
    return utils::RandomGenerator::Rand<u64>(0, numWorkers);
  }
};

TEST_F(RecoveringTest, SerializeAndDeserialize) {
  FLAGS_data_dir = "/tmp/RecoveringTest/SerializeAndDeserialize";
  std::filesystem::path dirPath = FLAGS_data_dir;
  std::filesystem::remove_all(dirPath);
  std::filesystem::create_directories(dirPath);

  dirPath = GetLogDir();
  std::filesystem::remove_all(dirPath);
  std::filesystem::create_directories(dirPath);

  FLAGS_worker_threads = 2;
  FLAGS_init = true;
  mLeanStore = std::make_unique<LeanStore>();
  TransactionKV* btree;

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
  auto btreeConfig = BTreeConfig{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };

  mLeanStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    mLeanStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
  });

  // insert some values
  mLeanStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->Insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
    }
  });

  mLeanStore->ExecSync(0, [&]() {
    rapidjson::Document doc(rapidjson::kObjectType);
    BTreeGeneric::ToJson(*btree, &doc);
    LOG(INFO) << "btree before destroy: " << utils::JsonToStr(&doc);
  });

  // meta file should be serialized during destructor.
  mLeanStore.reset(nullptr);
  FLAGS_init = false;

  // recreate the store, it's expected that all the meta and pages are rebult.
  mLeanStore = std::make_unique<LeanStore>();
  mLeanStore->GetTransactionKV(btreeName, &btree);
  EXPECT_NE(btree, nullptr);

  mLeanStore->ExecSync(0, [&]() {
    rapidjson::Document doc(rapidjson::kObjectType);
    BTreeGeneric::ToJson(*btree, &doc);
    LOG(INFO) << "btree after recovery: " << utils::JsonToStr(&doc);
  });

  // lookup the restored btree
  mLeanStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
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

  mLeanStore->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    mLeanStore->DropTransactionKV(btreeName);
  });
}

TEST_F(RecoveringTest, RecoverAfterInsert) {
  FLAGS_data_dir = "/tmp/RecoveringTest/RecoverAfterInsert";
  std::filesystem::path dirPath = FLAGS_data_dir;
  std::filesystem::remove_all(dirPath);
  std::filesystem::create_directories(dirPath);

  dirPath = GetLogDir();
  std::filesystem::remove_all(dirPath);
  std::filesystem::create_directories(dirPath);

  FLAGS_worker_threads = 2;
  FLAGS_init = true;
  FLAGS_logtostdout = true;
  mLeanStore = std::make_unique<LeanStore>();
  TransactionKV* btree;

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
  auto btreeConfig = BTreeConfig{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };

  mLeanStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    mLeanStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::My().CommitTx();

    // insert some values
    cr::Worker::My().StartTx();
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->Insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
    }
    cr::Worker::My().CommitTx();

    rapidjson::Document doc(rapidjson::kObjectType);
    leanstore::storage::btree::BTreeGeneric::ToJson(*btree, &doc);
    LOG(INFO) << "BTree before destroy:\n" << leanstore::utils::JsonToStr(&doc);
  });

  // skip dumpping buffer frames on exit
  LS_DEBUG_ENABLE("skip_CheckpointAllBufferFrames");
  SCOPED_DEFER(LS_DEBUG_DISABLE("skip_CheckpointAllBufferFrames"));
  mLeanStore.reset(nullptr);
  FLAGS_init = false;

  // recreate the store, it's expected that all the meta and pages are rebult
  // based on the WAL entries
  mLeanStore = std::make_unique<LeanStore>();
  mLeanStore->GetTransactionKV(btreeName, &btree);
  EXPECT_NE(btree, nullptr);
  mLeanStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    rapidjson::Document doc(rapidjson::kObjectType);
    BTreeGeneric::ToJson(*static_cast<BTreeGeneric*>(btree), &doc);
    DLOG(INFO) << "TransactionKV after recovery: " << utils::JsonToStr(&doc);
  });

  // lookup the restored btree
  mLeanStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
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