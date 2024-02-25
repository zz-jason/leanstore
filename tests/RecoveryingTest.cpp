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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <ostream>

using namespace leanstore::storage::btree;

namespace leanstore::test {

class RecoveringTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> mStore;

  RecoveringTest() {
    // Create a leanstore instance for the test case
    auto* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
    auto curTestName = std::string(curTest->test_case_name()) + "_" +
                       std::string(curTest->name());
    FLAGS_init = true;
    FLAGS_logtostdout = true;
    FLAGS_data_dir = "/tmp/" + curTestName;
    FLAGS_worker_threads = 2;
    FLAGS_enable_eager_garbage_collection = true;
    auto res = LeanStore::Open();
    mStore = std::move(res.value());
  }

  ~RecoveringTest() = default;

  static u64 randomWorkerId() {
    auto numWorkers = FLAGS_worker_threads;
    return utils::RandomGenerator::Rand<u64>(0, numWorkers);
  }
};

TEST_F(RecoveringTest, SerializeAndDeserialize) {
  TransactionKV* btree;

  // prepare key-value pairs to insert
  size_t numKVs(100);
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

  mStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
  });

  // insert some values
  mStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->Insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
    }
  });

  // meta file should be serialized during destructor.
  mStore.reset(nullptr);

  // recreate the store, it's expected that all the meta and pages are rebult.
  FLAGS_init = false;
  auto res = LeanStore::Open();
  EXPECT_TRUE(res);

  mStore = std::move(res.value());
  mStore->GetTransactionKV(btreeName, &btree);
  EXPECT_NE(btree, nullptr);

  // lookup the restored btree
  mStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, expectedVal] = kvToTest[i];
      auto opCode =
          btree->Lookup(Slice((const u8*)key.data(), key.size()), copyValueOut);
      EXPECT_EQ(opCode, OpCode::kOK);
      EXPECT_EQ(copiedValue, expectedVal);
    }
  });

  mStore->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    mStore->DropTransactionKV(btreeName);
  });

  mStore = nullptr;
}

TEST_F(RecoveringTest, RecoverAfterInsert) {
#ifndef DEBUG
  GTEST_SKIP() << "This test only works in debug mode";
#endif

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

  mStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
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
  LS_DEBUG_ENABLE(mStore, "skip_CheckpointAllBufferFrames");
  SCOPED_DEFER(LS_DEBUG_DISABLE(mStore, "skip_CheckpointAllBufferFrames"));
  mStore.reset(nullptr);

  // recreate the store, it's expected that all the meta and pages are rebult
  // based on the WAL entries
  FLAGS_init = false;
  auto res = LeanStore::Open();
  EXPECT_TRUE(res);

  mStore = std::move(res.value());
  mStore->GetTransactionKV(btreeName, &btree);
  EXPECT_NE(btree, nullptr);
  mStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    rapidjson::Document doc(rapidjson::kObjectType);
    BTreeGeneric::ToJson(*static_cast<BTreeGeneric*>(btree), &doc);
    DLOG(INFO) << "TransactionKV after recovery: " << utils::JsonToStr(&doc);
  });

  // lookup the restored btree
  mStore->ExecSync(0, [&]() {
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

} // namespace leanstore::test