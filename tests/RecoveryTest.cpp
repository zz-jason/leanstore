#include "btree/BasicKV.hpp"
#include "btree/TransactionKV.hpp"
#include "btree/core/BTreeGeneric.hpp"
#include "buffer-manager/BufferManager.hpp"
#include "concurrency/CRManager.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/LeanStore.hpp"
#include "utils/DebugFlags.hpp"
#include "utils/Defer.hpp"
#include "utils/JsonUtil.hpp"
#include "utils/RandomGenerator.hpp"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <format>
#include <ostream>
#include <string>

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

  static uint64_t randomWorkerId() {
    auto numWorkers = FLAGS_worker_threads;
    return utils::RandomGenerator::Rand<uint64_t>(0, numWorkers);
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
      EXPECT_EQ(btree->Insert(key, val), OpCode::kOK);
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
      auto opCode = btree->Lookup(key, copyValueOut);
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
  size_t numKVs(100);
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
      EXPECT_EQ(btree->Insert(key, val), OpCode::kOK);
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
      EXPECT_EQ(btree->Lookup(key, copyValueOut), OpCode::kOK);
      EXPECT_EQ(copiedValue, expectedVal);
    }
  });
}

// generate value in the {}-{} format
static std::string GenerateValue(int ordinalPrefix, size_t valSize) {
  auto prefix = std::format("{}-", ordinalPrefix);
  if (prefix.size() >= valSize) {
    return prefix.substr(0, valSize);
  }

  return prefix +
         utils::RandomGenerator::RandAlphString(valSize - prefix.size());
}

TEST_F(RecoveringTest, RecoverAfterUpdate) {
#ifndef DEBUG
  GTEST_SKIP() << "This test only works in debug mode";
#endif

  TransactionKV* btree;

  // prepare key-value pairs to insert
  auto valSize = 120u;
  size_t numKVs(20);
  std::vector<std::tuple<std::string, std::string>> kvToTest;
  for (size_t i = 0; i < numKVs; ++i) {
    std::string key("key_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val = GenerateValue(0, valSize);
    kvToTest.push_back(std::make_tuple(key, val));
  }

  // create leanstore btree for table records
  const auto* btreeName = "testTree1";
  auto btreeConfig = BTreeConfig{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };

  // update all the values to this newVal
  const uint64_t updateDescBufSize = UpdateDesc::Size(1);
  uint8_t updateDescBuf[updateDescBufSize];
  auto* updateDesc = UpdateDesc::CreateFrom(updateDescBuf);
  updateDesc->mNumSlots = 1;
  updateDesc->mUpdateSlots[0].mOffset = 0;
  updateDesc->mUpdateSlots[0].mSize = valSize;

  mStore->ExecSync(0, [&]() {
    // create btree
    cr::Worker::My().StartTx();
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::My().CommitTx();

    // insert some values
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      cr::Worker::My().StartTx();
      EXPECT_EQ(btree->Insert(key, val), OpCode::kOK);
      cr::Worker::My().CommitTx();
    }

    // update all the values
    for (size_t i = 0; i < numKVs; ++i) {
      auto& [key, val] = kvToTest[i];
      auto updateCallBack = [&](MutableSlice mutRawVal) {
        std::memcpy(mutRawVal.Data(), val.data(), mutRawVal.Size());
      };
      // update each key 3 times
      for (auto j = 1u; j <= 3; j++) {
        val = GenerateValue(j, valSize);
        cr::Worker::My().StartTx();
        EXPECT_EQ(btree->UpdatePartial(key, updateCallBack, *updateDesc),
                  OpCode::kOK);
        cr::Worker::My().CommitTx();
      }
    }

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
    auto copyValueOut = [&](Slice val) { copiedValue = val.ToString(); };
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, expectedVal] = kvToTest[i];
      auto retCode = btree->Lookup(key, copyValueOut);
      EXPECT_EQ(retCode, OpCode::kOK);
      EXPECT_EQ(copiedValue, expectedVal);
    }
  });
}

TEST_F(RecoveringTest, RecoverAfterRemove) {
#ifndef DEBUG
  GTEST_SKIP() << "This test only works in debug mode";
#endif

  TransactionKV* btree;
  const auto* btreeName = "testTree1";

  // prepare key-value pairs to insert
  auto valSize = 120u;
  size_t numKVs(10);
  std::vector<std::tuple<std::string, std::string>> kvToTest;
  for (size_t i = 0; i < numKVs; ++i) {
    std::string key("key_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val = utils::RandomGenerator::RandAlphString(valSize);
    kvToTest.push_back(std::make_tuple(key, val));
  }

  mStore->ExecSync(0, [&]() {
    // create btree
    auto btreeConfig = BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };
    cr::Worker::My().StartTx();
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::My().CommitTx();

    // insert some values
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      cr::Worker::My().StartTx();
      EXPECT_EQ(btree->Insert(key, val), OpCode::kOK);
      cr::Worker::My().CommitTx();
    }

    // remove all the values
    for (size_t i = 0; i < numKVs; ++i) {
      auto& [key, val] = kvToTest[i];
      cr::Worker::My().StartTx();
      EXPECT_EQ(btree->Remove(key), OpCode::kOK);
      cr::Worker::My().CommitTx();
    }

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
      EXPECT_EQ(btree->Lookup(key, copyValueOut), OpCode::kNotFound);
    }
  });
}

} // namespace leanstore::test