#include "btree/BasicKV.hpp"

#include "btree/TransactionKV.hpp"
#include "btree/core/BTreeGeneric.hpp"
#include "buffer-manager/BufferManager.hpp"
#include "concurrency/CRManager.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/Store.hpp"
#include "utils/Defer.hpp"

#include <gtest/gtest.h>

#include <cstddef>

namespace leanstore::test {

class BasicKVTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> mStore;

  BasicKVTest() = default;

  ~BasicKVTest() = default;

  void SetUp() override {
    // Create a leanstore instance for the test case
    auto* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
    auto curTestName = std::string(curTest->test_case_name()) + "_" +
                       std::string(curTest->name());

    auto res = LeanStore::Open(StoreOption{
        .mCreateFromScratch = true,
        .mStoreDir = "/tmp/" + curTestName,
        .mWorkerThreads = 2,
        .mEnableBulkInsert = false,
        .mEnableEagerGc = true,
    });
    ASSERT_TRUE(res);
    mStore = std::move(res.value());
  }
};

TEST_F(BasicKVTest, BasicKVCreate) {
  // create leanstore btree for table records
  const auto* btreeName = "testTree1";
  auto btreeConfig = leanstore::storage::btree::BTreeConfig{
      .mEnableWal = mStore->mStoreOption.mEnableWal,
      .mUseBulkInsert = mStore->mStoreOption.mEnableBulkInsert,
  };

  mStore->ExecSync(0, [&]() {
    auto res = mStore->CreateBasicKV(btreeName, btreeConfig);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);
  });

  // create btree with same should fail in the same worker
  mStore->ExecSync(0, [&]() {
    auto res = mStore->CreateBasicKV(btreeName, btreeConfig);
    EXPECT_FALSE(res);
  });

  // create btree with same should also fail in other workers
  mStore->ExecSync(1, [&]() {
    auto res = mStore->CreateBasicKV(btreeName, btreeConfig);
    EXPECT_FALSE(res);
  });

  // create btree with another different name should success
  btreeName = "testTree2";
  mStore->ExecSync(0, [&]() {
    auto res = mStore->CreateBasicKV(btreeName, btreeConfig);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);
  });
}

TEST_F(BasicKVTest, BasicKVInsertAndLookup) {
  storage::btree::BasicKV* btree;

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
  auto btreeConfig = leanstore::storage::btree::BTreeConfig{
      .mEnableWal = mStore->mStoreOption.mEnableWal,
      .mUseBulkInsert = mStore->mStoreOption.mEnableBulkInsert,
  };
  mStore->ExecSync(0, [&]() {
    auto res = mStore->CreateBasicKV(btreeName, btreeConfig);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);

    // insert some values
    btree = res.value();
    cr::Worker::My().StartTx();
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)val.data(), val.size())),
                OpCode::kOK);
    }
    cr::Worker::My().CommitTx();
  });

  // query on the created btree in the same worker
  mStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, expectedVal] = kvToTest[i];
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()),
                              copyValueOut),
                OpCode::kOK);
      EXPECT_EQ(copiedValue, expectedVal);
    }
  });

  // query on the created btree in another worker
  mStore->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, expectedVal] = kvToTest[i];
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()),
                              copyValueOut),
                OpCode::kOK);
      EXPECT_EQ(copiedValue, expectedVal);
    }
  });
}

} // namespace leanstore::test
