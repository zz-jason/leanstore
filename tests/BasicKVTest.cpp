#include "leanstore/btree/BasicKV.hpp"

#include "leanstore-c/StoreOption.h"
#include "leanstore/LeanStore.hpp"
#include "leanstore/btree/TransactionKV.hpp"
#include "leanstore/buffer-manager/BufferManager.hpp"
#include "leanstore/concurrency/CRManager.hpp"
#include "leanstore/utils/Defer.hpp"

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
    StoreOption* option = CreateStoreOption(getTestDataDir().c_str());
    option->mWorkerThreads = 2;
    option->mEnableEagerGc = true;
    auto res = LeanStore::Open(option);
    ASSERT_TRUE(res);

    mStore = std::move(res.value());
  }

private:
  std::string getTestDataDir() {
    auto* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
    return std::string("/tmp/leanstore/") + curTest->test_case_name() + "_" + curTest->name();
  }
};

TEST_F(BasicKVTest, BasicKVCreate) {
  // create leanstore btree for table records
  const auto* btreeName = "testTree1";

  mStore->ExecSync(0, [&]() {
    auto res = mStore->CreateBasicKV(btreeName);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);
  });

  // create btree with same should fail in the same worker
  mStore->ExecSync(0, [&]() {
    auto res = mStore->CreateBasicKV(btreeName);
    EXPECT_FALSE(res);
  });

  // create btree with same should also fail in other workers
  mStore->ExecSync(1, [&]() {
    auto res = mStore->CreateBasicKV(btreeName);
    EXPECT_FALSE(res);
  });

  // create btree with another different name should success
  btreeName = "testTree2";
  mStore->ExecSync(0, [&]() {
    auto res = mStore->CreateBasicKV(btreeName);
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
  mStore->ExecSync(0, [&]() {
    auto res = mStore->CreateBasicKV(btreeName);
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
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copyValueOut),
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
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copyValueOut),
                OpCode::kOK);
      EXPECT_EQ(copiedValue, expectedVal);
    }
  });
}

TEST_F(BasicKVTest, BasicKVInsertDuplicatedKey) {
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

  mStore->ExecSync(0, [&]() {
    auto res = mStore->CreateBasicKV(btreeName);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);

    btree = res.value();
    cr::Worker::My().StartTx();
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)val.data(), val.size())),
                OpCode::kOK);
    }
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    // query the keys
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, expectedVal] = kvToTest[i];
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copyValueOut),
                OpCode::kOK);
      EXPECT_EQ(copiedValue, expectedVal);
    }
    // it will failed when insert duplicated keys
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)val.data(), val.size())),
                OpCode::kDuplicated);
    }
    cr::Worker::My().CommitTx();
  });

  // insert duplicated keys in another worker
  mStore->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    // duplicated keys will failed
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)val.data(), val.size())),
                OpCode::kDuplicated);
    }
  });
}
TEST_F(BasicKVTest, BasicKVScanAscAndScanDesc) {
  storage::btree::BasicKV* btree;
  // prepare key-value pairs to insert
  size_t numKVs(10);
  std::vector<std::tuple<std::string, std::string>> kvToTest;
  const auto keySize = sizeof(size_t);
  uint8_t keyBuffer[keySize];
  for (size_t i = 0; i < numKVs; ++i) {
    utils::Fold(keyBuffer, i);
    std::string key("key_btree_LL_xxxxxxxxxxxx_" +
                    std::string(reinterpret_cast<char*>(keyBuffer), keySize));
    std::string val("VAL_BTREE_LL_YYYYYYYYYYYY_" +
                    std::string(reinterpret_cast<char*>(keyBuffer), keySize));
    kvToTest.push_back(std::make_tuple(key, val));
  }

  // create leanstore btree for table records
  auto* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
  auto btreeName = std::string(curTest->test_case_name()) + "_" + std::string(curTest->name());

  mStore->ExecSync(0, [&]() {
    auto res = mStore->CreateBasicKV(btreeName);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);

    btree = res.value();
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->Insert(Slice(key), Slice(val)), OpCode::kOK);
    }

    std::vector<std::tuple<std::string, std::string>> copiedKeyValue;
    auto copyKeyAndValueOut = [&](Slice key, Slice val) {
      copiedKeyValue.emplace_back(key.ToString(), val.ToString());
      return true;
    };

    size_t startIndex = 5;
    auto startKey = Slice(std::get<0>(kvToTest[startIndex]));
    auto callbackReturnFalse = [&]([[maybe_unused]] Slice key, [[maybe_unused]] Slice val) {
      return false;
    };

    // ScanAsc
    {
      // no bigger than largestLexicographicalOrderKey in ScanAsc should return OpCode::kNotFound
      Slice largestLexicographicalOrderKey("zzzzzzz");
      EXPECT_EQ(btree->ScanAsc(largestLexicographicalOrderKey, copyKeyAndValueOut),
                OpCode::kNotFound);
      EXPECT_EQ(btree->ScanAsc(largestLexicographicalOrderKey, callbackReturnFalse),
                OpCode::kNotFound);

      // callback return false should terminate scan
      EXPECT_EQ(btree->ScanAsc(startKey, callbackReturnFalse), OpCode::kOK);

      // query on ScanAsc
      EXPECT_EQ(btree->ScanAsc(startKey, copyKeyAndValueOut), OpCode::kOK);
      EXPECT_EQ(copiedKeyValue.size(), 5);
      for (size_t i = startIndex, j = 0; i < numKVs && j < copiedKeyValue.size(); i++, j++) {
        const auto& [key, expectedVal] = kvToTest[i];
        const auto& [copiedKey, copiedValue] = copiedKeyValue[j];
        EXPECT_EQ(copiedKey, key);
        EXPECT_EQ(copiedValue, expectedVal);
      }
    }

    // ScanDesc
    {
      // no smaller than key in ScanDesc should return OpCode::kNotFound
      Slice smallestLexicographicalOrderKey("aaaaaaaa");
      EXPECT_EQ(btree->ScanDesc(smallestLexicographicalOrderKey, copyKeyAndValueOut),
                OpCode::kNotFound);
      EXPECT_EQ(btree->ScanDesc(smallestLexicographicalOrderKey, callbackReturnFalse),
                OpCode::kNotFound);

      // callback return false should terminate scan
      EXPECT_EQ(btree->ScanDesc(startKey, callbackReturnFalse), OpCode::kOK);

      // query on ScanDesc
      copiedKeyValue.clear();
      EXPECT_EQ(btree->ScanDesc(startKey, copyKeyAndValueOut), OpCode::kOK);
      EXPECT_EQ(copiedKeyValue.size(), 6);
      for (int i = startIndex, j = 0; i >= 0 && j < static_cast<int>(copiedKeyValue.size());
           i--, j++) {
        const auto& [key, expectedVal] = kvToTest[i];
        const auto& [copiedKey, copiedValue] = copiedKeyValue[j];
        EXPECT_EQ(copiedKey, key);
        EXPECT_EQ(copiedValue, expectedVal);
      }
    }
  });
}
} // namespace leanstore::test
