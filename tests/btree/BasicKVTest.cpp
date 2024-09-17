#include "leanstore/btree/BasicKV.hpp"

#include "leanstore-c/StoreOption.h"
#include "leanstore/LeanStore.hpp"
#include "leanstore/btree/TransactionKV.hpp"
#include "leanstore/buffer-manager/BufferManager.hpp"
#include "leanstore/concurrency/CRManager.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <sys/types.h>

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
    auto res = LeanStore::Open(option);
    ASSERT_TRUE(res);

    mStore = std::move(res.value());
    ASSERT_NE(mStore, nullptr);
  }

protected:
  std::string getTestDataDir() {
    auto* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
    return std::string("/tmp/leanstore/") + curTest->name();
  }

  std::string genBtreeName(const std::string& suffix = "") {
    auto* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
    return std::string(curTest->name()) + suffix;
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
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)val.data(), val.size())),
                OpCode::kOK);
    }
  });

  // query on the created btree in the same worker
  mStore->ExecSync(0, [&]() {
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
  });

  // insert duplicated keys in another worker
  mStore->ExecSync(1, [&]() {
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

TEST_F(BasicKVTest, SameKeyInsertRemoveMultiTimes) {
  // create a basickv
  storage::btree::BasicKV* btree;
  mStore->ExecSync(0, [&]() {
    auto res = mStore->CreateBasicKV(genBtreeName("_tree1"));
    ASSERT_TRUE(res);
    ASSERT_NE(res.value(), nullptr);
    btree = res.value();
  });

  // insert 1000 key-values to the btree
  size_t numKVs(1000);
  std::vector<std::pair<std::string, std::string>> kvToTest;
  for (size_t i = 0; i < numKVs; ++i) {
    std::string key("key_" + std::to_string(i) + std::string(10, 'x'));
    std::string val("val_" + std::to_string(i) + std::string(200, 'x'));
    mStore->ExecSync(0, [&]() { EXPECT_EQ(btree->Insert(key, val), OpCode::kOK); });
    kvToTest.emplace_back(std::move(key), std::move(val));
  }

  // 1. remove the key-values from the btree
  // 2. insert the key-values to the btree again
  const auto& [key, val] = kvToTest[numKVs / 2];
  std::atomic<bool> stop{false};

  std::thread t1([&]() {
    while (!stop) {
      mStore->ExecSync(0, [&]() { btree->Remove(key); });
      mStore->ExecSync(0, [&]() { btree->Insert(key, val); });
    }
  });

  std::thread t2([&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice valSlice) {
      copiedValue = std::string((const char*)valSlice.data(), valSlice.size());
      EXPECT_EQ(copiedValue, val);
    };
    while (!stop) {
      mStore->ExecSync(0, [&]() { btree->Lookup(key, std::move(copyValueOut)); });
    }
  });

  // sleep for 2 seconds
  std::this_thread::sleep_for(std::chrono::seconds(2));
  stop = true;
  t1.join();
  t2.join();

  // count the key-values in the btree
  mStore->ExecSync(0, [&]() { EXPECT_EQ(btree->CountEntries(), numKVs); });
}

TEST_F(BasicKVTest, PrefixLookup) {
  storage::btree::BasicKV* btree;
  mStore->ExecSync(0, [&]() {
    auto res = mStore->CreateBasicKV(genBtreeName("_tree1"));
    ASSERT_TRUE(res);
    ASSERT_NE(res.value(), nullptr);
    btree = res.value();
  });
  // callback function
  std::vector<std::tuple<std::string, std::string>> copiedKeyValue;
  auto copyKeyAndValueOut = [&](Slice key, Slice val) {
    copiedKeyValue.emplace_back(key.ToString(), val.ToString());
  };

  {
    // not found valid prefix key
    std::string prefixString("key_");
    auto prefixKey = Slice(prefixString);
    EXPECT_EQ(btree->PrefixLookup(prefixKey, copyKeyAndValueOut), OpCode::kNotFound);
  }

  {
    // insert key and value
    size_t numKVs(10);
    std::vector<std::pair<std::string, std::string>> kvToTest;
    for (size_t i = 0; i < numKVs; ++i) {
      std::string key("key_" + std::string(10, 'x') + std::to_string(i));
      std::string val("val_" + std::string(100, 'x') + std::to_string(i));
      mStore->ExecSync(0, [&]() { EXPECT_EQ(btree->Insert(key, val), OpCode::kOK); });
      kvToTest.emplace_back(std::move(key), std::move(val));
    }

    // prefix lookup the full set
    auto prefixString("key_" + std::string(10, 'x'));
    auto prefixKey = Slice(prefixString);
    EXPECT_EQ(btree->PrefixLookup(prefixKey, copyKeyAndValueOut), OpCode::kOK);
    EXPECT_EQ(copiedKeyValue.size(), kvToTest.size());
    for (size_t i = 0; i < copiedKeyValue.size(); i++) {
      const auto& [key, expectedVal] = kvToTest[i];
      const auto& [copiedKey, copiedValue] = copiedKeyValue[i];
      EXPECT_EQ(copiedKey, key);
      EXPECT_EQ(copiedValue, expectedVal);
    }

    // insert special key for prefix lookup
    std::vector<std::pair<std::string, std::string>> kvToTest2;
    for (size_t i = 0; i < numKVs; ++i) {
      std::string key("prefix_key_" + std::string(10, 'x') + std::to_string(i));
      std::string val("prefix_value_" + std::string(100, 'x') + std::to_string(i));
      mStore->ExecSync(0, [&]() { EXPECT_EQ(btree->Insert(key, val), OpCode::kOK); });
      kvToTest2.emplace_back(std::move(key), std::move(val));
    }

    // prefix lookup the partial set
    copiedKeyValue.clear();
    auto prefixString2("prefix_key_" + std::string(10, 'x'));
    auto prefixKey2 = Slice(prefixString2);
    EXPECT_EQ(btree->PrefixLookup(prefixKey2, copyKeyAndValueOut), OpCode::kOK);
    EXPECT_EQ(copiedKeyValue.size(), kvToTest2.size());
    for (size_t i = 0; i < copiedKeyValue.size(); i++) {
      const auto& [key, expectedVal] = kvToTest2[i];
      const auto& [copiedKey, copiedValue] = copiedKeyValue[i];
      EXPECT_EQ(copiedKey, key);
      EXPECT_EQ(copiedValue, expectedVal);
    }
  }
  {
    // greater than the prefix key, but not a true prefix
    copiedKeyValue.clear();
    auto prefixString("prefix_kex_" + std::string(10, 'w'));
    auto prefixKey = Slice(prefixString);
    EXPECT_EQ(btree->PrefixLookup(prefixKey, copyKeyAndValueOut), OpCode::kNotFound);
  }
}

} // namespace leanstore::test
