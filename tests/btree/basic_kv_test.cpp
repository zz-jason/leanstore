#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/common/types.h"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/lean_store.hpp"

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
  std::unique_ptr<LeanStore> store_;

  BasicKVTest() = default;

  ~BasicKVTest() override = default;

  void SetUp() override {
    // Create a leanstore instance for the test case
    lean_store_option* option = lean_store_option_create(GetTestDataDir().c_str());
    option->worker_threads_ = 2;
    auto res = LeanStore::Open(option);
    ASSERT_TRUE(res);

    store_ = std::move(res.value());
    ASSERT_NE(store_, nullptr);
  }

protected:
  std::string GetTestDataDir() {
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    return std::string("/tmp/leanstore/") + cur_test->name();
  }

  std::string GenBtreeName(const std::string& suffix = "") {
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    return std::string(cur_test->name()) + suffix;
  }
};

TEST_F(BasicKVTest, BasicKVCreate) {
  // create leanstore btree for table records
  const auto* btree_name = "testTree1";

  store_->ExecSync(0, [&]() {
    auto res = store_->CreateBasicKv(btree_name);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);
  });

  // create btree with same should fail in the same worker
  store_->ExecSync(0, [&]() {
    auto res = store_->CreateBasicKv(btree_name);
    EXPECT_FALSE(res);
  });

  // create btree with same should also fail in other workers
  store_->ExecSync(1, [&]() {
    auto res = store_->CreateBasicKv(btree_name);
    EXPECT_FALSE(res);
  });

  // create btree with another different name should success
  btree_name = "testTree2";
  store_->ExecSync(0, [&]() {
    auto res = store_->CreateBasicKv(btree_name);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);
  });
}

TEST_F(BasicKVTest, BasicKVInsertAndLookup) {
  BasicKV* btree;

  // prepare key-value pairs to insert
  size_t num_keys(10);
  std::vector<std::tuple<std::string, std::string>> kv_to_test;
  for (size_t i = 0; i < num_keys; ++i) {
    std::string key("key_btree_LL_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val("VAL_BTREE_LL_YYYYYYYYYYYY_" + std::to_string(i));
    kv_to_test.emplace_back(key, val);
  }

  // create leanstore btree for table records
  const auto* btree_name = "testTree1";
  store_->ExecSync(0, [&]() {
    auto res = store_->CreateBasicKv(btree_name);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);

    // insert some values
    btree = res.value();
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, val] = kv_to_test[i];
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)val.data(), val.size())),
                OpCode::kOK);
    }
  });

  // query on the created btree in the same worker
  store_->ExecSync(0, [&]() {
    std::string copied_value;
    auto copy_value_out = [&](Slice val) {
      copied_value = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, expected_val] = kv_to_test[i];
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copy_value_out),
                OpCode::kOK);
      EXPECT_EQ(copied_value, expected_val);
    }
  });

  // query on the created btree in another worker
  store_->ExecSync(1, [&]() {
    std::string copied_value;
    auto copy_value_out = [&](Slice val) {
      copied_value = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, expected_val] = kv_to_test[i];
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copy_value_out),
                OpCode::kOK);
      EXPECT_EQ(copied_value, expected_val);
    }
  });
}

TEST_F(BasicKVTest, BasicKVInsertDuplicatedKey) {
  BasicKV* btree;
  // prepare key-value pairs to insert
  size_t num_keys(10);
  std::vector<std::tuple<std::string, std::string>> kv_to_test;
  for (size_t i = 0; i < num_keys; ++i) {
    std::string key("key_btree_LL_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val("VAL_BTREE_LL_YYYYYYYYYYYY_" + std::to_string(i));
    kv_to_test.emplace_back(key, val);
  }
  // create leanstore btree for table records
  const auto* btree_name = "testTree1";

  store_->ExecSync(0, [&]() {
    auto res = store_->CreateBasicKv(btree_name);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);

    btree = res.value();
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, val] = kv_to_test[i];
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)val.data(), val.size())),
                OpCode::kOK);
    }
    std::string copied_value;
    auto copy_value_out = [&](Slice val) {
      copied_value = std::string((const char*)val.data(), val.size());
    };
    // query the keys
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, expected_val] = kv_to_test[i];
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copy_value_out),
                OpCode::kOK);
      EXPECT_EQ(copied_value, expected_val);
    }
    // it will failed when insert duplicated keys
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, val] = kv_to_test[i];
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)val.data(), val.size())),
                OpCode::kDuplicated);
    }
  });

  // insert duplicated keys in another worker
  store_->ExecSync(1, [&]() {
    // duplicated keys will failed
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, val] = kv_to_test[i];
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)val.data(), val.size())),
                OpCode::kDuplicated);
    }
  });
}

TEST_F(BasicKVTest, BasicKVScanAscAndScanDesc) {
  BasicKV* btree;
  // prepare key-value pairs to insert
  size_t num_keys(10);
  std::vector<std::tuple<std::string, std::string>> kv_to_test;
  const auto key_size = sizeof(size_t);
  uint8_t key_buffer[key_size];
  for (size_t i = 0; i < num_keys; ++i) {
    utils::Fold(key_buffer, i);
    std::string key("key_btree_LL_xxxxxxxxxxxx_" +
                    std::string(reinterpret_cast<char*>(key_buffer), key_size));
    std::string val("VAL_BTREE_LL_YYYYYYYYYYYY_" +
                    std::string(reinterpret_cast<char*>(key_buffer), key_size));
    kv_to_test.emplace_back(key, val);
  }

  // create leanstore btree for table records
  auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
  auto btree_name = std::string(cur_test->test_suite_name()) + "_" + std::string(cur_test->name());

  store_->ExecSync(0, [&]() {
    auto res = store_->CreateBasicKv(btree_name);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);

    btree = res.value();
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, val] = kv_to_test[i];
      EXPECT_EQ(btree->Insert(Slice(key), Slice(val)), OpCode::kOK);
    }

    std::vector<std::tuple<std::string, std::string>> copied_key_value;
    auto copy_key_and_value_out = [&](Slice key, Slice val) {
      copied_key_value.emplace_back(key.ToString(), val.ToString());
      return true;
    };

    size_t start_index = 5;
    auto start_key = Slice(std::get<0>(kv_to_test[start_index]));
    auto callback_return_false = [&]([[maybe_unused]] Slice key, [[maybe_unused]] Slice val) {
      return false;
    };

    // ScanAsc
    {
      // no bigger than largestLexicographicalOrderKey in ScanAsc should return OpCode::kNotFound
      Slice largest_lexicographical_order_key("zzzzzzz");
      EXPECT_EQ(btree->ScanAsc(largest_lexicographical_order_key, copy_key_and_value_out),
                OpCode::kNotFound);
      EXPECT_EQ(btree->ScanAsc(largest_lexicographical_order_key, callback_return_false),
                OpCode::kNotFound);

      // callback return false should terminate scan
      EXPECT_EQ(btree->ScanAsc(start_key, callback_return_false), OpCode::kOK);

      // query on ScanAsc
      EXPECT_EQ(btree->ScanAsc(start_key, copy_key_and_value_out), OpCode::kOK);
      EXPECT_EQ(copied_key_value.size(), 5);
      for (size_t i = start_index, j = 0; i < num_keys && j < copied_key_value.size(); i++, j++) {
        const auto& [key, expected_val] = kv_to_test[i];
        const auto& [copied_key, copied_value] = copied_key_value[j];
        EXPECT_EQ(copied_key, key);
        EXPECT_EQ(copied_value, expected_val);
      }
    }

    // ScanDesc
    {
      // no smaller than key in ScanDesc should return OpCode::kNotFound
      Slice smallest_lexicographical_order_key("aaaaaaaa");
      EXPECT_EQ(btree->ScanDesc(smallest_lexicographical_order_key, copy_key_and_value_out),
                OpCode::kNotFound);
      EXPECT_EQ(btree->ScanDesc(smallest_lexicographical_order_key, callback_return_false),
                OpCode::kNotFound);

      // callback return false should terminate scan
      EXPECT_EQ(btree->ScanDesc(start_key, callback_return_false), OpCode::kOK);

      // query on ScanDesc
      copied_key_value.clear();
      EXPECT_EQ(btree->ScanDesc(start_key, copy_key_and_value_out), OpCode::kOK);
      EXPECT_EQ(copied_key_value.size(), 6);
      for (int i = start_index, j = 0; i >= 0 && j < static_cast<int>(copied_key_value.size());
           i--, j++) {
        const auto& [key, expected_val] = kv_to_test[i];
        const auto& [copied_key, copied_value] = copied_key_value[j];
        EXPECT_EQ(copied_key, key);
        EXPECT_EQ(copied_value, expected_val);
      }
    }
  });
}

TEST_F(BasicKVTest, SameKeyInsertRemoveMultiTimes) {
  // create a basickv
  BasicKV* btree;
  store_->ExecSync(0, [&]() {
    auto res = store_->CreateBasicKv(GenBtreeName("_tree1"));
    ASSERT_TRUE(res);
    ASSERT_NE(res.value(), nullptr);
    btree = res.value();
  });

  // insert 1000 key-values to the btree
  size_t num_keys(1000);
  std::vector<std::pair<std::string, std::string>> kv_to_test;
  for (size_t i = 0; i < num_keys; ++i) {
    std::string key("key_" + std::to_string(i) + std::string(10, 'x'));
    std::string val("val_" + std::to_string(i) + std::string(200, 'x'));
    store_->ExecSync(0, [&]() { EXPECT_EQ(btree->Insert(key, val), OpCode::kOK); });
    kv_to_test.emplace_back(std::move(key), std::move(val));
  }

  // 1. remove the key-values from the btree
  // 2. insert the key-values to the btree again
  const auto& [key, val] = kv_to_test[num_keys / 2];
  std::atomic<bool> stop{false};

  std::thread t1([&]() {
    while (!stop) {
      store_->ExecSync(0, [&]() { btree->Remove(key); });
      store_->ExecSync(0, [&]() { btree->Insert(key, val); });
    }
  });

  std::thread t2([&]() {
    std::string copied_value;
    auto copy_value_out = [&](Slice val_slice) {
      copied_value = std::string((const char*)val_slice.data(), val_slice.size());
      EXPECT_EQ(copied_value, val);
    };
    while (!stop) {
      store_->ExecSync(0, [&]() { btree->Lookup(key, std::move(copy_value_out)); });
    }
  });

  // sleep for 2 seconds
  std::this_thread::sleep_for(std::chrono::seconds(2));
  stop = true;
  t1.join();
  t2.join();

  // count the key-values in the btree
  store_->ExecSync(0, [&]() { EXPECT_EQ(btree->CountEntries(), num_keys); });
}

TEST_F(BasicKVTest, PrefixLookup) {
  BasicKV* btree;
  store_->ExecSync(0, [&]() {
    auto res = store_->CreateBasicKv(GenBtreeName("_tree1"));
    ASSERT_TRUE(res);
    ASSERT_NE(res.value(), nullptr);
    btree = res.value();
  });
  // callback function
  std::vector<std::tuple<std::string, std::string>> copied_key_value;
  auto copy_key_and_value_out = [&](Slice key, Slice val) {
    copied_key_value.emplace_back(key.ToString(), val.ToString());
  };

  {
    // not found valid prefix key
    std::string prefix_string("key_");
    auto prefix_key = Slice(prefix_string);
    EXPECT_EQ(btree->PrefixLookup(prefix_key, copy_key_and_value_out), OpCode::kNotFound);
  }

  {
    // insert key and value
    size_t num_keys(10);
    std::vector<std::pair<std::string, std::string>> kv_to_test;
    for (size_t i = 0; i < num_keys; ++i) {
      std::string key("key_" + std::string(10, 'x') + std::to_string(i));
      std::string val("val_" + std::string(100, 'x') + std::to_string(i));
      store_->ExecSync(0, [&]() { EXPECT_EQ(btree->Insert(key, val), OpCode::kOK); });
      kv_to_test.emplace_back(std::move(key), std::move(val));
    }

    // prefix lookup the full set
    auto prefix_string("key_" + std::string(10, 'x'));
    auto prefix_key = Slice(prefix_string);
    EXPECT_EQ(btree->PrefixLookup(prefix_key, copy_key_and_value_out), OpCode::kOK);
    EXPECT_EQ(copied_key_value.size(), kv_to_test.size());
    for (size_t i = 0; i < copied_key_value.size(); i++) {
      const auto& [key, expected_val] = kv_to_test[i];
      const auto& [copied_key, copied_value] = copied_key_value[i];
      EXPECT_EQ(copied_key, key);
      EXPECT_EQ(copied_value, expected_val);
    }

    // insert special key for prefix lookup
    std::vector<std::pair<std::string, std::string>> kv_to_test2;
    for (size_t i = 0; i < num_keys; ++i) {
      std::string key("prefix_key_" + std::string(10, 'x') + std::to_string(i));
      std::string val("prefix_value_" + std::string(100, 'x') + std::to_string(i));
      store_->ExecSync(0, [&]() { EXPECT_EQ(btree->Insert(key, val), OpCode::kOK); });
      kv_to_test2.emplace_back(std::move(key), std::move(val));
    }

    // prefix lookup the partial set
    copied_key_value.clear();
    auto prefix_string2("prefix_key_" + std::string(10, 'x'));
    auto prefix_key2 = Slice(prefix_string2);
    EXPECT_EQ(btree->PrefixLookup(prefix_key2, copy_key_and_value_out), OpCode::kOK);
    EXPECT_EQ(copied_key_value.size(), kv_to_test2.size());
    for (size_t i = 0; i < copied_key_value.size(); i++) {
      const auto& [key, expected_val] = kv_to_test2[i];
      const auto& [copied_key, copied_value] = copied_key_value[i];
      EXPECT_EQ(copied_key, key);
      EXPECT_EQ(copied_value, expected_val);
    }
  }
  {
    // greater than the prefix key, but not a true prefix
    copied_key_value.clear();
    auto prefix_string("prefix_kex_" + std::string(10, 'w'));
    auto prefix_key = Slice(prefix_string);
    EXPECT_EQ(btree->PrefixLookup(prefix_key, copy_key_and_value_out), OpCode::kNotFound);
  }
}

} // namespace leanstore::test
