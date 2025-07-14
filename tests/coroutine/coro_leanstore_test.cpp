#include "leanstore-c/store_option.h"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/log.hpp"
#include "utils/coroutine/coro_executor.hpp"

#include <gtest/gtest.h>

#include <cassert>
#include <cstddef>
#include <cstring>
#include <format>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <sys/types.h>

namespace leanstore::test {

class CoroLeanStoreTest : public ::testing::Test {
protected:
  std::string GetTestDataDir() {
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    auto cur_test_name = std::format("{}_{}", cur_test->test_case_name(), cur_test->name());
    return std::format("/tmp/leanstore/{}", cur_test_name);
  }
};

TEST_F(CoroLeanStoreTest, BasicKv) {
  StoreOption* option = CreateStoreOption(GetTestDataDir().c_str());
  option->create_from_scratch_ = true;
  option->enable_wal_ = false;
  option->worker_threads_ = 2;
  auto res = LeanStore::Open(option);
  ASSERT_TRUE(res);

  auto store = std::move(res.value());
  ASSERT_NE(store, nullptr);

  // prepare key-value pairs to insert
  static constexpr size_t kNumKeys = 10;
  std::vector<std::tuple<std::string, std::string>> kv_to_test;
  for (size_t i = 0; i < kNumKeys; ++i) {
    std::string key("key_btree_LL_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val("VAL_BTREE_LL_YYYYYYYYYYYY_" + std::to_string(i));
    kv_to_test.push_back(std::make_tuple(key, val));
  }

  // create leanstore btree for table records
  storage::btree::BasicKV* btree;
  const auto* btree_name = "testTree1";
  BTreeConfig btree_config{.enable_wal_ = false, .use_bulk_insert_ = false};
  auto future1 = store->Submit(
      [&]() {
        auto res = store->CreateBasicKv(btree_name, btree_config);
        EXPECT_TRUE(res);
        EXPECT_NE(res.value(), nullptr);
        Log::Info("Created BasicKV btree, name={}", btree_name);

        // insert some values
        btree = res.value();
        for (const auto& [key, val] : kv_to_test) {
          EXPECT_EQ(btree->Insert(key, val), OpCode::kOK);
        }
        Log::Info("Inserted {} key-value pairs into BasicKV btree, name={}", kv_to_test.size(),
                  btree_name);
      },
      0);
  future1->Wait();

  auto future2 = store->Submit(
      [&]() {
        std::string copied_value;
        auto copy_value_out = [&](Slice val) {
          copied_value = std::string((const char*)val.data(), val.size());
        };
        for (const auto& [key, expected_val] : kv_to_test) {
          EXPECT_EQ(btree->Lookup(key, copy_value_out), OpCode::kOK);
          EXPECT_EQ(copied_value, expected_val);
        }
      },
      1);
  future2->Wait();
}

} // namespace leanstore::test