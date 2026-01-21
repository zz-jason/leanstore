#include "common/lean_test_suite.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/buffer/buffer_manager.hpp"
#include "leanstore/c/types.h"
#include "leanstore/coro/coro_executor.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/tx/cr_manager.hpp"
#include "leanstore/tx/transaction_kv.hpp"

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

class CoroLeanStoreTest : public LeanTestSuite {};

TEST_F(CoroLeanStoreTest, BasicKv) {
  static constexpr auto kBtreeName = "test_btree";
  static constexpr auto kNumKeys = 100;
  static constexpr auto kKeyPattern = "key_btree_LL_xxxxxxxxxxxx_{}";
  static constexpr auto kValPattern = "VAL_BTREE_LL_YYYYYYYYYYYY_{}";
  static constexpr auto kEnableWal = true;
  static constexpr auto kBtreeConfig = lean_btree_config{
      .enable_wal_ = kEnableWal,
      .use_bulk_insert_ = false,
  };

  lean_store_option* option = lean_store_option_create(TestCaseStoreDir().c_str());
  option->create_from_scratch_ = true;
  option->enable_wal_ = kEnableWal;
  option->worker_threads_ = 2;
  auto res = LeanStore::Open(option);
  ASSERT_TRUE(res);

  auto store = std::move(res.value());
  ASSERT_NE(store, nullptr);

  // prepare key-value pairs to insert
  std::vector<std::tuple<std::string, std::string>> kv_to_test;
  for (size_t i = 0; i < kNumKeys; ++i) {
    std::string key(std::format(kKeyPattern, i));
    std::string val(std::format(kValPattern, i));
    kv_to_test.emplace_back(key, val);
  }

  // create leanstore btree for table records
  BasicKV* btree;
  auto* coro_session_0 = store->GetCoroScheduler().TryReserveCoroSession(0);
  assert(coro_session_0 != nullptr && "Failed to reserve a CoroSession for coroutine execution");
  auto job_init_btree = [&]() {
    // create btree
    auto res = store->CreateBasicKv(kBtreeName, kBtreeConfig);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);

    // insert some values
    btree = res.value();
    for (const auto& [key, val] : kv_to_test) {
      EXPECT_EQ(btree->Insert(key, val), OpCode::kOK);
    }
  };
  store->GetCoroScheduler().Submit(coro_session_0, std::move(job_init_btree))->Wait();

  // read back the values
  auto* coro_session_1 = store->GetCoroScheduler().TryReserveCoroSession(1);
  assert(coro_session_1 != nullptr && "Failed to reserve a CoroSession for coroutine execution");
  auto job_lookup = [&]() {
    std::string copied_value;
    auto copy_value_out = [&](Slice val) {
      copied_value = std::string((const char*)val.data(), val.size());
    };
    for (const auto& [key, expected_val] : kv_to_test) {
      EXPECT_EQ(btree->Lookup(key, copy_value_out), OpCode::kOK);
      EXPECT_EQ(copied_value, expected_val);
    }
  };
  store->GetCoroScheduler().Submit(coro_session_1, std::move(job_lookup))->Wait();

  store->GetCoroScheduler().ReleaseCoroSession(coro_session_0);
  store->GetCoroScheduler().ReleaseCoroSession(coro_session_1);
}

} // namespace leanstore::test