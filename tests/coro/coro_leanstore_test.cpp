#include "common/lean_test_suite.hpp"
#include "leanstore/c/types.h"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_cursor.hpp"
#include "leanstore/lean_session.hpp"
#include "leanstore/lean_store.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstring>
#include <format>
#include <memory>
#include <string>
#include <tuple>
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

  auto session_opt = store->TryConnect(0);
  ASSERT_TRUE(session_opt.has_value());
  auto session = std::move(session_opt.value());

  auto btree_res = session.CreateBTree(kBtreeName, LEAN_BTREE_TYPE_ATOMIC, kBtreeConfig);
  ASSERT_TRUE(btree_res);
  auto btree = std::move(btree_res.value());

  for (const auto& [key, val] : kv_to_test) {
    auto insert_res = btree.Insert(key, val);
    ASSERT_TRUE(insert_res) << insert_res.error().ToString();
  }

  for (const auto& [key, expected_val] : kv_to_test) {
    auto lookup_res = btree.Lookup(key);
    ASSERT_TRUE(lookup_res) << lookup_res.error().ToString();
    std::string value(reinterpret_cast<const char*>(lookup_res.value().data()),
                      lookup_res.value().size());
    EXPECT_EQ(value, expected_val);
  }

  auto cursor = btree.OpenCursor();
  size_t visited = 0;
  if (cursor.SeekToFirst()) {
    do {
      ++visited;
    } while (cursor.Next());
  }
  EXPECT_EQ(visited, kv_to_test.size());
}

} // namespace leanstore::test
