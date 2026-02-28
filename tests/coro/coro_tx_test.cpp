#include "common/lean_test_suite.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/buffer/buffer_manager.hpp"
#include "leanstore/c/types.h"
#include "leanstore/coro/coro_env.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_session.hpp"
#include "leanstore/lean_store.hpp"

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

class CoroTxTest : public LeanTestSuite {
protected:
  static constexpr auto kTestDirPattern = "/tmp/leanstore/{}/{}";
  static constexpr auto kBtreeName = "coro_txn_test";
  static constexpr auto kNumKeys = 100;
  static constexpr auto kKeyPattern = "key_btree_LL_xxxxxxxxxxxx_{}";
  static constexpr auto kValPattern = "VAL_BTREE_LL_YYYYYYYYYYYY_{}";
  static constexpr auto kEnableWal = true;
  static constexpr auto kBtreeConfig = lean_btree_config{
      .enable_wal_ = kEnableWal,
      .use_bulk_insert_ = false,
  };
};

TEST_F(CoroTxTest, BasicCommit) {
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

  // create btree for table records
  auto s0 = store->Connect(0);
  auto create_res = s0.CreateBTree(kBtreeName, lean_btree_type::LEAN_BTREE_TYPE_MVCC, kBtreeConfig);
  ASSERT_TRUE(create_res);
  auto btree = std::move(create_res.value());

  {
    for (const auto& [key, val] : kv_to_test) {
      s0.StartTx();
      EXPECT_TRUE(btree.Insert(key, val));
      s0.CommitTx();
    }
  }
}

TEST_F(CoroTxTest, BasicSnapshotIsolation) {
  lean_store_option* option = lean_store_option_create(TestCaseStoreDir().c_str());
  option->create_from_scratch_ = true;
  option->enable_wal_ = kEnableWal;
  option->worker_threads_ = 2;
  auto res = LeanStore::Open(option);
  ASSERT_TRUE(res);
  auto store = std::move(res.value());
  ASSERT_NE(store, nullptr);

  auto s0 = store->Connect(0);
  auto s1 = store->Connect(1);

  auto create_res = s0.CreateBTree(kBtreeName, lean_btree_type::LEAN_BTREE_TYPE_MVCC, kBtreeConfig);
  ASSERT_TRUE(create_res);

  auto btree0 = std::move(create_res.value());
  auto btree1_res = s1.GetBTree(kBtreeName);
  ASSERT_TRUE(btree1_res);
  auto btree1 = std::move(btree1_res.value());

  s0.StartTx();
  EXPECT_TRUE(btree0.Insert("k1", "v1"));
  EXPECT_TRUE(btree0.Insert("k2", "v2"));
  s0.CommitTx();

  auto lookup_as = [](LeanBTree& tree, Slice key) -> std::string {
    auto res = tree.Lookup(key);
    if (!res) {
      return {};
    }
    auto& value = res.value();
    return std::string(reinterpret_cast<const char*>(value.data()), value.size());
  };

  s0.StartTx();
  EXPECT_EQ(lookup_as(btree0, "k1"), "v1");
  EXPECT_EQ(lookup_as(btree0, "k2"), "v2");

  s1.StartTx();
  EXPECT_TRUE(btree1.Insert("k3", "v3"));
  EXPECT_EQ(lookup_as(btree1, "k3"), "v3");
  s1.CommitTx();

  s1.StartTx();
  EXPECT_EQ(lookup_as(btree1, "k3"), "v3");
  s1.CommitTx();

  EXPECT_FALSE(btree0.Lookup("k3"));
  s0.CommitTx();

  s0.StartTx();
  EXPECT_EQ(lookup_as(btree0, "k3"), "v3");
  s0.CommitTx();
}

} // namespace leanstore::test
