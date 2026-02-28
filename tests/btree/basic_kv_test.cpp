#include "common/lean_test_suite.hpp"
#include "leanstore/c/types.h"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_cursor.hpp"
#include "leanstore/lean_session.hpp"
#include "leanstore/lean_store.hpp"

#include <gtest/gtest.h>

#include <format>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace leanstore::test {

namespace {
auto ReadString(LeanBTree& tree, const std::string& key) -> Optional<std::string> {
  auto res = tree.Lookup(key);
  if (!res) {
    return std::nullopt;
  }
  return std::string(reinterpret_cast<const char*>(res.value().data()), res.value().size());
}
} // namespace

class BasicKVTest : public LeanTestSuite {
protected:
  std::unique_ptr<LeanStore> store_;
  Optional<LeanSession> s0_;
  Optional<LeanSession> s1_;

  void SetUp() override {
    auto* option = lean_store_option_create(TestCaseStoreDir().c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 2;
    auto opened = LeanStore::Open(option);
    ASSERT_TRUE(opened);
    store_ = std::move(opened.value());
    s0_ = store_->Connect(0);
    s1_ = store_->Connect(1);
  }
};

TEST_F(BasicKVTest, BasicKVCreate) {
  auto c1 = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_ATOMIC);
  ASSERT_TRUE(c1);
  auto c2 = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_ATOMIC);
  ASSERT_FALSE(c2);
  auto c3 = s1_->CreateBTree("testTree1", LEAN_BTREE_TYPE_ATOMIC);
  ASSERT_FALSE(c3);
  auto c4 = s0_->CreateBTree("testTree2", LEAN_BTREE_TYPE_ATOMIC);
  ASSERT_TRUE(c4);
}

TEST_F(BasicKVTest, BasicKVInsertAndLookup) {
  auto created = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_ATOMIC);
  ASSERT_TRUE(created);
  auto t0 = std::move(created.value());
  auto got = s1_->GetBTree("testTree1");
  ASSERT_TRUE(got);
  auto t1 = std::move(got.value());

  std::vector<std::pair<std::string, std::string>> kv;
  for (size_t i = 0; i < 10; ++i) {
    kv.emplace_back("key_" + std::to_string(i), "val_" + std::to_string(i));
  }

  for (const auto& [k, v] : kv) {
    ASSERT_TRUE(t0.Insert(k, v));
  }
  for (const auto& [k, v] : kv) {
    ASSERT_EQ(*ReadString(t0, k), v);
    ASSERT_EQ(*ReadString(t1, k), v);
  }
}

TEST_F(BasicKVTest, BasicKVInsertDuplicatedKey) {
  auto created = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_ATOMIC);
  ASSERT_TRUE(created);
  auto t0 = std::move(created.value());
  ASSERT_TRUE(t0.Insert("dup", "v1"));
  ASSERT_FALSE(t0.Insert("dup", "v2"));
}

TEST_F(BasicKVTest, BasicKVScanAscAndScanDesc) {
  auto created = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_ATOMIC);
  ASSERT_TRUE(created);
  auto tree = std::move(created.value());
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(tree.Insert(std::format("k{:02d}", i), std::format("v{:02d}", i)));
  }

  auto c1 = tree.OpenCursor();
  int asc = 0;
  if (c1.SeekToFirst()) {
    do {
      asc++;
    } while (c1.Next());
  }
  ASSERT_EQ(asc, 10);

  auto c2 = tree.OpenCursor();
  int desc = 0;
  if (c2.SeekToLast()) {
    do {
      desc++;
    } while (c2.Prev());
  }
  ASSERT_EQ(desc, 10);
}

TEST_F(BasicKVTest, SameKeyInsertRemoveMultiTimes) {
  auto created = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_ATOMIC);
  ASSERT_TRUE(created);
  auto tree = std::move(created.value());

  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_TRUE(tree.Insert("key_" + std::to_string(i), "val_" + std::to_string(i)));
  }

  for (size_t i = 0; i < 100; ++i) {
    ASSERT_TRUE(tree.Remove("key_500"));
    ASSERT_TRUE(tree.Insert("key_500", "val_500"));
  }

  auto cursor = tree.OpenCursor();
  size_t count = 0;
  if (cursor.SeekToFirst()) {
    do {
      count++;
    } while (cursor.Next());
  }
  ASSERT_EQ(count, 1000u);
}

TEST_F(BasicKVTest, PrefixLookup) {
  auto created = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_ATOMIC);
  ASSERT_TRUE(created);
  auto tree = std::move(created.value());

  for (size_t i = 0; i < 10; ++i) {
    ASSERT_TRUE(tree.Insert("prefix_key_" + std::to_string(i), "v" + std::to_string(i)));
  }
  for (size_t i = 0; i < 10; ++i) {
    ASSERT_TRUE(tree.Insert("other_key_" + std::to_string(i), "o" + std::to_string(i)));
  }

  size_t matched = 0;
  for (size_t i = 0; i < 10; ++i) {
    if (tree.Lookup("prefix_key_" + std::to_string(i))) {
      matched++;
    }
  }
  ASSERT_EQ(matched, 10u);
  ASSERT_FALSE(tree.Lookup("prefix_kex_0"));
}

} // namespace leanstore::test
