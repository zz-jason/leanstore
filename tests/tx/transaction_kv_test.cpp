#include "common/lean_test_suite.hpp"
#include "leanstore/c/types.h"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_session.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/random_generator.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <format>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
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

class TransactionKVTest : public LeanTestSuite {
protected:
  std::unique_ptr<LeanStore> store_;
  Optional<LeanSession> s0_;
  Optional<LeanSession> s1_;
  Optional<LeanSession> s2_;

  void SetUp() override {
    auto* option = lean_store_option_create(TestCaseStoreDir().c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 3;
    option->enable_eager_gc_ = true;
    auto opened = LeanStore::Open(option);
    ASSERT_TRUE(opened);
    store_ = std::move(opened.value());

    s0_ = store_->Connect();
    s1_ = store_->Connect();
    s2_ = store_->Connect();
  }
};

TEST_F(TransactionKVTest, Create) {
  auto c1 = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_MVCC);
  ASSERT_TRUE(c1);

  auto c2 = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_MVCC);
  ASSERT_FALSE(c2);

  auto c3 = s1_->CreateBTree("testTree1", LEAN_BTREE_TYPE_MVCC);
  ASSERT_FALSE(c3);

  auto c4 = s0_->CreateBTree("testTree2", LEAN_BTREE_TYPE_MVCC);
  ASSERT_TRUE(c4);
}

TEST_F(TransactionKVTest, InsertAndLookup) {
  auto created = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_MVCC);
  ASSERT_TRUE(created);
  auto t0 = std::move(created.value());
  auto t1res = s1_->GetBTree("testTree1");
  ASSERT_TRUE(t1res);
  auto t1 = std::move(t1res.value());

  std::vector<std::pair<std::string, std::string>> kv;
  for (size_t i = 0; i < 10; ++i) {
    kv.emplace_back("key_" + std::to_string(i), "val_" + std::to_string(i));
  }

  s0_->StartTx();
  for (const auto& [k, v] : kv) {
    ASSERT_TRUE(t0.Insert(k, v));
  }
  s0_->CommitTx();

  s0_->StartTx();
  for (const auto& [k, v] : kv) {
    ASSERT_EQ(ReadString(t0, k), Optional<std::string>(v));
  }
  s0_->CommitTx();

  s1_->StartTx();
  for (const auto& [k, v] : kv) {
    ASSERT_EQ(ReadString(t1, k), Optional<std::string>(v));
  }
  s1_->CommitTx();
}

TEST_F(TransactionKVTest, Insert1000KVs) {
  auto created = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_MVCC);
  ASSERT_TRUE(created);
  auto t0 = std::move(created.value());

  std::set<std::string> unique_keys;
  s0_->StartTx();
  while (unique_keys.size() < 1000) {
    auto key = utils::RandomGenerator::RandAlphString(24);
    if (!unique_keys.insert(key).second) {
      continue;
    }
    ASSERT_TRUE(t0.Insert(key, utils::RandomGenerator::RandAlphString(64)));
  }
  s0_->CommitTx();
}

TEST_F(TransactionKVTest, InsertDuplicates) {
  auto created = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_MVCC);
  ASSERT_TRUE(created);
  auto t0 = std::move(created.value());

  std::set<std::string> keys;
  while (keys.size() < 100) {
    keys.insert(utils::RandomGenerator::RandAlphString(24));
  }

  s0_->StartTx();
  for (const auto& k : keys) {
    ASSERT_TRUE(t0.Insert(k, "v"));
  }
  s0_->CommitTx();

  s0_->StartTx();
  for (const auto& k : keys) {
    ASSERT_FALSE(t0.Insert(k, "v2"));
  }
  s0_->CommitTx();
}

TEST_F(TransactionKVTest, Remove) {
  auto created = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_MVCC);
  ASSERT_TRUE(created);
  auto t0 = std::move(created.value());

  std::set<std::string> keys;
  while (keys.size() < 100) {
    keys.insert(utils::RandomGenerator::RandAlphString(24));
  }

  s0_->StartTx();
  for (const auto& k : keys) {
    ASSERT_TRUE(t0.Insert(k, "v"));
  }
  s0_->CommitTx();

  s0_->StartTx();
  for (const auto& k : keys) {
    ASSERT_TRUE(t0.Remove(k));
  }
  s0_->CommitTx();

  s0_->StartTx();
  for (const auto& k : keys) {
    ASSERT_FALSE(t0.Lookup(k));
  }
  s0_->CommitTx();
}

TEST_F(TransactionKVTest, RemoveNotExisted) {
  auto created = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_MVCC);
  ASSERT_TRUE(created);
  auto t0 = std::move(created.value());

  s0_->StartTx();
  for (int i = 0; i < 100; ++i) {
    ASSERT_FALSE(t0.Remove("k" + std::to_string(i)));
  }
  s0_->CommitTx();
}

TEST_F(TransactionKVTest, RemoveFromOthers) {
  auto created = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_MVCC);
  ASSERT_TRUE(created);
  auto t0 = std::move(created.value());
  auto t1res = s1_->GetBTree("testTree1");
  ASSERT_TRUE(t1res);
  auto t1 = std::move(t1res.value());

  std::set<std::string> keys;
  while (keys.size() < 100) {
    keys.insert(utils::RandomGenerator::RandAlphString(24));
  }

  s0_->StartTx();
  for (const auto& k : keys) {
    ASSERT_TRUE(t0.Insert(k, "v"));
  }
  s0_->CommitTx();

  s1_->StartTx();
  for (const auto& k : keys) {
    ASSERT_TRUE(t1.Remove(k));
  }
  s1_->CommitTx();

  s0_->StartTx();
  for (const auto& k : keys) {
    ASSERT_FALSE(t0.Lookup(k));
  }
  s0_->CommitTx();
}

TEST_F(TransactionKVTest, Update) {
  auto created = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_MVCC);
  ASSERT_TRUE(created);
  auto t0 = std::move(created.value());

  std::vector<std::string> keys;
  for (size_t i = 0; i < 100; ++i) {
    keys.emplace_back(utils::RandomGenerator::RandAlphString(24));
  }

  s0_->StartTx();
  for (const auto& k : keys) {
    ASSERT_TRUE(t0.Insert(k, "old"));
  }
  s0_->CommitTx();

  s0_->StartTx();
  for (const auto& k : keys) {
    ASSERT_TRUE(t0.Update(k, "new"));
  }
  s0_->CommitTx();

  s0_->StartTx();
  for (const auto& k : keys) {
    ASSERT_EQ(ReadString(t0, k), Optional<std::string>("new"));
  }
  s0_->CommitTx();
}

TEST_F(TransactionKVTest, ScanAsc) {
  auto created = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_MVCC);
  ASSERT_TRUE(created);
  auto t0 = std::move(created.value());

  s0_->StartTx();
  for (int i = 0; i < 100; ++i) {
    ASSERT_TRUE(t0.Insert(std::format("k{:03d}", i), std::format("v{:03d}", i)));
  }
  s0_->CommitTx();

  s0_->StartTx();
  for (int expected = 0; expected < 100; ++expected) {
    ASSERT_EQ(ReadString(t0, std::format("k{:03d}", expected)),
              Optional<std::string>(std::format("v{:03d}", expected)));
  }
  s0_->CommitTx();
}

TEST_F(TransactionKVTest, ScanDesc) {
  auto created = s0_->CreateBTree("testTree1", LEAN_BTREE_TYPE_MVCC);
  ASSERT_TRUE(created);
  auto t0 = std::move(created.value());

  s0_->StartTx();
  for (int i = 0; i < 100; ++i) {
    ASSERT_TRUE(t0.Insert(std::format("k{:03d}", i), std::format("v{:03d}", i)));
  }
  s0_->CommitTx();

  s0_->StartTx();
  for (int expected = 99; expected >= 0; --expected) {
    ASSERT_EQ(ReadString(t0, std::format("k{:03d}", expected)),
              Optional<std::string>(std::format("v{:03d}", expected)));
  }
  s0_->CommitTx();
}

TEST_F(TransactionKVTest, InsertAfterRemove) {
  auto created = s0_->CreateBTree("InsertAfterRemove", LEAN_BTREE_TYPE_MVCC);
  ASSERT_TRUE(created);
  auto t0 = std::move(created.value());

  s0_->StartTx();
  ASSERT_TRUE(t0.Insert("k", "v"));
  s0_->CommitTx();

  s0_->StartTx();
  ASSERT_TRUE(t0.Remove("k"));
  ASSERT_FALSE(t0.Remove("k"));
  ASSERT_FALSE(t0.Lookup("k"));
  ASSERT_TRUE(t0.Insert("k", "new"));
  ASSERT_EQ(ReadString(t0, "k"), Optional<std::string>("new"));
  s0_->CommitTx();
}

TEST_F(TransactionKVTest, InsertAfterRemoveDifferentWorkers) {
  auto created = s0_->CreateBTree("InsertAfterRemoveDifferentWorkers", LEAN_BTREE_TYPE_MVCC);
  ASSERT_TRUE(created);
  auto t0 = std::move(created.value());
  auto t1res = s1_->GetBTree("InsertAfterRemoveDifferentWorkers");
  ASSERT_TRUE(t1res);
  auto t1 = std::move(t1res.value());

  s0_->StartTx();
  ASSERT_TRUE(t0.Insert("k", "v"));
  ASSERT_TRUE(t0.Remove("k"));
  s0_->CommitTx();

  s1_->StartTx();
  ASSERT_FALSE(t1.Remove("k"));
  ASSERT_TRUE(t1.Insert("k", "new"));
  ASSERT_EQ(ReadString(t1, "k"), Optional<std::string>("new"));
  s1_->CommitTx();
}

TEST_F(TransactionKVTest, ConcurrentInsertWithSplit) {
  auto created = s0_->CreateBTree("ConcurrentInsertWithSplit", LEAN_BTREE_TYPE_MVCC);
  ASSERT_TRUE(created);

  std::atomic<bool> stop = false;
  std::atomic<int> inserted = 0;

  std::thread th0([&]() {
    auto session = store_->Connect();
    auto tree_res = session.GetBTree("ConcurrentInsertWithSplit");
    ASSERT_TRUE(tree_res);
    auto tree = std::move(tree_res.value());
    int i = 0;
    while (!stop.load()) {
      session.StartTx();
      (void)tree.Insert(std::format("{}_{}", i, 0), "v");
      session.CommitTx();
      inserted.fetch_add(1);
      i++;
    }
  });

  std::thread th1([&]() {
    auto session = store_->Connect();
    auto tree_res = session.GetBTree("ConcurrentInsertWithSplit");
    ASSERT_TRUE(tree_res);
    auto tree = std::move(tree_res.value());
    int i = 0;
    while (!stop.load()) {
      session.StartTx();
      (void)tree.Insert(std::format("{}_{}", i, 1), "v");
      session.CommitTx();
      inserted.fetch_add(1);
      i++;
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  stop.store(true);
  th0.join();
  th1.join();
  ASSERT_GT(inserted.load(), 0);
}

} // namespace leanstore::test
