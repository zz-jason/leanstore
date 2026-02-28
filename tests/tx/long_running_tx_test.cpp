#include "common/lean_test_suite.hpp"
#include "leanstore/c/types.h"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_session.hpp"
#include "leanstore/lean_store.hpp"

#include <gtest/gtest.h>

#include <format>
#include <memory>
#include <optional>
#include <string>

namespace leanstore::test {

namespace {
auto ReadString(LeanBTree& tree, const std::string& key) -> std::optional<std::string> {
  auto res = tree.Lookup(key);
  if (!res) {
    return std::nullopt;
  }
  return std::string(reinterpret_cast<const char*>(res.value().data()), res.value().size());
}
} // namespace

class LongRunningTxTest : public LeanTestSuite {
protected:
  std::unique_ptr<LeanStore> store_;
  std::optional<LeanSession> s0_;
  std::optional<LeanSession> s1_;
  std::optional<LeanSession> s2_;
  std::optional<LeanBTree> t0_;
  std::optional<LeanBTree> t1_;
  std::optional<LeanBTree> t2_;

  void SetUp() override {
    auto* option = lean_store_option_create(TestCaseStoreDir().c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 3;
    option->enable_eager_gc_ = true;
    auto opened = LeanStore::Open(option);
    ASSERT_TRUE(opened);
    store_ = std::move(opened.value());

    s0_.emplace(store_->Connect(0));
    s1_.emplace(store_->Connect(1));
    s2_.emplace(store_->Connect(2));

    auto created = s0_->CreateBTree("long_tx", LEAN_BTREE_TYPE_MVCC);
    ASSERT_TRUE(created);
    t0_.emplace(std::move(created.value()));
    auto g1 = s1_->GetBTree("long_tx");
    auto g2 = s2_->GetBTree("long_tx");
    ASSERT_TRUE(g1);
    ASSERT_TRUE(g2);
    t1_.emplace(std::move(g1.value()));
    t2_.emplace(std::move(g2.value()));
  }
};

TEST_F(LongRunningTxTest, LookupFromGraveyard) {
  s1_->StartTx();
  ASSERT_TRUE(t1_->Insert("1", "10"));
  ASSERT_TRUE(t1_->Insert("2", "20"));
  s1_->CommitTx();

  s2_->StartTx();
  ASSERT_EQ(ReadString(*t2_, "1"), std::optional<std::string>("10"));
  ASSERT_EQ(ReadString(*t2_, "2"), std::optional<std::string>("20"));

  s1_->StartTx();
  ASSERT_TRUE(t1_->Remove("1"));
  ASSERT_TRUE(t1_->Remove("2"));
  s1_->CommitTx();

  ASSERT_EQ(ReadString(*t2_, "1"), std::optional<std::string>("10"));
  ASSERT_EQ(ReadString(*t2_, "2"), std::optional<std::string>("20"));
  s2_->CommitTx();
}

TEST_F(LongRunningTxTest, LookupAfterUpdate100Times) {
  s1_->StartTx();
  ASSERT_TRUE(t1_->Insert("1", "10"));
  ASSERT_TRUE(t1_->Insert("2", "20"));
  s1_->CommitTx();

  s2_->StartTx();
  ASSERT_EQ(ReadString(*t2_, "1"), std::optional<std::string>("10"));

  s1_->StartTx();
  for (size_t i = 0; i < 100; ++i) {
    ASSERT_TRUE(t1_->Remove("1"));
    ASSERT_TRUE(t1_->Insert("1", std::format("{:02d}", static_cast<int>(i))));
  }
  s1_->CommitTx();

  ASSERT_EQ(ReadString(*t2_, "1"), std::optional<std::string>("10"));
  s2_->CommitTx();

  s2_->StartTx();
  ASSERT_EQ(ReadString(*t2_, "1"), std::optional<std::string>("99"));
  s2_->CommitTx();
}

TEST_F(LongRunningTxTest, ScanAscFromGraveyard) {
  s1_->StartTx();
  for (int i = 0; i < 20; ++i) {
    ASSERT_TRUE(t1_->Insert(std::format("k{:02d}", i), std::format("v{:02d}", i)));
  }
  s1_->CommitTx();

  s2_->StartTx();
  size_t first_snapshot_count = 0;
  for (int i = 0; i < 20; ++i) {
    if (ReadString(*t2_, std::format("k{:02d}", i)).has_value()) {
      first_snapshot_count++;
    }
  }
  ASSERT_EQ(first_snapshot_count, 20u);

  s1_->StartTx();
  for (int i = 0; i < 20; ++i) {
    ASSERT_TRUE(t1_->Remove(std::format("k{:02d}", i)));
  }
  s1_->CommitTx();

  size_t second_snapshot_count = 0;
  for (int i = 0; i < 20; ++i) {
    if (ReadString(*t2_, std::format("k{:02d}", i)).has_value()) {
      second_snapshot_count++;
    }
  }
  ASSERT_EQ(second_snapshot_count, 20u);
  s2_->CommitTx();
}

} // namespace leanstore::test
