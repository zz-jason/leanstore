#include "common/lean_test_suite.hpp"
#include "leanstore/c/types.h"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_session.hpp"
#include "leanstore/lean_store.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>

#ifdef LEAN_ENABLE_CORO

namespace leanstore::test {

namespace {
auto ReadString(LeanBTree& tree, const std::string& key) -> std::optional<std::string> {
  auto res = tree.Lookup(key);
  if (!res) {
    return std::nullopt;
  }
  return std::string(reinterpret_cast<const char*>(res.value().data()), res.value().size());
}

void Rewrite(LeanBTree& tree, const std::string& key, const std::string& value) {
  auto old = tree.Remove(key);
  if (!old) {
    (void)tree.Insert(key, value);
    return;
  }
  ASSERT_TRUE(tree.Insert(key, value));
}
} // namespace

class AnomaliesTest : public LeanTestSuite {
protected:
  std::unique_ptr<LeanStore> store_;
  std::optional<LeanSession> s1_;
  std::optional<LeanSession> s2_;
  std::optional<LeanSession> s3_;
  std::optional<LeanBTree> t1_;
  std::optional<LeanBTree> t2_;
  std::optional<LeanBTree> t3_;

  void SetUp() override {
    auto* option = lean_store_option_create(TestCaseStoreDir().c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 3;
    auto opened = LeanStore::Open(option);
    ASSERT_TRUE(opened);
    store_ = std::move(opened.value());

    s1_.emplace(store_->Connect(0));
    s2_.emplace(store_->Connect(1));
    s3_.emplace(store_->Connect(2));

    auto created = s1_->CreateBTree("anomalies", LEAN_BTREE_TYPE_MVCC);
    ASSERT_TRUE(created);
    t1_.emplace(std::move(created.value()));

    auto got2 = s2_->GetBTree("anomalies");
    ASSERT_TRUE(got2);
    t2_.emplace(std::move(got2.value()));
    auto got3 = s3_->GetBTree("anomalies");
    ASSERT_TRUE(got3);
    t3_.emplace(std::move(got3.value()));

    s1_->StartTx();
    ASSERT_TRUE(t1_->Insert("1", "10"));
    ASSERT_TRUE(t1_->Insert("2", "20"));
    s1_->CommitTx();
  }
};

TEST_F(AnomaliesTest, NoG0) {
  s1_->StartTx();
  s2_->StartTx();
  Rewrite(*t1_, "1", "11");
  ASSERT_EQ(ReadString(*t2_, "1"), std::optional<std::string>("10"));
  Rewrite(*t1_, "2", "21");
  s1_->CommitTx();
  s2_->CommitTx();
}

TEST_F(AnomaliesTest, NoG1a) {
  s1_->StartTx();
  Rewrite(*t1_, "1", "11");
  s2_->StartTx();
  ASSERT_EQ(ReadString(*t2_, "1"), std::optional<std::string>("10"));
  s1_->AbortTx();
  ASSERT_EQ(ReadString(*t2_, "1"), std::optional<std::string>("10"));
  s2_->CommitTx();
}

TEST_F(AnomaliesTest, NoG1b) {
  s1_->StartTx();
  Rewrite(*t1_, "1", "11");
  s2_->StartTx();
  ASSERT_EQ(ReadString(*t2_, "1"), std::optional<std::string>("10"));
  s1_->CommitTx();
  ASSERT_EQ(ReadString(*t2_, "1"), std::optional<std::string>("10"));
  s2_->CommitTx();
}

TEST_F(AnomaliesTest, NoG1c) {
  s1_->StartTx();
  s2_->StartTx();
  Rewrite(*t1_, "1", "11");
  Rewrite(*t2_, "2", "22");
  ASSERT_EQ(ReadString(*t1_, "2"), std::optional<std::string>("20"));
  ASSERT_EQ(ReadString(*t2_, "1"), std::optional<std::string>("10"));
  s1_->CommitTx();
  s2_->CommitTx();
}

TEST_F(AnomaliesTest, NoOTV) {
  s1_->StartTx();
  Rewrite(*t1_, "1", "11");
  Rewrite(*t1_, "2", "21");
  s2_->StartTx();
  s3_->StartTx();
  ASSERT_EQ(ReadString(*t3_, "1"), std::optional<std::string>("10"));
  ASSERT_EQ(ReadString(*t3_, "2"), std::optional<std::string>("20"));
  s1_->CommitTx();
  s2_->CommitTx();
  s3_->CommitTx();
}

TEST_F(AnomaliesTest, NoPMP) {
  s1_->StartTx();
  s2_->StartTx();
  ASSERT_FALSE(ReadString(*t1_, "3").has_value());
  ASSERT_TRUE(t2_->Insert("3", "30"));
  s2_->CommitTx();
  ASSERT_FALSE(ReadString(*t1_, "3").has_value());
  s1_->CommitTx();
}

TEST_F(AnomaliesTest, NoP4) {
  s1_->StartTx();
  s2_->StartTx();
  ASSERT_EQ(ReadString(*t1_, "1"), std::optional<std::string>("10"));
  ASSERT_EQ(ReadString(*t2_, "1"), std::optional<std::string>("10"));
  Rewrite(*t1_, "1", "11");
  auto second = t2_->Insert("1", "12");
  ASSERT_FALSE(second);
  s1_->CommitTx();
  s2_->AbortTx();
}

TEST_F(AnomaliesTest, NoGSingle) {
  s1_->StartTx();
  s2_->StartTx();
  ASSERT_EQ(ReadString(*t1_, "1"), std::optional<std::string>("10"));
  ASSERT_EQ(ReadString(*t2_, "2"), std::optional<std::string>("20"));
  Rewrite(*t2_, "1", "11");
  Rewrite(*t2_, "2", "21");
  s2_->CommitTx();
  ASSERT_EQ(ReadString(*t1_, "2"), std::optional<std::string>("20"));
  s1_->CommitTx();
}

TEST_F(AnomaliesTest, G2Item) {
  s1_->StartTx();
  s2_->StartTx();
  ASSERT_EQ(ReadString(*t1_, "1"), std::optional<std::string>("10"));
  ASSERT_EQ(ReadString(*t2_, "2"), std::optional<std::string>("20"));
  Rewrite(*t1_, "1", "11");
  Rewrite(*t2_, "2", "21");
  s1_->CommitTx();
  s2_->CommitTx();
}

} // namespace leanstore::test

#else

namespace leanstore::test {
TEST(AnomaliesTestCoroOnly, DisabledWhenNoCoro) {
  GTEST_SKIP() << "LEAN_ENABLE_CORO is disabled";
}
} // namespace leanstore::test

#endif
