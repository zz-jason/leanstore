#include "common/lean_test_suite.hpp"
#include "leanstore/c/types.h"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_session.hpp"
#include "leanstore/lean_store.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>

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

class AnomaliesTest : public LeanTestSuite {
protected:
  std::unique_ptr<LeanStore> store_;
  Optional<LeanSession> s1_;
  Optional<LeanSession> s2_;
  Optional<LeanSession> s3_;
  Optional<LeanBTree> t1_;
  Optional<LeanBTree> t2_;
  Optional<LeanBTree> t3_;

  void SetUp() override {
    auto* option = lean_store_option_create(TestCaseStoreDir().c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 3;
    auto opened = LeanStore::Open(option);
    ASSERT_TRUE(opened);
    store_ = std::move(opened.value());

    s1_ = store_->Connect();
    s2_ = store_->Connect();
    s3_ = store_->Connect();

    auto created = s1_->CreateBTree("anomalies", LEAN_BTREE_TYPE_MVCC);
    ASSERT_TRUE(created);
    t1_ = std::move(created.value());

    auto got2 = s2_->GetBTree("anomalies");
    ASSERT_TRUE(got2);
    t2_ = std::move(got2.value());
    auto got3 = s3_->GetBTree("anomalies");
    ASSERT_TRUE(got3);
    t3_ = std::move(got3.value());

    s1_->StartTx();
    ASSERT_TRUE(t1_->Insert("1", "10"));
    ASSERT_TRUE(t1_->Insert("2", "20"));
    s1_->CommitTx();
  }
};

TEST_F(AnomaliesTest, NoG0) {
  s1_->StartTx();
  s2_->StartTx();
  ASSERT_TRUE(t1_->Update("1", "11"));
  ASSERT_EQ(ReadString(*t2_, "1"), Optional<std::string>("10"));
  ASSERT_TRUE(t1_->Update("2", "21"));
  s1_->CommitTx();
  s2_->CommitTx();
}

TEST_F(AnomaliesTest, NoG1a) {
  s1_->StartTx();
  ASSERT_TRUE(t1_->Update("1", "11"));
  s2_->StartTx();
  ASSERT_EQ(ReadString(*t2_, "1"), Optional<std::string>("10"));
  s1_->AbortTx();
  ASSERT_EQ(ReadString(*t2_, "1"), Optional<std::string>("10"));
  s2_->CommitTx();
}

TEST_F(AnomaliesTest, NoG1b) {
  s1_->StartTx();
  ASSERT_TRUE(t1_->Update("1", "11"));
  s2_->StartTx();
  ASSERT_EQ(ReadString(*t2_, "1"), Optional<std::string>("10"));
  s1_->CommitTx();
  ASSERT_EQ(ReadString(*t2_, "1"), Optional<std::string>("10"));
  s2_->CommitTx();
}

TEST_F(AnomaliesTest, NoG1c) {
  s1_->StartTx();
  s2_->StartTx();
  ASSERT_TRUE(t1_->Update("1", "11"));
  ASSERT_TRUE(t2_->Update("2", "22"));
  ASSERT_EQ(ReadString(*t1_, "2"), Optional<std::string>("20"));
  ASSERT_EQ(ReadString(*t2_, "1"), Optional<std::string>("10"));
  s1_->CommitTx();
  s2_->CommitTx();
}

TEST_F(AnomaliesTest, NoOTV) {
  s1_->StartTx();
  ASSERT_TRUE(t1_->Update("1", "11"));
  ASSERT_TRUE(t1_->Update("2", "21"));
  s2_->StartTx();
  s3_->StartTx();
  ASSERT_EQ(ReadString(*t3_, "1"), Optional<std::string>("10"));
  ASSERT_EQ(ReadString(*t3_, "2"), Optional<std::string>("20"));
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
  ASSERT_EQ(ReadString(*t1_, "1"), Optional<std::string>("10"));
  ASSERT_EQ(ReadString(*t2_, "1"), Optional<std::string>("10"));
  ASSERT_TRUE(t1_->Update("1", "11"));
  auto second = t2_->Insert("1", "12");
  ASSERT_FALSE(second);
  s1_->CommitTx();
  s2_->AbortTx();
}

TEST_F(AnomaliesTest, NoGSingle) {
  s1_->StartTx();
  s2_->StartTx();
  ASSERT_EQ(ReadString(*t1_, "1"), Optional<std::string>("10"));
  ASSERT_EQ(ReadString(*t2_, "2"), Optional<std::string>("20"));
  ASSERT_TRUE(t2_->Update("1", "11"));
  ASSERT_TRUE(t2_->Update("2", "21"));
  s2_->CommitTx();
  ASSERT_EQ(ReadString(*t1_, "2"), Optional<std::string>("20"));
  s1_->CommitTx();
}

TEST_F(AnomaliesTest, G2Item) {
  s1_->StartTx();
  s2_->StartTx();
  ASSERT_EQ(ReadString(*t1_, "1"), Optional<std::string>("10"));
  ASSERT_EQ(ReadString(*t2_, "2"), Optional<std::string>("20"));
  ASSERT_TRUE(t1_->Update("1", "11"));
  ASSERT_TRUE(t2_->Update("2", "21"));
  s1_->CommitTx();
  s2_->CommitTx();
}

} // namespace leanstore::test
