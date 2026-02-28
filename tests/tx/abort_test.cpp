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

class AbortTest : public LeanTestSuite {
protected:
  std::unique_ptr<LeanStore> store_;
  Optional<LeanSession> s0_;
  Optional<LeanSession> s1_;
  Optional<LeanBTree> t0_;
  Optional<LeanBTree> t1_;

  void SetUp() override {
    auto* option = lean_store_option_create(TestCaseStoreDir().c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 2;
    auto opened = LeanStore::Open(option);
    ASSERT_TRUE(opened);
    store_ = std::move(opened.value());

    s0_ = store_->Connect(0);
    s1_ = store_->Connect(1);

    auto created = s0_->CreateBTree("abort_tx", LEAN_BTREE_TYPE_MVCC);
    ASSERT_TRUE(created);
    t0_ = std::move(created.value());

    auto fetched = s1_->GetBTree("abort_tx");
    ASSERT_TRUE(fetched);
    t1_ = std::move(fetched.value());
  }
};

TEST_F(AbortTest, AfterInsert) {
  s0_->StartTx();
  s1_->StartTx();
  ASSERT_TRUE(t0_->Insert("1", "10"));

  ASSERT_EQ(ReadString(*t0_, "1"), Optional<std::string>("10"));
  ASSERT_FALSE(ReadString(*t1_, "1").has_value());

  s0_->AbortTx();
  s1_->CommitTx();

  s1_->StartTx();
  ASSERT_FALSE(ReadString(*t1_, "1").has_value());
  s1_->CommitTx();
}

TEST_F(AbortTest, AfterUpdate) {
  s0_->StartTx();
  ASSERT_TRUE(t0_->Insert("1", "10"));
  s0_->CommitTx();

  s0_->StartTx();
  s1_->StartTx();
  ASSERT_TRUE(t0_->Remove("1"));
  ASSERT_TRUE(t0_->Insert("1", "11"));

  ASSERT_EQ(ReadString(*t0_, "1"), Optional<std::string>("11"));
  ASSERT_EQ(ReadString(*t1_, "1"), Optional<std::string>("10"));

  s0_->AbortTx();
  s1_->CommitTx();

  s1_->StartTx();
  ASSERT_EQ(ReadString(*t1_, "1"), Optional<std::string>("10"));
  s1_->CommitTx();
}

TEST_F(AbortTest, AfterRemove) {
  s0_->StartTx();
  ASSERT_TRUE(t0_->Insert("1", "10"));
  s0_->CommitTx();

  s0_->StartTx();
  s1_->StartTx();
  ASSERT_TRUE(t0_->Remove("1"));

  ASSERT_FALSE(ReadString(*t0_, "1").has_value());
  ASSERT_EQ(ReadString(*t1_, "1"), Optional<std::string>("10"));

  s0_->AbortTx();
  s1_->CommitTx();

  s1_->StartTx();
  ASSERT_EQ(ReadString(*t1_, "1"), Optional<std::string>("10"));
  s1_->CommitTx();
}

TEST_F(AbortTest, AfterInsertOnRemove) {
  s0_->StartTx();
  ASSERT_TRUE(t0_->Insert("1", "10"));
  s0_->CommitTx();

  s0_->StartTx();
  s1_->StartTx();
  ASSERT_TRUE(t0_->Remove("1"));
  ASSERT_TRUE(t0_->Insert("1", "11"));

  ASSERT_EQ(ReadString(*t0_, "1"), Optional<std::string>("11"));
  ASSERT_EQ(ReadString(*t1_, "1"), Optional<std::string>("10"));

  s0_->AbortTx();
  s1_->CommitTx();

  s1_->StartTx();
  ASSERT_EQ(ReadString(*t1_, "1"), Optional<std::string>("10"));
  s1_->CommitTx();
}

} // namespace leanstore::test
