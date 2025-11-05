#include "leanstore/utils/random_generator.hpp"
#include "tx_kv.hpp"

#include "gtest/gtest.h"
#include <gtest/gtest.h>

#include <memory>

namespace leanstore::test {

class AbortTest : public ::testing::Test {
protected:
  std::unique_ptr<Store> store_;
  std::string tbl_name_;
  TableRef* tbl_;

protected:
  AbortTest() = default;

  ~AbortTest() = default;

  void SetUp() override {
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    auto cur_test_name =
        std::string(cur_test->test_case_name()) + "_" + std::string(cur_test->name());
    std::string store_dir = "/tmp/leanstore/" + cur_test_name;
    uint32_t session_limit = 2;
    store_ = StoreFactory::NewLeanStoreMVCC(store_dir, session_limit);
    ASSERT_NE(store_, nullptr);

    // Set transaction isolation to SI before transaction tests, get ride of
    // tests running before which changed the isolation level.
    for (uint32_t i = 0; i < session_limit; ++i) {
      store_->GetSession(i)->SetIsolationLevel(IsolationLevel::kSnapshotIsolation);
    }

    // Create a table with random name.
    auto* s0 = store_->GetSession(0);
    tbl_name_ = utils::RandomGenerator::RandAlphString(10);
    auto res = s0->CreateTable(tbl_name_, true);
    ASSERT_TRUE(res);
    tbl_ = res.value();
    ASSERT_NE(tbl_, nullptr);
  }

  void TearDown() override {
    // Cleanup, remove the created table.
    auto* s0 = store_->GetSession(0);
    s0->SetIsolationLevel(IsolationLevel::kSnapshotIsolation);
    auto res = s0->DropTable(tbl_name_, true);
    ASSERT_TRUE(res);
  }
};

TEST_F(AbortTest, AfterInsert) {
  auto* s0 = store_->GetSession(0);
  auto* s1 = store_->GetSession(1);

  std::string key1("1"), val1("10");
  std::string val_read;

  s0->StartTx();
  s1->StartTx();
  ASSERT_TRUE(s0->Put(tbl_, key1, val1));

  auto res = s0->Get(tbl_, key1, val_read);
  ASSERT_TRUE(res && res.value() == 1); // got the uncommitted value
  ASSERT_EQ(val_read, val1);

  res = s1->Get(tbl_, key1, val_read);
  ASSERT_TRUE(res && res.value() == 0); // got nothing

  s0->AbortTx();
  s1->CommitTx();

  res = s1->Get(tbl_, key1, val_read, true);
  ASSERT_TRUE(res && res.value() == 0); // got nothing still

  res = s0->Get(tbl_, key1, val_read, true);
  ASSERT_TRUE(res && res.value() == 0); // got nothing still
}

TEST_F(AbortTest, AfterUpdate) {
  auto* s0 = store_->GetSession(0);
  auto* s1 = store_->GetSession(1);

  std::string key1("1"), val1("10"), val11("11");
  std::string val_read;
  ASSERT_TRUE(s0->Put(tbl_, key1, ToSlice(val1), true));

  s0->StartTx();
  s1->StartTx();
  ASSERT_TRUE(s0->Update(tbl_, key1, ToSlice(val11)));

  auto res = s0->Get(tbl_, key1, val_read);
  ASSERT_TRUE(res && res.value() == 1); // got the uncommitted value
  ASSERT_EQ(val_read, val11);

  res = s1->Get(tbl_, key1, val_read);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(val_read, val1);

  s0->AbortTx();
  s1->CommitTx();

  res = s0->Get(tbl_, key1, val_read, true);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(val_read, val1);

  res = s1->Get(tbl_, key1, val_read, true);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(val_read, val1);
}

TEST_F(AbortTest, AfterRemove) {
  auto* s0 = store_->GetSession(0);
  auto* s1 = store_->GetSession(1);

  std::string key1("1"), val1("10"), val11("11");
  std::string val_read;
  ASSERT_TRUE(s0->Put(tbl_, key1, ToSlice(val1), true));

  s0->StartTx();
  s1->StartTx();
  ASSERT_TRUE(s0->Delete(tbl_, key1));

  auto res = s0->Get(tbl_, key1, val_read);
  ASSERT_TRUE(res && res.value() == 0); // got nothing

  res = s1->Get(tbl_, key1, val_read);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(val_read, val1);

  s0->AbortTx();
  s1->CommitTx();

  res = s0->Get(tbl_, key1, val_read, true);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(val_read, val1);

  res = s1->Get(tbl_, key1, val_read, true);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(val_read, val1);
}

TEST_F(AbortTest, AfterInsertOnRemove) {
  auto* s0 = store_->GetSession(0);
  auto* s1 = store_->GetSession(1);

  std::string key1("1"), val1("10"), val11("11");
  std::string val_read;
  ASSERT_TRUE(s0->Put(tbl_, key1, ToSlice(val1), true));

  s0->StartTx();
  s1->StartTx();
  ASSERT_TRUE(s0->Delete(tbl_, key1));

  auto res = s0->Get(tbl_, key1, val_read);
  ASSERT_TRUE(res && res.value() == 0); // got nothing

  res = s1->Get(tbl_, key1, val_read);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(val_read, val1);

  // insert on removed key
  ASSERT_TRUE(s0->Put(tbl_, key1, ToSlice(val11)));
  res = s0->Get(tbl_, key1, val_read);
  ASSERT_TRUE(res && res.value() == 1); // get the uncommitted value
  ASSERT_EQ(val_read, val11);

  res = s1->Get(tbl_, key1, val_read);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(val_read, val1);

  s0->AbortTx();
  s1->CommitTx();

  res = s0->Get(tbl_, key1, val_read, true);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(val_read, val1);

  res = s1->Get(tbl_, key1, val_read, true);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(val_read, val1);
}

} // namespace leanstore::test
