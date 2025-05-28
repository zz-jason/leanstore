#include "leanstore/utils/random_generator.hpp"
#include "tx_kv.hpp"

#include <gtest/gtest.h>

#include <memory>

using namespace leanstore::utils;
using namespace leanstore::storage::btree;

namespace leanstore::test {

class AnomaliesTest : public ::testing::Test {
protected:
  std::unique_ptr<Store> store_;
  std::string tbl_name_;
  TableRef* tbl_;

protected:
  AnomaliesTest() = default;

  ~AnomaliesTest() = default;

  void SetUp() override {
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    auto cur_test_name =
        std::string(cur_test->test_case_name()) + "_" + std::string(cur_test->name());
    std::string store_dir = "/tmp/leanstore/" + cur_test_name;
    uint32_t session_limit = 4;
    store_ = StoreFactory::NewLeanStoreMVCC(store_dir, session_limit);
    ASSERT_NE(store_, nullptr);

    // Set transaction isolation to SI before transaction tests, get ride of
    // tests running before which changed the isolation level.
    for (uint32_t i = 0; i < session_limit; ++i) {
      store_->GetSession(i)->SetIsolationLevel(IsolationLevel::kSnapshotIsolation);
    }

    // Create a table with random name.
    auto* s0 = store_->GetSession(0);
    tbl_name_ = RandomGenerator::RandAlphString(10);
    auto res = s0->CreateTable(tbl_name_, true);
    ASSERT_TRUE(res);
    tbl_ = res.value();
    ASSERT_NE(tbl_, nullptr);

    // Insert 2 key-values as the test base.
    std::string key1("1"), val1("10");
    std::string key2("2"), val2("20");
    ASSERT_TRUE(s0->Put(tbl_, key1, ToSlice(val1), true));
    ASSERT_TRUE(s0->Put(tbl_, key2, ToSlice(val2), true));
  }

  void TearDown() override {
    // Cleanup, remove the created table.
    auto* s0 = store_->GetSession(0);
    s0->SetIsolationLevel(IsolationLevel::kSnapshotIsolation);
    auto res = s0->DropTable(tbl_name_, true);
    ASSERT_TRUE(res);
  }
};

// G0: Write Cycles (dirty writes)
TEST_F(AnomaliesTest, NoG0) {
  auto* s1 = store_->GetSession(1);
  auto* s2 = store_->GetSession(2);

  std::string key1("1");
  std::string new_val11("11"), new_val12("12");

  std::string key2("2");
  std::string new_val21("21"), new_val22("22");
  std::string res;

  // Snapshot isolation can prevent G0
  s1->SetIsolationLevel(IsolationLevel::kSnapshotIsolation);
  s2->SetIsolationLevel(IsolationLevel::kSnapshotIsolation);
  s1->StartTx();
  s2->StartTx();
  EXPECT_TRUE(s1->Update(tbl_, key1, ToSlice(new_val11)));
  EXPECT_FALSE(s2->Update(tbl_, key1, ToSlice(new_val12)));
  EXPECT_TRUE(s1->Update(tbl_, key2, ToSlice(new_val21)));
  s1->CommitTx();
  EXPECT_TRUE(s1->Get(tbl_, key1, res, true));
  EXPECT_EQ(res, new_val11);
  EXPECT_TRUE(s1->Get(tbl_, key2, res, true));
  EXPECT_EQ(res, new_val21);

  EXPECT_FALSE(s2->Update(tbl_, key2, ToSlice(new_val22)));
  s2->CommitTx();
  EXPECT_TRUE(s2->Get(tbl_, key1, res, true));
  EXPECT_EQ(res, new_val11);
  EXPECT_TRUE(s2->Get(tbl_, key2, res, true));
  EXPECT_EQ(res, new_val21);

  // Serializable can prevent G0
  s1->SetIsolationLevel(IsolationLevel::kSerializable);
  s2->SetIsolationLevel(IsolationLevel::kSerializable);
  s1->StartTx();
  s2->StartTx();
  EXPECT_TRUE(s1->Update(tbl_, key1, ToSlice(new_val11)));
  EXPECT_FALSE(s2->Update(tbl_, key1, ToSlice(new_val12)));
  EXPECT_TRUE(s1->Update(tbl_, key2, ToSlice(new_val21)));
  s1->CommitTx();
  EXPECT_TRUE(s1->Get(tbl_, key1, res, true));
  EXPECT_EQ(res, new_val11);
  EXPECT_TRUE(s1->Get(tbl_, key2, res, true));
  EXPECT_EQ(res, new_val21);

  EXPECT_FALSE(s2->Update(tbl_, key2, ToSlice(new_val22)));
  s2->CommitTx();
  EXPECT_TRUE(s2->Get(tbl_, key1, res, true));
  EXPECT_EQ(res, new_val11);
  EXPECT_TRUE(s2->Get(tbl_, key2, res, true));
  EXPECT_EQ(res, new_val21);
}

// G1a: Aborted Reads (dirty reads, cascaded aborts)
TEST_F(AnomaliesTest, NoG1a) {
  auto* s1 = store_->GetSession(1);
  auto* s2 = store_->GetSession(2);

  std::string key1("1");
  std::string new_val11("11"), new_val12("12");

  std::string key2("2");
  std::string new_val21("21"), new_val22("22");
  std::string res;

  s1->StartTx();
  s2->StartTx();
  EXPECT_TRUE(s1->Update(tbl_, key1, ToSlice(new_val11)));
  EXPECT_TRUE(s2->Get(tbl_, key1, res));
  EXPECT_EQ(res, "10");
  s1->AbortTx();
  EXPECT_TRUE(s2->Get(tbl_, key1, res));
  EXPECT_EQ(res, "10");
  s2->CommitTx();
}

// G1b: Intermediate Reads (dirty reads)
TEST_F(AnomaliesTest, NoG1b) {
  auto* s1 = store_->GetSession(1);
  auto* s2 = store_->GetSession(2);

  std::string key1("1");
  std::string new_val11("11"), new_val12("12");

  std::string key2("2");
  std::string new_val21("21"), new_val22("22");
  std::string res;

  s1->StartTx();
  s2->StartTx();
  EXPECT_TRUE(s1->Update(tbl_, key1, ToSlice(new_val11)));
  EXPECT_TRUE(s2->Get(tbl_, key1, res));
  EXPECT_EQ(res, "10");
  s1->CommitTx();
  EXPECT_TRUE(s2->Get(tbl_, key1, res));
  EXPECT_EQ(res, "10");
  s2->CommitTx();
}

// G1c: Circular Information Flow (dirty reads)
TEST_F(AnomaliesTest, NoG1c) {
  auto* s1 = store_->GetSession(1);
  auto* s2 = store_->GetSession(2);

  std::string key1("1");
  std::string new_val11("11"), new_val12("12");

  std::string key2("2");
  std::string new_val21("21"), new_val22("22");
  std::string val_read;

  s1->StartTx();
  s2->StartTx();
  EXPECT_TRUE(s1->Update(tbl_, key1, ToSlice(new_val11)));
  EXPECT_TRUE(s2->Update(tbl_, key2, ToSlice(new_val22)));

  auto res = s1->Get(tbl_, key2, val_read);
  EXPECT_TRUE(res);
  EXPECT_TRUE(res.value() == 1);
  EXPECT_EQ(val_read, "20");

  res = s2->Get(tbl_, key1, val_read);
  EXPECT_TRUE(res);
  EXPECT_TRUE(res.value() == 1);
  EXPECT_EQ(val_read, "10");

  s1->CommitTx();
  s2->CommitTx();
}

// OTV: Observed Transaction Vanishes
TEST_F(AnomaliesTest, NoOTV) {
  auto* s1 = store_->GetSession(1);
  auto* s2 = store_->GetSession(2);
  auto* s3 = store_->GetSession(3);

  std::string key1("1");
  std::string new_val11("11"), new_val12("12");

  std::string key2("2");
  std::string new_val21("21"), new_val22("22");
  std::string res;

  s1->StartTx();
  s2->StartTx();
  s3->StartTx();
  EXPECT_TRUE(s1->Update(tbl_, key1, ToSlice(new_val11)));
  EXPECT_TRUE(s1->Update(tbl_, key2, ToSlice(new_val21)));
  // update conflict
  EXPECT_FALSE(s2->Update(tbl_, key1, ToSlice(new_val12)));
  s1->CommitTx();
  EXPECT_TRUE(s3->Get(tbl_, key1, res));
  EXPECT_EQ(res, "10");
  // update conflict
  EXPECT_FALSE(s2->Update(tbl_, key2, ToSlice(new_val22)));
  EXPECT_TRUE(s3->Get(tbl_, key2, res));
  EXPECT_EQ(res, "20");
  s2->CommitTx();
  EXPECT_TRUE(s3->Get(tbl_, key1, res));
  EXPECT_EQ(res, "10");
  EXPECT_TRUE(s3->Get(tbl_, key2, res));
  EXPECT_EQ(res, "20");
  s3->CommitTx();
}

// PMP: Predicate-Many-Preceders
TEST_F(AnomaliesTest, NoPMP) {
  auto* s1 = store_->GetSession(1);
  auto* s2 = store_->GetSession(2);

  // Prepare: insert into test (id, value) values (1, 10), (2, 20);
  std::string key3("3"), val3("30");
  std::string val_read;

  s1->StartTx();
  s2->StartTx();

  auto res = s1->Get(tbl_, ToSlice(key3), val_read);
  EXPECT_TRUE(res && res.value() == 0u);

  EXPECT_TRUE(s2->Put(tbl_, ToSlice(key3), ToSlice(val3)));
  s2->CommitTx();

  res = s1->Get(tbl_, ToSlice(key3), val_read);
  EXPECT_TRUE(res && res.value() == 0u);

  s1->CommitTx();
}

// P4: Lost Update
TEST_F(AnomaliesTest, NoP4) {
  auto* s1 = store_->GetSession(1);
  auto* s2 = store_->GetSession(2);

  // Prepare: insert into test (id, value) values (1, 10), (2, 20);
  std::string key1("1"), new_val11("11"), new_val12("12");
  std::string res;

  s1->StartTx();
  s2->StartTx();
  EXPECT_TRUE(s1->Get(tbl_, key1, res));
  EXPECT_EQ(res, "10");
  EXPECT_TRUE(s2->Get(tbl_, key1, res));
  EXPECT_EQ(res, "10");
  EXPECT_TRUE(s1->Update(tbl_, key1, ToSlice(new_val11)));
  EXPECT_FALSE(s2->Update(tbl_, key1, ToSlice(new_val12)));
  s1->CommitTx();
  s2->AbortTx();
}

// G-single: Single Anti-dependency Cycles (read skew)
TEST_F(AnomaliesTest, NoGSingle) {
  auto* s1 = store_->GetSession(1);
  auto* s2 = store_->GetSession(2);

  // Prepare: insert into test (id, value) values (1, 10), (2, 20);
  std::string key1("1"), new_val11("11");
  std::string key2("2"), new_val21("21");
  std::string res;

  s1->StartTx();
  s2->StartTx();
  EXPECT_TRUE(s1->Get(tbl_, key1, res));
  EXPECT_EQ(res, "10");
  EXPECT_TRUE(s2->Get(tbl_, key1, res));
  EXPECT_EQ(res, "10");
  EXPECT_TRUE(s2->Get(tbl_, key2, res));
  EXPECT_EQ(res, "20");
  EXPECT_TRUE(s2->Update(tbl_, key1, ToSlice(new_val11)));
  EXPECT_TRUE(s2->Update(tbl_, key2, ToSlice(new_val21)));
  s2->CommitTx();
  EXPECT_TRUE(s1->Get(tbl_, key2, res));
  EXPECT_EQ(res, "20");
  s1->CommitTx();
}

// G2-item: Item Anti-dependency Cycles (write skew on disjoint read)
TEST_F(AnomaliesTest, G2Item) {
  auto* s1 = store_->GetSession(1);
  auto* s2 = store_->GetSession(2);

  std::string key1("1"), new_val11("11");
  std::string key2("2"), new_val21("21");
  std::string res;

  s1->StartTx();
  s2->StartTx();

  EXPECT_TRUE(s1->Get(tbl_, key1, res));
  EXPECT_EQ(res, "10");
  EXPECT_TRUE(s1->Get(tbl_, key2, res));
  EXPECT_EQ(res, "20");

  EXPECT_TRUE(s2->Get(tbl_, key1, res));
  EXPECT_EQ(res, "10");
  EXPECT_TRUE(s2->Get(tbl_, key2, res));
  EXPECT_EQ(res, "20");

  EXPECT_TRUE(s1->Update(tbl_, key1, ToSlice(new_val11)));
  EXPECT_TRUE(s2->Update(tbl_, key2, ToSlice(new_val21)));

  s1->CommitTx();
  s2->CommitTx();
}

} // namespace leanstore::test
