#include "TxKV.hpp"
#include "concurrency-recovery/Transaction.hpp"
#include "utils/RandomGenerator.hpp"

#include <gtest/gtest.h>

using namespace leanstore::utils;
using namespace leanstore::storage::btree;

namespace leanstore {
namespace test {

class AnomaliesTest : public ::testing::Test {
protected:
  Store* mStore;
  std::string mTblName;
  TableRef* mTbl;

protected:
  AnomaliesTest() = default;

  ~AnomaliesTest() = default;

  void SetUp() override {
    std::string storeDir = "/tmp/AnomaliesTest";
    u32 sessionLimit = 4;
    mStore = StoreFactory::GetLeanStoreMVCC(storeDir, sessionLimit);
    ASSERT_NE(mStore, nullptr);

    // Set transaction isolation to SI before transaction tests, get ride of
    // tests running before which changed the isolation level.
    for (u32 i = 0; i < sessionLimit; ++i) {
      mStore->GetSession(i)->SetIsolationLevel(
          IsolationLevel::kSnapshotIsolation);
    }

    // Create a table with random name.
    auto* s0 = mStore->GetSession(0);
    mTblName = RandomGenerator::RandomAlphString(10);
    auto res = s0->CreateTable(mTblName, true);
    ASSERT_TRUE(res);
    mTbl = res.value();
    ASSERT_NE(mTbl, nullptr);

    // Insert 2 key-values as the test base.
    std::string key1("1"), val1("10");
    std::string key2("2"), val2("20");
    ASSERT_TRUE(s0->Put(mTbl, ToSlice(key1), ToSlice(val1), true));
    ASSERT_TRUE(s0->Put(mTbl, ToSlice(key2), ToSlice(val2), true));
  }

  void TearDown() override {
    // Cleanup, remove the created table.
    auto* s0 = mStore->GetSession(0);
    s0->SetIsolationLevel(IsolationLevel::kSnapshotIsolation);
    auto res = s0->DropTable(mTblName, true);
    ASSERT_TRUE(res);
  }
};

// G0: Write Cycles (dirty writes)
TEST_F(AnomaliesTest, NoG0) {
  auto* s1 = mStore->GetSession(1);
  auto* s2 = mStore->GetSession(2);

  std::string key1("1");
  std::string newVal11("11"), newVal12("12");

  std::string key2("2");
  std::string newVal21("21"), newVal22("22");
  std::string res;

  // Snapshot isolation can prevent G0
  s1->SetIsolationLevel(IsolationLevel::kSnapshotIsolation);
  s2->SetIsolationLevel(IsolationLevel::kSnapshotIsolation);
  s1->StartTx();
  s2->StartTx();
  EXPECT_TRUE(s1->Update(mTbl, ToSlice(key1), ToSlice(newVal11)));
  EXPECT_FALSE(s2->Update(mTbl, ToSlice(key1), ToSlice(newVal12)));
  EXPECT_TRUE(s1->Update(mTbl, ToSlice(key2), ToSlice(newVal21)));
  s1->CommitTx();
  EXPECT_TRUE(s1->Get(mTbl, ToSlice(key1), res, true));
  EXPECT_EQ(res, newVal11);
  EXPECT_TRUE(s1->Get(mTbl, ToSlice(key2), res, true));
  EXPECT_EQ(res, newVal21);

  EXPECT_FALSE(s2->Update(mTbl, ToSlice(key2), ToSlice(newVal22)));
  s2->CommitTx();
  EXPECT_TRUE(s2->Get(mTbl, ToSlice(key1), res, true));
  EXPECT_EQ(res, newVal11);
  EXPECT_TRUE(s2->Get(mTbl, ToSlice(key2), res, true));
  EXPECT_EQ(res, newVal21);

  // Serializable can prevent G0
  s1->SetIsolationLevel(IsolationLevel::kSerializable);
  s2->SetIsolationLevel(IsolationLevel::kSerializable);
  s1->StartTx();
  s2->StartTx();
  EXPECT_TRUE(s1->Update(mTbl, ToSlice(key1), ToSlice(newVal11)));
  EXPECT_FALSE(s2->Update(mTbl, ToSlice(key1), ToSlice(newVal12)));
  EXPECT_TRUE(s1->Update(mTbl, ToSlice(key2), ToSlice(newVal21)));
  s1->CommitTx();
  EXPECT_TRUE(s1->Get(mTbl, ToSlice(key1), res, true));
  EXPECT_EQ(res, newVal11);
  EXPECT_TRUE(s1->Get(mTbl, ToSlice(key2), res, true));
  EXPECT_EQ(res, newVal21);

  EXPECT_FALSE(s2->Update(mTbl, ToSlice(key2), ToSlice(newVal22)));
  s2->CommitTx();
  EXPECT_TRUE(s2->Get(mTbl, ToSlice(key1), res, true));
  EXPECT_EQ(res, newVal11);
  EXPECT_TRUE(s2->Get(mTbl, ToSlice(key2), res, true));
  EXPECT_EQ(res, newVal21);
}

// G1a: Aborted Reads (dirty reads, cascaded aborts)
TEST_F(AnomaliesTest, NoG1a) {
  auto* s1 = mStore->GetSession(1);
  auto* s2 = mStore->GetSession(2);

  std::string key1("1");
  std::string newVal11("11"), newVal12("12");

  std::string key2("2");
  std::string newVal21("21"), newVal22("22");
  std::string res;

  s1->StartTx();
  s2->StartTx();
  EXPECT_TRUE(s1->Update(mTbl, ToSlice(key1), ToSlice(newVal11)));
  EXPECT_TRUE(s2->Get(mTbl, ToSlice(key1), res));
  EXPECT_EQ(res, "10");
  s1->AbortTx();
  EXPECT_TRUE(s2->Get(mTbl, ToSlice(key1), res));
  EXPECT_EQ(res, "10");
  s2->CommitTx();
}

// G1b: Intermediate Reads (dirty reads)
TEST_F(AnomaliesTest, NoG1b) {
  auto* s1 = mStore->GetSession(1);
  auto* s2 = mStore->GetSession(2);

  std::string key1("1");
  std::string newVal11("11"), newVal12("12");

  std::string key2("2");
  std::string newVal21("21"), newVal22("22");
  std::string res;

  s1->StartTx();
  s2->StartTx();
  EXPECT_TRUE(s1->Update(mTbl, ToSlice(key1), ToSlice(newVal11)));
  EXPECT_TRUE(s2->Get(mTbl, ToSlice(key1), res));
  EXPECT_EQ(res, "10");
  s1->CommitTx();
  EXPECT_TRUE(s2->Get(mTbl, ToSlice(key1), res));
  EXPECT_EQ(res, "10");
  s2->CommitTx();
}

// G1c: Circular Information Flow (dirty reads)
TEST_F(AnomaliesTest, NoG1c) {
  auto* s1 = mStore->GetSession(1);
  auto* s2 = mStore->GetSession(2);

  std::string key1("1");
  std::string newVal11("11"), newVal12("12");

  std::string key2("2");
  std::string newVal21("21"), newVal22("22");
  std::string valRead;

  s1->StartTx();
  s2->StartTx();
  EXPECT_TRUE(s1->Update(mTbl, ToSlice(key1), ToSlice(newVal11)));
  EXPECT_TRUE(s2->Update(mTbl, ToSlice(key2), ToSlice(newVal22)));

  auto res = s1->Get(mTbl, ToSlice(key2), valRead);
  EXPECT_TRUE(res);
  EXPECT_TRUE(res.value() == 1);
  EXPECT_EQ(valRead, "20");

  res = s2->Get(mTbl, ToSlice(key1), valRead);
  EXPECT_TRUE(res);
  EXPECT_TRUE(res.value() == 1);
  EXPECT_EQ(valRead, "10");

  s1->CommitTx();
  s2->CommitTx();
}

// OTV: Observed Transaction Vanishes
TEST_F(AnomaliesTest, NoOTV) {
  auto* s1 = mStore->GetSession(1);
  auto* s2 = mStore->GetSession(2);
  auto* s3 = mStore->GetSession(3);

  std::string key1("1");
  std::string newVal11("11"), newVal12("12");

  std::string key2("2");
  std::string newVal21("21"), newVal22("22");
  std::string res;

  s1->StartTx();
  s2->StartTx();
  s3->StartTx();
  EXPECT_TRUE(s1->Update(mTbl, ToSlice(key1), ToSlice(newVal11)));
  EXPECT_TRUE(s1->Update(mTbl, ToSlice(key2), ToSlice(newVal21)));
  // update conflict
  EXPECT_FALSE(s2->Update(mTbl, ToSlice(key1), ToSlice(newVal12)));
  s1->CommitTx();
  EXPECT_TRUE(s3->Get(mTbl, ToSlice(key1), res));
  EXPECT_EQ(res, "10");
  // update conflict
  EXPECT_FALSE(s2->Update(mTbl, ToSlice(key2), ToSlice(newVal22)));
  EXPECT_TRUE(s3->Get(mTbl, ToSlice(key2), res));
  EXPECT_EQ(res, "20");
  s2->CommitTx();
  EXPECT_TRUE(s3->Get(mTbl, ToSlice(key1), res));
  EXPECT_EQ(res, "10");
  EXPECT_TRUE(s3->Get(mTbl, ToSlice(key2), res));
  EXPECT_EQ(res, "20");
  s3->CommitTx();
}

// PMP: Predicate-Many-Preceders
TEST_F(AnomaliesTest, NoPMP) {
  auto* s1 = mStore->GetSession(1);
  auto* s2 = mStore->GetSession(2);

  // Prepare: insert into test (id, value) values (1, 10), (2, 20);
  std::string key3("3"), val3("30");
  std::string valRead;

  s1->StartTx();
  s2->StartTx();

  auto res = s1->Get(mTbl, ToSlice(key3), valRead);
  EXPECT_TRUE(res && res.value() == 0u);

  EXPECT_TRUE(s2->Put(mTbl, ToSlice(key3), ToSlice(val3)));
  s2->CommitTx();

  res = s1->Get(mTbl, ToSlice(key3), valRead);
  EXPECT_TRUE(res && res.value() == 0u);

  s1->CommitTx();
}

// P4: Lost Update
TEST_F(AnomaliesTest, NoP4) {
  auto* s1 = mStore->GetSession(1);
  auto* s2 = mStore->GetSession(2);

  // Prepare: insert into test (id, value) values (1, 10), (2, 20);
  std::string key1("1"), newVal11("11"), newVal12("12");
  std::string res;

  s1->StartTx();
  s2->StartTx();
  EXPECT_TRUE(s1->Get(mTbl, ToSlice(key1), res));
  EXPECT_EQ(res, "10");
  EXPECT_TRUE(s2->Get(mTbl, ToSlice(key1), res));
  EXPECT_EQ(res, "10");
  EXPECT_TRUE(s1->Update(mTbl, ToSlice(key1), ToSlice(newVal11)));
  EXPECT_FALSE(s2->Update(mTbl, ToSlice(key1), ToSlice(newVal12)));
  s1->CommitTx();
  s2->AbortTx();
}

// G-single: Single Anti-dependency Cycles (read skew)
TEST_F(AnomaliesTest, NoGSingle) {
  auto* s1 = mStore->GetSession(1);
  auto* s2 = mStore->GetSession(2);

  // Prepare: insert into test (id, value) values (1, 10), (2, 20);
  std::string key1("1"), newVal11("11");
  std::string key2("2"), newVal21("21");
  std::string res;

  s1->StartTx();
  s2->StartTx();
  EXPECT_TRUE(s1->Get(mTbl, ToSlice(key1), res));
  EXPECT_EQ(res, "10");
  EXPECT_TRUE(s2->Get(mTbl, ToSlice(key1), res));
  EXPECT_EQ(res, "10");
  EXPECT_TRUE(s2->Get(mTbl, ToSlice(key2), res));
  EXPECT_EQ(res, "20");
  EXPECT_TRUE(s2->Update(mTbl, ToSlice(key1), ToSlice(newVal11)));
  EXPECT_TRUE(s2->Update(mTbl, ToSlice(key2), ToSlice(newVal21)));
  s2->CommitTx();
  EXPECT_TRUE(s1->Get(mTbl, ToSlice(key2), res));
  EXPECT_EQ(res, "20");
  s1->CommitTx();
}

// G2-item: Item Anti-dependency Cycles (write skew on disjoint read)
TEST_F(AnomaliesTest, G2Item) {
  auto* s1 = mStore->GetSession(1);
  auto* s2 = mStore->GetSession(2);

  std::string key1("1"), newVal11("11");
  std::string key2("2"), newVal21("21");
  std::string res;

  s1->StartTx();
  s2->StartTx();

  EXPECT_TRUE(s1->Get(mTbl, ToSlice(key1), res));
  EXPECT_EQ(res, "10");
  EXPECT_TRUE(s1->Get(mTbl, ToSlice(key2), res));
  EXPECT_EQ(res, "20");

  EXPECT_TRUE(s2->Get(mTbl, ToSlice(key1), res));
  EXPECT_EQ(res, "10");
  EXPECT_TRUE(s2->Get(mTbl, ToSlice(key2), res));
  EXPECT_EQ(res, "20");

  EXPECT_TRUE(s1->Update(mTbl, ToSlice(key1), ToSlice(newVal11)));
  EXPECT_TRUE(s2->Update(mTbl, ToSlice(key2), ToSlice(newVal21)));

  s1->CommitTx();
  s2->CommitTx();
}

} // namespace test
} // namespace leanstore

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}