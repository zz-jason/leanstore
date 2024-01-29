#include "TxKV.hpp"
#include "concurrency-recovery/Transaction.hpp"
#include "utils/RandomGenerator.hpp"

#include "gtest/gtest.h"
#include <gtest/gtest.h>

#include <memory>

using namespace leanstore::utils;
using namespace leanstore::storage::btree;

namespace leanstore::test {

class AbortTest : public ::testing::Test {
protected:
  std::unique_ptr<Store> mStore;
  std::string mTblName;
  TableRef* mTbl;

protected:
  AbortTest() = default;

  ~AbortTest() = default;

  void SetUp() override {
    auto* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
    auto curTestName = std::string(curTest->test_case_name()) + "_" +
                       std::string(curTest->name());
    std::string storeDir = "/tmp/" + curTestName;
    u32 sessionLimit = 2;
    mStore = StoreFactory::NewLeanStoreMVCC(storeDir, sessionLimit);
    ASSERT_NE(mStore, nullptr);

    // Set transaction isolation to SI before transaction tests, get ride of
    // tests running before which changed the isolation level.
    for (u32 i = 0; i < sessionLimit; ++i) {
      mStore->GetSession(i)->SetIsolationLevel(
          IsolationLevel::kSnapshotIsolation);
    }

    // Create a table with random name.
    auto* s0 = mStore->GetSession(0);
    mTblName = RandomGenerator::RandAlphString(10);
    auto res = s0->CreateTable(mTblName, true);
    ASSERT_TRUE(res);
    mTbl = res.value();
    ASSERT_NE(mTbl, nullptr);
  }

  void TearDown() override {
    // Cleanup, remove the created table.
    auto* s0 = mStore->GetSession(0);
    s0->SetIsolationLevel(IsolationLevel::kSnapshotIsolation);
    auto res = s0->DropTable(mTblName, true);
    ASSERT_TRUE(res);
  }
};

TEST_F(AbortTest, AfterInsert) {
  auto* s0 = mStore->GetSession(0);
  auto* s1 = mStore->GetSession(1);

  std::string key1("1"), val1("10");
  std::string valRead;

  s0->StartTx();
  s1->StartTx();
  ASSERT_TRUE(s0->Put(mTbl, ToSlice(key1), ToSlice(val1)));

  auto res = s0->Get(mTbl, ToSlice(key1), valRead);
  ASSERT_TRUE(res && res.value() == 1); // got the uncommitted value
  ASSERT_EQ(valRead, val1);

  res = s1->Get(mTbl, ToSlice(key1), valRead);
  ASSERT_TRUE(res && res.value() == 0); // got nothing

  s0->AbortTx();
  s1->CommitTx();

  res = s1->Get(mTbl, ToSlice(key1), valRead, true);
  ASSERT_TRUE(res && res.value() == 0); // got nothing still

  res = s0->Get(mTbl, ToSlice(key1), valRead, true);
  ASSERT_TRUE(res && res.value() == 0); // got nothing still
}

TEST_F(AbortTest, AfterUpdate) {
  auto* s0 = mStore->GetSession(0);
  auto* s1 = mStore->GetSession(1);

  std::string key1("1"), val1("10"), val11("11");
  std::string valRead;
  ASSERT_TRUE(s0->Put(mTbl, ToSlice(key1), ToSlice(val1), true));

  s0->StartTx();
  s1->StartTx();
  ASSERT_TRUE(s0->Update(mTbl, ToSlice(key1), ToSlice(val11)));

  auto res = s0->Get(mTbl, ToSlice(key1), valRead);
  ASSERT_TRUE(res && res.value() == 1); // got the uncommitted value
  ASSERT_EQ(valRead, val11);

  res = s1->Get(mTbl, ToSlice(key1), valRead);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(valRead, val1);

  s0->AbortTx();
  s1->CommitTx();

  res = s0->Get(mTbl, ToSlice(key1), valRead, true);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(valRead, val1);

  res = s1->Get(mTbl, ToSlice(key1), valRead, true);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(valRead, val1);
}

TEST_F(AbortTest, AfterRemove) {
  auto* s0 = mStore->GetSession(0);
  auto* s1 = mStore->GetSession(1);

  std::string key1("1"), val1("10"), val11("11");
  std::string valRead;
  ASSERT_TRUE(s0->Put(mTbl, ToSlice(key1), ToSlice(val1), true));

  s0->StartTx();
  s1->StartTx();
  ASSERT_TRUE(s0->Delete(mTbl, ToSlice(key1)));

  auto res = s0->Get(mTbl, ToSlice(key1), valRead);
  ASSERT_TRUE(res && res.value() == 0); // got nothing

  res = s1->Get(mTbl, ToSlice(key1), valRead);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(valRead, val1);

  s0->AbortTx();
  s1->CommitTx();

  res = s0->Get(mTbl, ToSlice(key1), valRead, true);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(valRead, val1);

  res = s1->Get(mTbl, ToSlice(key1), valRead, true);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(valRead, val1);
}

TEST_F(AbortTest, AfterInsertOnRemove) {
  auto* s0 = mStore->GetSession(0);
  auto* s1 = mStore->GetSession(1);

  std::string key1("1"), val1("10"), val11("11");
  std::string valRead;
  ASSERT_TRUE(s0->Put(mTbl, ToSlice(key1), ToSlice(val1), true));

  s0->StartTx();
  s1->StartTx();
  ASSERT_TRUE(s0->Delete(mTbl, ToSlice(key1)));

  auto res = s0->Get(mTbl, ToSlice(key1), valRead);
  ASSERT_TRUE(res && res.value() == 0); // got nothing

  res = s1->Get(mTbl, ToSlice(key1), valRead);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(valRead, val1);

  // insert on removed key
  ASSERT_TRUE(s0->Put(mTbl, ToSlice(key1), ToSlice(val11)));
  res = s0->Get(mTbl, ToSlice(key1), valRead);
  ASSERT_TRUE(res && res.value() == 1); // get the uncommitted value
  ASSERT_EQ(valRead, val11);

  res = s1->Get(mTbl, ToSlice(key1), valRead);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(valRead, val1);

  s0->AbortTx();
  s1->CommitTx();

  res = s0->Get(mTbl, ToSlice(key1), valRead, true);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(valRead, val1);

  res = s1->Get(mTbl, ToSlice(key1), valRead, true);
  ASSERT_TRUE(res && res.value() == 1); // got the old value
  ASSERT_EQ(valRead, val1);
}

} // namespace leanstore::test
