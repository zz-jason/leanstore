#include "LeanStore.hpp"
#include "TxKV.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "storage/buffer-manager/BufferFrame.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "utils/DebugFlags.hpp"
#include "utils/Defer.hpp"
#include "utils/RandomGenerator.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <iostream>
#include <mutex>
#include <shared_mutex>

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
    mStore = StoreFactory::GetLeanStoreMVCC("/tmp/AnomaliesTest", 3);
    ASSERT_NE(mStore, nullptr);

    auto* session = mStore->GetSession(0);
    mTblName = RandomGenerator::RandomAlphString(10);
    auto res = session->CreateTable(mTblName, true);
    ASSERT_TRUE(res);

    mTbl = res.value();
    ASSERT_NE(mTbl, nullptr);
  }

  void TearDown() override {
    auto* session = mStore->GetSession(0);
    auto res = session->DropTable(mTblName, true);
    ASSERT_TRUE(res);
  }
};

// G0: Write Cycles (dirty writes)
//
// set session transaction isolation level read uncommitted; begin; -- T1
// set session transaction isolation level read uncommitted; begin; -- T2
// update test set value = 11 where id = 1; -- T1
// update test set value = 12 where id = 1; -- T2, BLOCKS
// update test set value = 21 where id = 2; -- T1
// commit; -- T1. This unblocks T2
// select * from test; -- T1. Shows 1 => 12, 2 => 21
// update test set value = 22 where id = 2; -- T2
// commit; -- T2
// select * from test; -- either. Shows 1 => 12, 2 => 22
TEST_F(AnomaliesTest, G0) {
  // create table test (id int primary key, value int) engine=innodb;
  // insert into test (id, value) values (1, 10), (2, 20);
}

// G1a: Aborted Reads (dirty reads, cascaded aborts)
// G1b: Intermediate Reads (dirty reads)
// G1c: Circular Information Flow (dirty reads)
// OTV: Observed Transaction Vanishes

// PMP: Predicate-Many-Preceders
//
// set session transaction isolation level read committed; begin; -- T1
// set session transaction isolation level read committed; begin; -- T2
// select * from test where value = 30; -- T1. Returns nothing
// insert into test (id, value) values(3, 30); -- T2
// commit; -- T2
// select * from test where value % 3 = 0; -- T1. Returns the newly inserted row
// commit; -- T1
TEST_F(AnomaliesTest, NoPMP) {
  auto* s0 = mStore->GetSession(0);
  auto* s1 = mStore->GetSession(1);
  auto* s2 = mStore->GetSession(2);

  // Prepare: insert into test (id, value) values (1, 10), (2, 20);
  std::string key1("1"), val1("10");
  std::string key2("2"), val2("20");
  std::string key3("3"), val3("30");
  std::string result;

  EXPECT_TRUE(s0->Put(mTbl, ToSlice(key1), ToSlice(val1), true));
  EXPECT_TRUE(s0->Put(mTbl, ToSlice(key2), ToSlice(val2), true));

  s1->StartTx();
  s2->StartTx();
  EXPECT_FALSE(s1->Get(mTbl, ToSlice(key3), result));
  EXPECT_TRUE(s2->Put(mTbl, ToSlice(key3), ToSlice(val3)));
  s2->CommitTx();
  EXPECT_FALSE(s1->Get(mTbl, ToSlice(key3), result));
  s1->CommitTx();
}

// P4: Lost Update
// G-single: Single Anti-dependency Cycles (read skew)
// G2-item: Item Anti-dependency Cycles (write skew on disjoint read)
// G2: Anti-Dependency Cycles (write skew on predicate read)

} // namespace test
} // namespace leanstore

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}