#include "concurrency/WalEntry.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <memory>
#include <tuple>
#include <unordered_map>
#include <vector>

namespace leanstore::cr::test {

class WalEntryTest : public ::testing::Test {};

TEST_F(WalEntryTest, Size) {
  EXPECT_EQ(sizeof(WalEntry), 1);
  EXPECT_EQ(sizeof(WalTxAbort), 9);
  EXPECT_EQ(sizeof(WalTxFinish), 9);
  EXPECT_EQ(sizeof(WalCarriageReturn), 9);
  EXPECT_EQ(sizeof(WalEntryComplex), 57);
}

TEST_F(WalEntryTest, ToJsonString) {
  // WalTxAbort
  {
    auto wal = WalTxAbort(0);
    auto walStr = WalEntry::ToJsonString(&wal);
    EXPECT_TRUE(walStr.contains("kTxAbort"));
    EXPECT_TRUE(walStr.contains(kType));
    EXPECT_TRUE(walStr.contains(kTxId));
  }

  // WalTxFinish
  {
    auto wal = WalTxFinish(0);
    auto walStr = WalEntry::ToJsonString(&wal);
    EXPECT_TRUE(walStr.contains("kTxFinish"));
    EXPECT_TRUE(walStr.contains(kType));
    EXPECT_TRUE(walStr.contains(kTxId));
  }

  // WalCarriageReturn
  {
    auto wal = WalCarriageReturn(0);
    auto walStr = WalEntry::ToJsonString(&wal);
    EXPECT_TRUE(walStr.contains("kCarriageReturn"));
    EXPECT_TRUE(walStr.contains(kType));
  }

  // WalEntryComplex
  {
    auto wal = WalEntryComplex(0, 0, 0, 0, 0, 0, 0, 0);
    auto walStr = WalEntry::ToJsonString(&wal);
    EXPECT_TRUE(walStr.contains("kComplex"));
    EXPECT_TRUE(walStr.contains(kType));
    EXPECT_TRUE(walStr.contains(kTxId));
    EXPECT_TRUE(walStr.contains(kWorkerId));
    EXPECT_TRUE(walStr.contains(kPrevLsn));
    EXPECT_TRUE(walStr.contains(kGsn));
    EXPECT_TRUE(walStr.contains(kTreeId));
    EXPECT_TRUE(walStr.contains(kPageId));
  }
}

} // namespace leanstore::cr::test