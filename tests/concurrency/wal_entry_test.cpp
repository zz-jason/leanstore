#include "leanstore/concurrency/wal_entry.hpp"
#include "utils/to_json.hpp"

#include <gtest/gtest.h>

namespace leanstore::cr::test {

class WalEntryTest : public ::testing::Test {};

TEST_F(WalEntryTest, Size) {
  EXPECT_EQ(sizeof(WalEntry), 1);
  EXPECT_EQ(sizeof(WalTxAbort), 9);
  EXPECT_EQ(sizeof(WalTxFinish), 9);
  EXPECT_EQ(sizeof(WalCarriageReturn), 3);
  EXPECT_EQ(sizeof(WalEntryComplex), 57);
}

constexpr char kCrc32[] = "crc32";
constexpr char kLsn[] = "lsn";
constexpr char kSize[] = "size";
constexpr char kType[] = "type";
constexpr char kTxId[] = "tx_id";
constexpr char kWorkerId[] = "worker_id";
constexpr char kPrevLsn[] = "prev_lsn";
constexpr char kPsn[] = "psn";
constexpr char kTreeId[] = "tree_id";
constexpr char kPageId[] = "page_id";

TEST_F(WalEntryTest, ToJsonString) {
  // WalTxAbort
  {
    auto wal = WalTxAbort(0);
    auto wal_str = utils::ToJsonString(&wal);
    EXPECT_TRUE(wal_str.contains("kTxAbort"));
    EXPECT_TRUE(wal_str.contains(kType));
    EXPECT_TRUE(wal_str.contains(kTxId));
  }

  // WalTxFinish
  {
    auto wal = WalTxFinish(0);
    auto wal_str = utils::ToJsonString(&wal);
    EXPECT_TRUE(wal_str.contains("kTxFinish"));
    EXPECT_TRUE(wal_str.contains(kType));
    EXPECT_TRUE(wal_str.contains(kTxId));
  }

  // WalCarriageReturn
  {
    auto wal = WalCarriageReturn(0);
    auto wal_str = utils::ToJsonString(&wal);
    EXPECT_TRUE(wal_str.contains("kCarriageReturn"));
    EXPECT_TRUE(wal_str.contains(kType));
  }

  // WalEntryComplex
  {
    auto wal = WalEntryComplex(0, 0, 0, 0, 0, 0, 0, 0);
    auto wal_str = utils::ToJsonString(&wal);
    EXPECT_TRUE(wal_str.contains("kComplex"));
    EXPECT_TRUE(wal_str.contains(kType));
    EXPECT_TRUE(wal_str.contains(kTxId));
    EXPECT_TRUE(wal_str.contains(kWorkerId));
    EXPECT_TRUE(wal_str.contains(kPrevLsn));
    EXPECT_TRUE(wal_str.contains(kPsn));
    EXPECT_TRUE(wal_str.contains(kTreeId));
    EXPECT_TRUE(wal_str.contains(kPageId));
  }
}

} // namespace leanstore::cr::test