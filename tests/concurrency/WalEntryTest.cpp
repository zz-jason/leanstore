#include "concurrency/WalEntry.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <unordered_map>

namespace leanstore::cr::test {

class WalEntryTest : public ::testing::Test {};

TEST_F(WalEntryTest, Size) {
  EXPECT_EQ(sizeof(WalEntry), 33);
  EXPECT_EQ(sizeof(WalEntryComplex), 57);
}

TEST_F(WalEntryTest, ToJsonString) {
  auto typeNames = std::unordered_map<WalEntry::Type, std::string>{
      {WalEntry::Type::kTxStart, "kTxStart"},
      {WalEntry::Type::kTxCommit, "kTxCommit"},
      {WalEntry::Type::kTxAbort, "kTxAbort"},
      {WalEntry::Type::kTxFinish, "kTxFinish"},
      {WalEntry::Type::kCarriageReturn, "kCarriageReturn"},
      {WalEntry::Type::kComplex, "kComplex"}};

  for (auto& [type, name] : typeNames) {
    auto wal = WalEntrySimple(0, sizeof(WalEntrySimple), type);
    auto walStr = WalEntry::ToJsonString(&wal);
    EXPECT_TRUE(walStr.contains(name));
    EXPECT_TRUE(walStr.contains(kCrc32));
    EXPECT_TRUE(walStr.contains(kLsn));
    EXPECT_TRUE(walStr.contains(kSize));
    EXPECT_TRUE(walStr.contains(kType));
    EXPECT_TRUE(walStr.contains(kTxId));
    EXPECT_TRUE(walStr.contains(kWorkerId));
    EXPECT_TRUE(walStr.contains(kPrevLsn));

    if (type == WalEntry::Type::kComplex) {
      EXPECT_TRUE(walStr.contains(kGsn));
      EXPECT_TRUE(walStr.contains(kTreeId));
      EXPECT_TRUE(walStr.contains(kPageId));
    }
  }
}

} // namespace leanstore::cr::test