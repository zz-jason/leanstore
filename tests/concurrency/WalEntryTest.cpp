#include "concurrency/WalEntry.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

namespace leanstore::cr::test {

class WalEntryTest : public ::testing::Test {};

TEST_F(WalEntryTest, Size) {
  EXPECT_EQ(sizeof(WalEntry), 48);
  EXPECT_EQ(sizeof(WalEntryComplex), 72);
}

} // namespace leanstore::cr::test