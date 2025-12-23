#include "lean_test_suite.hpp"
#include "leanstore/cpp/base/range_splits.hpp"

#include "gtest/gtest.h"
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>

namespace leanstore::test {

class RangeSplitterTest : public LeanTestSuite {};

TEST_F(RangeSplitterTest, SingleSplit) {
  auto ranges = RangeSplits<int64_t>(5, 1);
  auto expected_ranges = std::vector<leanstore::RangeSplits<int64_t>::Range>{{0, 5}};
  for (size_t i = 0; i < expected_ranges.size(); i++) {
    auto range = ranges[i];
    EXPECT_EQ(range.begin(), expected_ranges[i].begin());
    EXPECT_EQ(range.end(), expected_ranges[i].end());
  }

  size_t idx = 0;
  for (auto range : ranges) {
    EXPECT_EQ(range.begin(), expected_ranges[idx].begin());
    EXPECT_EQ(range.end(), expected_ranges[idx].end());
    idx++;
  }
}

TEST_F(RangeSplitterTest, EvenSplit) {
  auto ranges = RangeSplits<int64_t>(12, 4);
  auto expected_ranges =
      std::vector<leanstore::RangeSplits<int64_t>::Range>{{0, 3}, {3, 6}, {6, 9}, {9, 12}};
  for (size_t i = 0; i < expected_ranges.size(); i++) {
    auto range = ranges[i];
    EXPECT_EQ(range.begin(), expected_ranges[i].begin());
    EXPECT_EQ(range.end(), expected_ranges[i].end());
  }

  size_t idx = 0;
  for (auto range : ranges) {
    EXPECT_EQ(range.begin(), expected_ranges[idx].begin());
    EXPECT_EQ(range.end(), expected_ranges[idx].end());
    idx++;
  }
}

TEST_F(RangeSplitterTest, UnevenSplit) {
  auto ranges = RangeSplits<int64_t>(10, 3);
  auto expected_ranges =
      std::vector<leanstore::RangeSplits<int64_t>::Range>{{0, 4}, {4, 7}, {7, 10}};
  for (size_t i = 0; i < expected_ranges.size(); i++) {
    auto range = ranges[i];
    EXPECT_EQ(range.begin(), expected_ranges[i].begin());
    EXPECT_EQ(range.end(), expected_ranges[i].end());
  }

  size_t idx = 0;
  for (auto range : ranges) {
    EXPECT_EQ(range.begin(), expected_ranges[idx].begin());
    EXPECT_EQ(range.end(), expected_ranges[idx].end());
    idx++;
  }
}

} // namespace leanstore::test
