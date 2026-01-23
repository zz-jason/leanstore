#include "common/lean_test_suite.hpp"
#include "leanstore/base/optional.hpp"

#include "gtest/gtest.h"
#include <gtest/gtest.h>

#include <cstring>
#include <format>
#include <memory>
#include <string>
#include <vector>

namespace leanstore::test {

class OptionalTest : public LeanTestSuite {};

// Test data structure with move semantics
struct TestData {
  std::string name_;
  std::vector<std::string> values_;

  TestData() = default;
  TestData(std::string n, std::vector<std::string> v) : name_(std::move(n)), values_(std::move(v)) {
  }

  TestData(TestData&&) = default;
  TestData& operator=(TestData&&) = default;

  TestData(const TestData&) = delete;
  TestData& operator=(const TestData&) = delete;
};

TEST_F(OptionalTest, MoveSemantics) {
  std::vector<std::string> test_values;
  for (int i = 0; i < 5; i++) {
    test_values.push_back(std::format("value_{}", i));
  }

  TestData test_data("test_array", std::move(test_values));

  auto opt1 = Optional<TestData>(std::move(test_data));
  ASSERT_TRUE(opt1);

  ASSERT_EQ(opt1->name_, "test_array");
  ASSERT_EQ(opt1->values_.size(), 5);

  // Test move from optional
  auto opt2 = Optional<TestData>(std::move(*opt1));
  ASSERT_TRUE(opt2);
  ASSERT_EQ(opt2->values_.size(), 5);
}

} // namespace leanstore::test
