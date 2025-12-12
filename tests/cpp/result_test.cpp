#include "lean_test_suite.hpp"
#include "leanstore/cpp/base/error.hpp"
#include "leanstore/cpp/base/result.hpp"

#include "gtest/gtest.h"
#include <gtest/gtest.h>

#include <cstring>

namespace leanstore::test {

class ResultTest : public LeanTestSuite {};

TEST_F(ResultTest, CreateAndCompareResults) {
  auto res1 = Result<int>(42);
  auto res2 = Result<int>(42);
  auto err_res1 = Result<int>(Error::General("Some error"));

  auto res3 = Result<void>();
  auto res4 = Result<void>();
  auto err_res2 = Result<void>(Error::General("Some void error"));

  // Test equality for value Results
  ASSERT_EQ(res1, res2);
  ASSERT_NE(res1, err_res1);

  // Test equality for void Results
  ASSERT_EQ(res3, res4);
  ASSERT_NE(res3, err_res2);
  Result<void> res5 = {};
  ASSERT_EQ(res3, res5);
}

} // namespace leanstore::test
