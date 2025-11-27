#include "lean_test_suite.hpp"
#include "leanstore/cpp/base/error.hpp"

#include "gtest/gtest.h"
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>

namespace leanstore::test {

class ErrorTest : public LeanTestSuite {};

TEST_F(ErrorTest, CreateAndCompareErrors) {
  auto err1 = Error::FileRead("file1.path", 42, "I/O error");
  auto err2 = Error::FileRead("file1.path", 42, "I/O error");
  auto err3 = Error::FileWrite("file1.path", 42, "I/O error");
  auto err4 = Error::General("Some general error");

  // Test equality
  ASSERT_EQ(err1, err2);
  ASSERT_NE(err1, err3);
  ASSERT_NE(err1, err4);

  // Test error code retrieval
  ASSERT_EQ(err1.GetCode(), static_cast<int64_t>(Error::Code::kFileRead));
  ASSERT_EQ(err4.GetCode(), static_cast<int64_t>(Error::Code::kGeneral));

  // Test string representation
  std::string expected_msg =
      "[ERR-005] Read file failed, file=file1.path, errno=42, strerror=I/O error";
  auto err_str = err1.ToString();
  // ASSERT_EQ(err_str, "");
  ASSERT_TRUE(err_str.starts_with(expected_msg));
  ASSERT_TRUE(err_str.contains("Stack trace"));
}

} // namespace leanstore::test