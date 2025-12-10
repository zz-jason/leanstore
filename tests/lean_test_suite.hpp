#include <gtest/gtest.h>

#include <format>

namespace leanstore::test {

class LeanTestSuite : public ::testing::Test {
protected:
  LeanTestSuite() = default;
  ~LeanTestSuite() override = default;

  static constexpr auto kCommonTestDir = "/tmp/leanstore/test";

  std::string TestCaseStoreDir() const {
    return std::format("{}/{}/{}", kCommonTestDir, TestName(), CaseName());
  }

  std::string TestName() const {
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    return cur_test->test_suite_name();
  }

  std::string CaseName() const {
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    return cur_test->name();
  }
};

} // namespace leanstore::test