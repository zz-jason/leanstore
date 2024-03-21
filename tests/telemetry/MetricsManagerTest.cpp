
#include "telemetry/MetricsManager.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstring>

#include <fcntl.h>

namespace leanstore::telemetry::test {

class MetricsManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
  }

  void TearDown() override {
  }
};

TEST_F(MetricsManagerTest, Basic) {
  MetricsManager metricsManager;
  for (size_t i = 0; i < 100; i++) {
    metricsManager.IncTxAbort(100);
    EXPECT_NEAR(metricsManager.GetTxAbort(), 100 * (i + 1), 1e-6);
  }
}

} // namespace leanstore::telemetry::test
