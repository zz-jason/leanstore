
#include "telemetry/MetricsManager.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <httplib.h>

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
    METRIC_COUNTER_INC(metricsManager, tx_abort_total, 100);
  }
}

} // namespace leanstore::telemetry::test
