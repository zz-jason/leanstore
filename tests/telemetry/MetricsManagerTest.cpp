#include "telemetry/MetricsManager.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <httplib.h>

#include <cstddef>
#include <cstring>
#include <format>
#include <iostream>

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
  FLAGS_enable_metrics = true;
  auto res = leanstore::LeanStore::Open();
  ASSERT_TRUE(res);

  auto store = std::move(res.value());
  METRIC_COUNTER_INC(store->mMetricsManager, tx_abort_total, 100);

  httplib::Client cli("0.0.0.0", FLAGS_metrics_port);

  auto result = cli.Get("/metrics");
  ASSERT_TRUE(result) << "Error: " << result.error();
  ASSERT_EQ(result->status, httplib::StatusCode::OK_200)
      << "HTTP status: " << result->status;
  ASSERT_TRUE(result->body.contains("tx_abort_total"));
}

} // namespace leanstore::telemetry::test
