#include "telemetry/MetricsManager.hpp"

#include "leanstore/LeanStore.hpp"
#include "leanstore/Store.hpp"

#include <gtest/gtest.h>
#include <httplib.h>

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
  auto res = leanstore::LeanStore::Open(StoreOption{
      .mEnableMetrics = true,
  });
  ASSERT_TRUE(res);

  auto store = std::move(res.value());
  METRIC_COUNTER_INC(store->mMetricsManager, tx_abort_total, 100);

  httplib::Client cli("0.0.0.0", store->mStoreOption.mMetricsPort);

  auto result = cli.Get("/metrics");
  ASSERT_TRUE(result) << "Error: " << result.error();
  ASSERT_EQ(result->status, httplib::StatusCode::OK_200)
      << "HTTP status: " << result->status;
  ASSERT_TRUE(result->body.contains("tx_abort_total"));
}

} // namespace leanstore::telemetry::test
