#pragma once

#include "leanstore/Exceptions.hpp"
#include "telemetry/MetricsHttpExposer.hpp"

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/family.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include <memory>
#include <vector>

#include <fcntl.h>

namespace leanstore::telemetry {

const std::vector<double> kBoundariesUs{
    0.001,    // 1ns
    0.004,    // 4ns
    0.016,    // 16ns
    0.064,    // 64ns
    0.256,    // 256ns
    1,        // 1us
    4,        // 4us
    16,       // 16us
    64,       // 64us
    256,      // 256us
    1000,     // 1ms
    4000,     // 4ms
    16000,    // 16ms
    64000,    // 64ms
    256000,   // 256ms
    1000000,  // 1s
    4000000,  // 4s
    16000000, // 16s
};

// -----------------------------------------------------------------------------
// Macros exported
// -----------------------------------------------------------------------------

/// All the counter metrics.
#define METRIC_COUNTER_LIST(ACTION, ...)                                       \
  ACTION(tx_kv_update_total, "number of tx kv updates", __VA_ARGS__)           \
  ACTION(tx_kv_lookup_total, "number of tx kv lookups", __VA_ARGS__)           \
  ACTION(tx_abort_total, "number of tx aborts", __VA_ARGS__)                   \
  ACTION(tx_version_read_total, "mvcc versions read", __VA_ARGS__)             \
  ACTION(group_committer_disk_write_total, "", __VA_ARGS__)

/// All the histogram metrics are listed here.
#define METRIC_HIST_LIST(ACTION, ...)                                          \
  ACTION(tx_commit_wal_wait_us, kBoundariesUs, "", __VA_ARGS__)                \
  ACTION(tx_kv_lookup_us, kBoundariesUs, "", __VA_ARGS__)                      \
  ACTION(tx_kv_update_us, kBoundariesUs, "", __VA_ARGS__)                      \
  ACTION(group_committer_prep_iocbs_us, kBoundariesUs, "", __VA_ARGS__)        \
  ACTION(group_committer_write_iocbs_us, kBoundariesUs, "", __VA_ARGS__)       \
  ACTION(group_committer_commit_txs_us, kBoundariesUs, "", __VA_ARGS__)

/// Macro to update a counter metric
#define METRIC_COUNTER_INC(metricsMgr, metricName, value)                      \
  metricsMgr.inc_##metricName(value);

/// Macro to update a histogram metric
#define METRIC_HIST_OBSERVE(metricsMgr, metricName, value)                     \
  metricsMgr.observe_##metricName(value);

// -----------------------------------------------------------------------------
// Macros not exported for counters
// -----------------------------------------------------------------------------

/// Macro to declare a counter metric
#define DECLARE_METRIC_COUNTER(metricName, help, ...)                          \
  prometheus::Counter* m_##metricName;

/// Macro to initialize a counter metric
#define INIT_METRIC_COUNTER(metricName, help, ...)                             \
  m_##metricName = &createCounterFamily(#metricName, help)->Add({});

/// Macro to define a function to increase a counter metric
#define DEFINE_METRIC_FUNC_COUNTER_INC(metricName, help, ...)                  \
  void inc_##metricName(double val = 1) {                                      \
    COUNTERS_BLOCK() { m_##metricName->Increment(val); }                       \
  }

// -----------------------------------------------------------------------------
// Macros not exported for histograms
// -----------------------------------------------------------------------------

/// Macro to declare a histogram metric
#define DECLARE_METRIC_HIST(metricName, boundaries, help, ...)                 \
  prometheus::Histogram* m_##metricName;

/// Macro to initialize a histogram metric
#define INIT_METRIC_HIST(metricName, boundaries, help, ...)                    \
  m_##metricName =                                                             \
      &createHistogramFamily(#metricName, help)->Add({}, boundaries);

/// Macro to define a function to observe a value to a histogram
#define DEFINE_METRIC_FUNC_HIST_OBSERVE(metricName, boundaries, help, ...)     \
  void observe_##metricName(double val) {                                      \
    COUNTERS_BLOCK() { m_##metricName->Observe(val); }                         \
  }

// -----------------------------------------------------------------------------
// Macros not exported
// -----------------------------------------------------------------------------

/// Macro to define a function to get the current value of a counter metric
#define DEFINE_METRIC_FUNC_GET(metricName, help, ...)                          \
  double get_##metricName() { return m_##metricName->Value(); }

/// The MetricsManager class is used to manage all the metrics for one
/// LeanStore. It's expected to be a singleton inside a LeanStore instance.
class MetricsManager {
public:
  MetricsManager() : mExposer() {
    // create a metrics registry
    mRegistry = std::make_shared<prometheus::Registry>();

    METRIC_COUNTER_LIST(INIT_METRIC_COUNTER);
    METRIC_HIST_LIST(INIT_METRIC_HIST);

    mExposer.SetCollectable(mRegistry);
  }

  void Expose() {
    mExposer.Start();
  }

  METRIC_COUNTER_LIST(DEFINE_METRIC_FUNC_COUNTER_INC);
  METRIC_HIST_LIST(DEFINE_METRIC_FUNC_HIST_OBSERVE);

private:
  prometheus::Family<prometheus::Counter>* createCounterFamily(
      const std::string& metricName, const std::string& help) {
    return &prometheus::BuildCounter()
                .Name(metricName)
                .Help(help)
                .Register(*mRegistry);
  }

  prometheus::Family<prometheus::Histogram>* createHistogramFamily(
      const std::string& metricName, const std::string& help) {
    return &prometheus::BuildHistogram()
                .Name(metricName)
                .Help(help)
                .Register(*mRegistry);
  }

  static prometheus::Histogram::BucketBoundaries createLinearBuckets(
      double start, double end, double step) {
    auto bucketBoundaries = prometheus::Histogram::BucketBoundaries{};
    for (auto i = start; i < end; i += step) {
      bucketBoundaries.push_back(i);
    }
    return bucketBoundaries;
  }

  std::shared_ptr<prometheus::Registry> mRegistry;

  MetricsHttpExposer mExposer;

  METRIC_COUNTER_LIST(DECLARE_METRIC_COUNTER);
  METRIC_HIST_LIST(DECLARE_METRIC_HIST);
};

#undef DECLARE_METRIC_COUNTER
#undef INIT_METRIC_COUNTER
#undef DEFINE_METRIC_FUNC_COUNTER_INC
#undef DEFINE_METRIC_FUNC_GET

} // namespace leanstore::telemetry
