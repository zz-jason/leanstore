#pragma once

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/family.h>
#include <prometheus/registry.h>

#include <memory>

namespace leanstore::telemetry {

/// All the counter metrics are listed here. Normally, you only need to add a
/// new metric here and then all the object members and functions will be
/// generated automatically. For example, define TxKvUpdate here and then the
/// following members and functions will be generated:
///   - prometheus::Counter* mTxKvUpdate;
///   - void IncTxKvUpdate(double val = 1); // increase the counter
///   - double GetTxKvUpdate(); // get the current counter value
#define DO_WITH_METRIC_COUNTER(ACTION, ...)                                    \
  ACTION(TxKvUpdate, "number of tx kv updates", __VA_ARGS__)                   \
  ACTION(TxKvLookup, "number of tx kv lookups", __VA_ARGS__)                   \
  ACTION(TxCommit, "number of tx commits", __VA_ARGS__)                        \
  ACTION(TxAbort, "number of tx aborts", __VA_ARGS__)

/// Macro to declare a counter metric
#define DECLARE_METRIC_COUNTER(metricName, help, ...)                          \
  prometheus::Counter* m##metricName;

/// Macro to initialize a counter metric
#define INIT_METRIC_COUNTER(metricName, help, ...)                             \
  m##metricName =                                                              \
      &createCounterFamily(#metricName, help)->Add({{"store", "leanstore"}});

/// Macro to define a function to increase a counter metric
#define INC_METRIC_COUNTER(metricName, help, ...)                              \
  void Inc##metricName(double val = 1) { m##metricName->Increment(val); }

/// Macro to define a function to get the current value of a counter metric
#define GET_METRIC_COUNTER(metricName, help, ...)                              \
  double Get##metricName() { return m##metricName->Value(); }

/// The MetricsManager class is used to manage all the metrics for one
/// LeanStore. It's expected to be a singleton inside a LeanStore instance.
class MetricsManager {
public:
  MetricsManager() {
    // create an http server running on port 8080
    prometheus::Exposer exposer{"0.0.0.0:2345"};

    // create a metrics registry
    mRegistry = std::make_shared<prometheus::Registry>();

    DO_WITH_METRIC_COUNTER(INIT_METRIC_COUNTER);
  }

  DO_WITH_METRIC_COUNTER(INC_METRIC_COUNTER);

  DO_WITH_METRIC_COUNTER(GET_METRIC_COUNTER);

private:
  prometheus::Family<prometheus::Counter>* createCounterFamily(
      std::string metricName, std::string help) {
    return &prometheus::BuildCounter()
                .Name(metricName)
                .Help(help)
                .Register(*mRegistry);
  }

  std::shared_ptr<prometheus::Registry> mRegistry;

  DO_WITH_METRIC_COUNTER(DECLARE_METRIC_COUNTER);
};

#undef DECLARE_METRIC_COUNTER
#undef INIT_METRIC_COUNTER

} // namespace leanstore::telemetry