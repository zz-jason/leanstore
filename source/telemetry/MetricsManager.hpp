#pragma once

#include "prometheus/family.h"
#include "prometheus/gauge.h"
#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include <memory>
#include <string>
#include <vector>

namespace leanstore {

/// Labels
static constexpr std::string kLabelStore = "store";
static constexpr std::string kLabelWorker = "worker";

/// Histogram boundaries
static const std::vector<double> kLatenciesUs = {
    2, 4, 8, 16, 32, 64, 128, 256, 500, 1000, 1000000, 10000000};

/// All the worker counters. New counters should be added here.
#define ACTION_ON_WORKER_COUNTERS(ACTION, ...)                                 \
  ACTION(TxCommittedShort, __VA_ARGS__)                                        \
  ACTION(TxCommittedLong, __VA_ARGS__)                                         \
  ACTION(TxAbortedShort, __VA_ARGS__)                                          \
  ACTION(TxAbortedLong, __VA_ARGS__)

/// All the worker histograms. New histograms should be added here.
#define ACTION_ON_WORKER_HISTOGRAMS(ACTION, ...)                               \
  ACTION(TxLatencyShort, kLatenciesUs, __VA_ARGS__)                            \
  ACTION(TxLatencyLong, kLatenciesUs, __VA_ARGS__)                             \
  ACTION(SeekToLeafShort, kLatenciesUs, __VA_ARGS__)                           \
  ACTION(SeekToLeafLong, kLatenciesUs, __VA_ARGS__)

/// Macros to declare and define worker counters.
#define DECR_COUNTER_FAMILY(name, ...)                                         \
  prometheus::Family<prometheus::Counter>* m##name##Family = nullptr;

#define INIT_COUNTER_FAMILY(name, registry, ...)                               \
  m##name##Family = &prometheus::BuildCounter().Name(#name).Register(registry);

/// Macros to declare and define worker histograms.
#define DECR_HISTOGRAM_FAMILY(name, boundaries, ...)                           \
  prometheus::Family<prometheus::Histogram>* m##name##Family = nullptr;

/// MetricsManager manages all the metrics in the system. All counters, gauges,
/// and histograms families are registered in the MetricsManager.
class MetricsManager {
public:
  static MetricsManager* Get();

public:
  /// The prometheus exposer to expose metrics to the outside world.
  std::unique_ptr<prometheus::Exposer> mExposer;

  /// The prometheus registry to store all the metrics.
  std::shared_ptr<prometheus::Registry> mRegistry;

  /// All the worker counter families.
  ACTION_ON_WORKER_COUNTERS(DECR_COUNTER_FAMILY);

  /// All the worker histogram families.
  ACTION_ON_WORKER_HISTOGRAMS(DECR_HISTOGRAM_FAMILY);

public:
  MetricsManager(const std::string& url = "") : mExposer(nullptr), mRegistry() {
    // create a http server on the target address to expose metrics
    if (!url.empty()) {
      mExposer = std::make_unique<prometheus::Exposer>(url);
    }

    ACTION_ON_WORKER_COUNTERS(INIT_COUNTER_FAMILY, *mRegistry);
    mTxCommittedShortFamily = &prometheus::BuildCounter()
                                   .Name("TxCommittedShort")
                                   .Register(*mRegistry);

    // ask the exposer to scrape the registry on incoming HTTP requests
    if (mExposer) {
      mExposer->RegisterCollectable(mRegistry);
    }
  }

  ~MetricsManager() {
  }

  prometheus::Counter* GetWorkerCounter(
      prometheus::Family<prometheus::Counter>* family, const std::string& store,
      const std::string& worker) {
    return &family->Add({{kLabelStore, store}, {kLabelWorker, worker}});
  }

  prometheus::Histogram* GetWorkerHistogram(
      prometheus::Family<prometheus::Histogram>* family,
      const std::string& store, const std::string& worker,
      const std::vector<double>& boundaries) {
    return &family->Add({{kLabelStore, store}, {kLabelWorker, worker}},
                        boundaries);
    return nullptr;
  }
};

#undef DECR_COUNTER_FAMILY
#undef DECR_HISTOGRAM_FAMILY

} // namespace leanstore