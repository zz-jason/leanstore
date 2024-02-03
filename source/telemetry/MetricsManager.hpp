#pragma once

#include "prometheus/family.h"
#include "prometheus/gauge.h"
#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include <memory>
#include <string>

namespace leanstore {

#define ACTION_ON_WORKER_COUNTERS(ACTION, ...)                                 \
  ACTION(TxCommittedShort, __VA_ARGS__)                                        \
  ACTION(TxCommittedLong, __VA_ARGS__)                                         \
  ACTION(TxAbortedShort, __VA_ARGS__)                                          \
  ACTION(TxAbortedLong, __VA_ARGS__)

#define ACTION_ON_WORKER_HISTOGRAMS(ACTION, ...)                               \
  ACTION(TxLatency, __VA_ARGS__)                                               \
  ACTION(TxSeekToLeaf, __VA_ARGS__)

#define DECR_WORKER_COUNTER(name, ...)                                         \
  prometheus::Family<prometheus::Counter>* m##name##Family = nullptr;

#define DECR_WORKER_HISTOGRAM(name, ...)                                       \
  prometheus::Family<prometheus::Histogram>* m##name##Family = nullptr;

class MetricsManager {
public:
  static MetricsManager* Get();

public:
  std::unique_ptr<prometheus::Exposer> mExposer;

  std::shared_ptr<prometheus::Registry> mRegistry;

  ACTION_ON_WORKER_COUNTERS(DECR_WORKER_COUNTER);
  ACTION_ON_WORKER_HISTOGRAMS(DECR_WORKER_HISTOGRAM);

public:
  MetricsManager(const std::string& url = "") : mExposer(nullptr), mRegistry() {
    // create a http server on the target address to expose metrics
    if (!url.empty()) {
      mExposer = std::make_unique<prometheus::Exposer>(url);
    }

    // register predefined metrics
    registerBufferPoolCounters();
    registerBufferPoolGuages();
    registerBufferPoolHistograms();

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
    static constexpr std::string kLabelStore = "store";
    static constexpr std::string kLabelWorker = "worker";
    return &family->Add({{kLabelStore, store}, {kLabelWorker, worker}});
  }

  prometheus::Histogram* GetWorkerHistogram(
      prometheus::Family<prometheus::Histogram>* family,
      const std::string& store, const std::string& worker) {
    // static constexpr std::string kLabelStore = "store";
    // static constexpr std::string kLabelWorker = "worker";
    // return &family->Add({{kLabelStore, store}, {kLabelWorker, worker}});
    return nullptr;
  }

private:
  void registerBufferPoolCounters() {
  }

  void registerBufferPoolGuages() {
  }

  void registerBufferPoolHistograms() {
  }
};

#undef DECR_WORKER_COUNTER
#undef DECR_WORKER_HISTOGRAM

} // namespace leanstore