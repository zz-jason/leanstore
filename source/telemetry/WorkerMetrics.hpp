#pragma once

#include "MetricsManager.hpp"

#include <prometheus/counter.h>

#define DECR_WORKER_COUNTER(name, ...) prometheus::Counter* m##name = nullptr;

#define INIT_WORKER_COUNTER(name, manager, store, worker, ...)                 \
  m##name = manager->GetWorkerCounter(manager->m##name##Family, store, worker);

#define DECR_WORKER_HISTOGRAM(name, ...)                                       \
  prometheus::Histogram* m##name = nullptr;

#define INIT_WORKER_HISTOGRAM(name, manager, store, worker, ...)               \
  m##name =                                                                    \
      manager->GetWorkerHistogram(manager->m##name##Family, store, worker);

namespace leanstore {

class WorkerMetrics {
public:
  ACTION_ON_WORKER_COUNTERS(DECR_WORKER_COUNTER);
  ACTION_ON_WORKER_HISTOGRAMS(DECR_WORKER_HISTOGRAM);

public:
  WorkerMetrics(MetricsManager* manager, const std::string& store,
                const std::string& worker) {
    ACTION_ON_WORKER_COUNTERS(INIT_WORKER_COUNTER, manager, store, worker);
    ACTION_ON_WORKER_HISTOGRAMS(INIT_WORKER_HISTOGRAM, manager, store, worker);
  }
};

} // namespace leanstore

#undef DECR_WORKER_COUNTER
#undef INIT_WORKER_COUNTER

#undef DECR_WORKER_HISTOGRAM
#undef INIT_WORKER_HISTOGRAM