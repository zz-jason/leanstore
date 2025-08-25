#include "leanstore-c/perf_counters.h"

#include "leanstore/utils/counter_util.hpp"

PerfCounters* GetTlsPerfCounters() {
  return &leanstore::cr::tls_perf_counters;
}