#include "leanstore/common/perf_counters.h"

#include "leanstore/utils/counter_util.hpp"

lean_perf_counters* lean_current_perf_counters() {
  return &leanstore::cr::tls_perf_counters;
}