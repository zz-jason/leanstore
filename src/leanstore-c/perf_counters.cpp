#include "leanstore-c/perf_counters.h"

namespace {
thread_local PerfCounters tls_perf_counters;
} // namespace

PerfCounters* GetTlsPerfCounters() {
  return &tls_perf_counters;
}