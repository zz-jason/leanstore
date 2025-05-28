#ifndef LEANSTORE_C_PERF_COUNTERS_H
#define LEANSTORE_C_PERF_COUNTERS_H

#include <stdatomic.h>

#ifdef __cplusplus
extern "C" {
#endif

/// The counter type.
typedef atomic_ullong CounterType;

/// The performance counters for each worker.
typedef struct PerfCounters {

  // ---------------------------------------------------------------------------
  // Transaction related counters
  // ---------------------------------------------------------------------------

  /// The number of transactions committed.
  CounterType tx_committed_;

  /// The number of transactions commit wait.
  CounterType tx_commit_wait_;

  /// The number of transactions aborted.
  CounterType tx_aborted_;

  //// The number of transactions with remote dependencies.
  CounterType tx_with_remote_dependencies_;

  /// The number of transactions without remote dependencies.
  CounterType tx_without_remote_dependencies_;

  /// The number of short running transactions.
  CounterType tx_short_running_;

  /// The number of long running transactions.
  CounterType tx_long_running_;

  // ---------------------------------------------------------------------------
  // MVCC concurrency control related counters
  // ---------------------------------------------------------------------------

  /// The number of LCB query executed.
  CounterType lcb_executed_;

  /// The total latency of LCB query in nanoseconds.
  CounterType lcb_total_lat_ns_;

  // ---------------------------------------------------------------------------
  // MVCC garbage collection related counters
  // ---------------------------------------------------------------------------

  /// The number of MVCC garbage collection executed.
  CounterType gc_executed_;

  /// The total latency of MVCC garbage collection in nanoseconds.
  CounterType gc_total_lat_ns_;

  // ---------------------------------------------------------------------------
  // Contention split related counters
  // ---------------------------------------------------------------------------

  /// The number of contention split succeed.
  CounterType contention_split_succeed_;

  /// The number of contention split failed.
  CounterType contention_split_failed_;

  /// The number of normal split succeed.
  CounterType split_succeed_;

  /// The number of normal split failed.
  CounterType split_failed_;

} PerfCounters;

#ifdef __cplusplus
}
#endif

#endif // LEANSTORE_C_PERF_COUNTERS_H