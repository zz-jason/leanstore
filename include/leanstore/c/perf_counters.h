#ifndef LEANSTORE_C_PERF_COUNTERS_H
#define LEANSTORE_C_PERF_COUNTERS_H

#include <stdatomic.h>

#ifdef __cplusplus
extern "C" {
#endif

/// NOLINTBEGIN

/// The counter type.
typedef atomic_ullong lean_counter_t;

/// The performance counters for each worker.
typedef struct lean_perf_counters {

  // ---------------------------------------------------------------------------
  // Transaction related counters
  // ---------------------------------------------------------------------------

  /// The number of transactions committed.
  lean_counter_t tx_committed_;

  /// The number of transactions commit wait.
  lean_counter_t tx_commit_wait_;

  /// The number of transactions aborted.
  lean_counter_t tx_aborted_;

  //// The number of transactions with remote dependencies.
  lean_counter_t tx_with_remote_dependencies_;

  /// The number of transactions without remote dependencies.
  lean_counter_t tx_without_remote_dependencies_;

  /// The number of short running transactions.
  lean_counter_t tx_short_running_;

  /// The number of long running transactions.
  lean_counter_t tx_long_running_;

  // ---------------------------------------------------------------------------
  // MVCC concurrency control related counters
  // ---------------------------------------------------------------------------

  /// The number of LCB query executed.
  lean_counter_t lcb_executed_;

  /// The total latency of LCB query in nanoseconds.
  lean_counter_t lcb_total_lat_ns_;

  // ---------------------------------------------------------------------------
  // MVCC garbage collection related counters
  // ---------------------------------------------------------------------------

  /// The number of MVCC garbage collection executed.
  lean_counter_t gc_executed_;

  /// The total latency of MVCC garbage collection in nanoseconds.
  lean_counter_t gc_total_lat_ns_;

  // ---------------------------------------------------------------------------
  // Contention split related counters
  // ---------------------------------------------------------------------------

  /// The number of contention split succeed.
  lean_counter_t contention_split_succeed_;

  /// The number of contention split failed.
  lean_counter_t contention_split_failed_;

  /// The number of normal split succeed.
  lean_counter_t split_succeed_;

  /// The number of normal split failed.
  lean_counter_t split_failed_;

} lean_perf_counters;

lean_perf_counters* lean_current_perf_counters();

/// NOLINTEND

#ifdef __cplusplus
}
#endif

#endif // LEANSTORE_C_PERF_COUNTERS_H