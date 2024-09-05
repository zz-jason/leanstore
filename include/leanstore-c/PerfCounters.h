#ifndef LEANSTORE_PERF_COUNTERS_H
#define LEANSTORE_PERF_COUNTERS_H

#include <stdatomic.h>

#ifdef __cplusplus
extern "C" {
#endif

//! The counter type.
typedef atomic_ullong CounterType;

//! The performance counters for each worker.
typedef struct PerfCounters {

  // ---------------------------------------------------------------------------
  // Transaction related counters
  // ---------------------------------------------------------------------------

  //! The number of transactions committed.
  CounterType mTxCommitted;

  //! The number of transactions commit wait.
  CounterType mTxCommitWait;

  //! The number of transactions aborted.
  CounterType mTxAborted;

  ///! The number of transactions with remote dependencies.
  CounterType mTxWithRemoteDependencies;

  //! The number of transactions without remote dependencies.
  CounterType mTxWithoutRemoteDependencies;

  //! The number of short running transactions.
  CounterType mTxShortRunning;

  //! The number of long running transactions.
  CounterType mTxLongRunning;

  // ---------------------------------------------------------------------------
  // MVCC concurrency control related counters
  // ---------------------------------------------------------------------------

  //! The number of LCB query executed.
  CounterType mLcbExecuted;

  //! The total latency of LCB query in nanoseconds.
  CounterType mLcbTotalLatNs;

  // ---------------------------------------------------------------------------
  // MVCC garbage collection related counters
  // ---------------------------------------------------------------------------

  //! The number of MVCC garbage collection executed.
  CounterType mGcExecuted;

  //! The total latency of MVCC garbage collection in nanoseconds.
  CounterType mGcTotalLatNs;

  // ---------------------------------------------------------------------------
  // Contention split related counters
  // ---------------------------------------------------------------------------

  //! The number of contention split succeed.
  CounterType mContentionSplitSucceed;

  //! The number of contention split failed.
  CounterType mContentionSplitFailed;

  //! The number of normal split succeed.
  CounterType mSplitSucceed;

  //! The number of normal split failed.
  CounterType mSplitFailed;

} PerfCounters;

#ifdef __cplusplus
}
#endif

#endif