#pragma once

#include "shared-headers/Units.hpp"
#include "utils/EnumerableThreadLocal.hpp"

#include <atomic>

/// Define a macro to update a specific counter if MACRO_COUNTERS_ALL is
/// defined. Otherwise, do nothing.
#ifdef MACRO_COUNTERS_ALL
#define UPDATE_COUNTER(counter, value) counter += value;
#define COUNTER_INC(counter) counter++;
#else
#define UPDATE_COUNTER(counter, value) (void)0;
#define COUNTER_INC(counter, value) (void)0;
#endif

namespace leanstore {

struct WorkerCounters {
  static constexpr u64 kMaxResearchyCounter = 10;

  // ATTENTION: buffer overflow if more than max_dt_id in system are registered
  static constexpr u64 kMaxBTreeId = 1000;

  std::atomic<u64> mWorkerId = -1;

  /// --------------------------------------------------------------------------
  /// System throughput overviews
  /// --------------------------------------------------------------------------

  /// The committed transaction counter. Updated only when a short-running
  /// transaction is committed.
  std::atomic<u64> mTxCommittedShort = 0;

  /// The committed transaction counter. Updated only when a long-running
  /// transaction is committed.
  std::atomic<u64> mTxCommittedLong = 0;

  /// The aborted transaction counter. Updated only when a short-running
  /// transaction is aborted.
  std::atomic<u64> mTxAbortedShort = 0;

  /// The aborted transaction counter. Updated only when a long-running
  /// transaction is aborted.
  std::atomic<u64> mTxAbortedLong = 0;

  /// --------------------------------------------------------------------------
  /// Page allocation, read, and write counters
  /// --------------------------------------------------------------------------

  /// The page allocation counter, happens when a page is allocated from the
  /// buffer pool. Usefull to measure the buffer pool size and insert rate.
  std::atomic<u64> mPageAllocCounter = 0;

  /// The page read counter, happens when a page is read from the disk.
  std::atomic<u64> mPageReadCounter = 0;

  /// The page write counter, happens when a page is written to the disk.
  std::atomic<u64> mPageWriteCounter = 0;

  /// --------------------------------------------------------------------------
  /// BTree split and merge counters
  /// --------------------------------------------------------------------------

  /// The contention split counters
  std::atomic<u64> mContentionSplitSucceed = 0;
  std::atomic<u64> mContentionSplitFailed = 0;

  /// The page split counters. Contentions-split are included.
  std::atomic<u64> mPageSplits[kMaxBTreeId] = {0};

  /// The page merge counters
  std::atomic<u64> mPageMergeSucceed[kMaxBTreeId] = {0};
  std::atomic<u64> mPageMergeFailed[kMaxBTreeId] = {0};
  std::atomic<u64> mPageMergeParentSucceed[kMaxBTreeId] = {0};
  std::atomic<u64> mPageMergeParentFailed[kMaxBTreeId] = {0};
  std::atomic<u64> mXMergePartialCounter[kMaxBTreeId] = {0};
  std::atomic<u64> mXMergeFullCounter[kMaxBTreeId] = {0};

  // without structural change
  std::atomic<u64> dt_restarts_update_same_size[kMaxBTreeId] = {0};

  // includes insert, remove, update with different size
  std::atomic<u64> dt_restarts_structural_change[kMaxBTreeId] = {0};
  std::atomic<u64> dt_restarts_read[kMaxBTreeId] = {0};

  // temporary counter used to track some value for an idea in My mind
  std::atomic<u64> dt_researchy[kMaxBTreeId][kMaxResearchyCounter] = {};

  std::atomic<u64> dt_find_parent[kMaxBTreeId] = {0};
  std::atomic<u64> dt_find_parent_root[kMaxBTreeId] = {0};
  std::atomic<u64> dt_find_parent_slow[kMaxBTreeId] = {0};

  std::atomic<u64> dt_empty_leaf[kMaxBTreeId] = {0};
  std::atomic<u64> mGotoPageExclusive[kMaxBTreeId] = {0};
  std::atomic<u64> mGotoPageShared[kMaxBTreeId] = {0};
  std::atomic<u64> dt_next_tuple[kMaxBTreeId] = {0};
  std::atomic<u64> dt_next_tuple_opt[kMaxBTreeId] = {0};
  std::atomic<u64> dt_prev_tuple[kMaxBTreeId] = {0};
  std::atomic<u64> dt_prev_tuple_opt[kMaxBTreeId] = {0};
  std::atomic<u64> dt_inner_page[kMaxBTreeId] = {0};
  std::atomic<u64> dt_scan_asc[kMaxBTreeId] = {0};
  std::atomic<u64> dt_scan_desc[kMaxBTreeId] = {0};
  std::atomic<u64> dt_scan_callback[kMaxBTreeId] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<u64> dt_range_removed[kMaxBTreeId] = {0};
  std::atomic<u64> dt_append[kMaxBTreeId] = {0};
  std::atomic<u64> dt_append_opt[kMaxBTreeId] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<u64> cc_read_versions_visited[kMaxBTreeId] = {0};
  std::atomic<u64> cc_read_versions_visited_not_found[kMaxBTreeId] = {0};
  std::atomic<u64> cc_read_chains_not_found[kMaxBTreeId] = {0};
  std::atomic<u64> cc_read_chains[kMaxBTreeId] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<u64> cc_update_versions_visited[kMaxBTreeId] = {0};
  std::atomic<u64> cc_update_versions_removed[kMaxBTreeId] = {0};
  std::atomic<u64> cc_update_versions_kept[kMaxBTreeId] = {0};
  std::atomic<u64> cc_update_versions_kept_max[kMaxBTreeId] = {0};
  std::atomic<u64> cc_update_versions_skipped[kMaxBTreeId] = {0};
  std::atomic<u64> cc_update_versions_recycled[kMaxBTreeId] = {0};
  std::atomic<u64> cc_update_versions_created[kMaxBTreeId] = {0};
  std::atomic<u64> cc_update_chains[kMaxBTreeId] = {0};
  std::atomic<u64> cc_update_chains_hwm[kMaxBTreeId] = {0};
  std::atomic<u64> cc_update_chains_pgc[kMaxBTreeId] = {0};
  std::atomic<u64> cc_update_chains_pgc_skipped[kMaxBTreeId] = {0};
  std::atomic<u64> cc_update_chains_pgc_workers_visited[kMaxBTreeId] = {0};
  std::atomic<u64> cc_update_chains_pgc_heavy[kMaxBTreeId] = {0};
  std::atomic<u64> cc_update_chains_pgc_heavy_removed[kMaxBTreeId] = {0};
  std::atomic<u64> cc_update_chains_pgc_light[kMaxBTreeId] = {0};
  std::atomic<u64> cc_update_chains_pgc_light_removed[kMaxBTreeId] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<u64> cc_versions_space_inserted[kMaxBTreeId] = {0};
  std::atomic<u64> cc_versions_space_inserted_opt[kMaxBTreeId] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<u64> cc_todo_removed[kMaxBTreeId] = {0};
  std::atomic<u64> cc_todo_moved_gy[kMaxBTreeId] = {0};
  std::atomic<u64> cc_todo_oltp_executed[kMaxBTreeId] = {0};
  std::atomic<u64> cc_gc_long_tx_executed[kMaxBTreeId] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<u64> cc_fat_tuple_triggered[kMaxBTreeId] = {0};
  std::atomic<u64> cc_fat_tuple_convert[kMaxBTreeId] = {0};
  std::atomic<u64> cc_fat_tuple_decompose[kMaxBTreeId] = {0};

  // WAL
  std::atomic<u64> wal_write_bytes = 0;
  std::atomic<u64> wal_read_bytes = 0;
  std::atomic<u64> wal_buffer_hit = 0;
  std::atomic<u64> wal_buffer_miss = 0;

  // -------------------------------------------------------------------------------------
  WorkerCounters() {
  }

  static std::atomic<u64> sNumWorkers;

  static utils::EnumerableThreadLocal<WorkerCounters> sCounters;

  static WorkerCounters& My() {
    return *sCounters.Local();
  }
};

} // namespace leanstore
