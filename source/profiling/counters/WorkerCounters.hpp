#pragma once

#include "shared-headers/Units.hpp"
#include "utils/EnumerableThreadLocal.hpp"

#include <atomic>

namespace leanstore {

struct WorkerCounters {
  static constexpr uint64_t max_researchy_counter = 10;

  // ATTENTION: buffer overflow if more than max_dt_id in system are registered
  static constexpr uint64_t max_dt_id = 1000;

  std::atomic<uint64_t> t_id = 9999;               // used by tpcc
  std::atomic<uint64_t> variable_for_workload = 0; // Used by tpcc

  std::atomic<uint64_t> mWorkerId = -1;

  std::atomic<uint64_t> hot_hit_counter = 0; // TODO: give it a try ?
  std::atomic<uint64_t> cold_hit_counter = 0;
  std::atomic<uint64_t> read_operations_counter = 0;
  std::atomic<uint64_t> allocate_operations_counter = 0;
  std::atomic<uint64_t> mNumContentions = 0;
  std::atomic<uint64_t> tx = 0;
  std::atomic<uint64_t> long_running_tx = 0;
  std::atomic<uint64_t> olap_scanned_tuples = 0;
  std::atomic<uint64_t> tx_abort = 0;
  std::atomic<uint64_t> olap_tx_abort = 0;
  std::atomic<uint64_t> tmp = 0;

  // Space and contention management
  std::atomic<uint64_t> contention_split_succ_counter[max_dt_id] = {0};
  std::atomic<uint64_t> contention_split_fail_counter[max_dt_id] = {0};
  std::atomic<uint64_t> dt_split[max_dt_id] = {0};
  std::atomic<uint64_t> dt_merge_succ[max_dt_id] = {0};
  std::atomic<uint64_t> dt_merge_parent_succ[max_dt_id] = {0};
  std::atomic<uint64_t> dt_merge_fail[max_dt_id] = {0};
  std::atomic<uint64_t> dt_merge_parent_fail[max_dt_id] = {0};
  std::atomic<uint64_t> xmerge_partial_counter[max_dt_id] = {0};
  std::atomic<uint64_t> xmerge_full_counter[max_dt_id] = {0};

  std::atomic<uint64_t> dt_page_reads[max_dt_id] = {0};
  std::atomic<uint64_t> dt_page_writes[max_dt_id] = {0};

  // without structural change
  std::atomic<uint64_t> dt_restarts_update_same_size[max_dt_id] = {0};

  // includes insert, remove, update with different size
  std::atomic<uint64_t> dt_restarts_structural_change[max_dt_id] = {0};
  std::atomic<uint64_t> dt_restarts_read[max_dt_id] = {0};

  // temporary counter used to track some value for an idea in My mind
  std::atomic<uint64_t> dt_researchy[max_dt_id][max_researchy_counter] = {};

  std::atomic<uint64_t> dt_find_parent[max_dt_id] = {0};
  std::atomic<uint64_t> dt_find_parent_root[max_dt_id] = {0};
  std::atomic<uint64_t> dt_find_parent_fast[max_dt_id] = {0};
  std::atomic<uint64_t> dt_find_parent_slow[max_dt_id] = {0};

  std::atomic<uint64_t> dt_empty_leaf[max_dt_id] = {0};
  std::atomic<uint64_t> dt_goto_page_exec[max_dt_id] = {0};
  std::atomic<uint64_t> dt_goto_page_shared[max_dt_id] = {0};
  std::atomic<uint64_t> dt_next_tuple[max_dt_id] = {0};
  std::atomic<uint64_t> dt_next_tuple_opt[max_dt_id] = {0};
  std::atomic<uint64_t> dt_prev_tuple[max_dt_id] = {0};
  std::atomic<uint64_t> dt_prev_tuple_opt[max_dt_id] = {0};
  std::atomic<uint64_t> dt_inner_page[max_dt_id] = {0};
  std::atomic<uint64_t> dt_scan_asc[max_dt_id] = {0};
  std::atomic<uint64_t> dt_scan_desc[max_dt_id] = {0};
  std::atomic<uint64_t> dt_scan_callback[max_dt_id] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<uint64_t> dt_range_removed[max_dt_id] = {0};
  std::atomic<uint64_t> dt_append[max_dt_id] = {0};
  std::atomic<uint64_t> dt_append_opt[max_dt_id] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<uint64_t> cc_read_versions_visited[max_dt_id] = {0};
  std::atomic<uint64_t> cc_read_versions_visited_not_found[max_dt_id] = {0};
  std::atomic<uint64_t> cc_read_chains_not_found[max_dt_id] = {0};
  std::atomic<uint64_t> cc_read_chains[max_dt_id] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<uint64_t> cc_update_versions_visited[max_dt_id] = {0};
  std::atomic<uint64_t> cc_update_versions_removed[max_dt_id] = {0};
  std::atomic<uint64_t> cc_update_versions_kept[max_dt_id] = {0};
  std::atomic<uint64_t> cc_update_versions_kept_max[max_dt_id] = {0};
  std::atomic<uint64_t> cc_update_versions_skipped[max_dt_id] = {0};
  std::atomic<uint64_t> cc_update_versions_recycled[max_dt_id] = {0};
  std::atomic<uint64_t> cc_update_versions_created[max_dt_id] = {0};
  std::atomic<uint64_t> cc_update_chains[max_dt_id] = {0};
  std::atomic<uint64_t> cc_update_chains_hwm[max_dt_id] = {0};
  std::atomic<uint64_t> cc_update_chains_pgc[max_dt_id] = {0};
  std::atomic<uint64_t> cc_update_chains_pgc_skipped[max_dt_id] = {0};
  std::atomic<uint64_t> cc_update_chains_pgc_workers_visited[max_dt_id] = {0};
  std::atomic<uint64_t> cc_update_chains_pgc_heavy[max_dt_id] = {0};
  std::atomic<uint64_t> cc_update_chains_pgc_heavy_removed[max_dt_id] = {0};
  std::atomic<uint64_t> cc_update_chains_pgc_light[max_dt_id] = {0};
  std::atomic<uint64_t> cc_update_chains_pgc_light_removed[max_dt_id] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<uint64_t> cc_versions_space_inserted[max_dt_id] = {0};
  std::atomic<uint64_t> cc_versions_space_inserted_opt[max_dt_id] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<uint64_t> cc_todo_removed[max_dt_id] = {0};
  std::atomic<uint64_t> cc_todo_moved_gy[max_dt_id] = {0};
  std::atomic<uint64_t> cc_todo_oltp_executed[max_dt_id] = {0};
  std::atomic<uint64_t> cc_gc_long_tx_executed[max_dt_id] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<uint64_t> cc_fat_tuple_triggered[max_dt_id] = {0};
  std::atomic<uint64_t> cc_fat_tuple_convert[max_dt_id] = {0};
  std::atomic<uint64_t> cc_fat_tuple_decompose[max_dt_id] = {0};

  // WAL
  std::atomic<uint64_t> wal_write_bytes = 0;
  std::atomic<uint64_t> wal_read_bytes = 0;
  std::atomic<uint64_t> wal_buffer_hit = 0;
  std::atomic<uint64_t> wal_buffer_miss = 0;

  // -------------------------------------------------------------------------------------
  WorkerCounters() {
    t_id = sNumWorkers++;
  }

  static std::atomic<uint64_t> sNumWorkers;

  static utils::EnumerableThreadLocal<WorkerCounters> sCounters;

  static WorkerCounters& MyCounters() {
    return *sCounters.Local();
  }
};

} // namespace leanstore
