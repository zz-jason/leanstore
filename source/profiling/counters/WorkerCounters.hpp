#pragma once

#include "shared-headers/Units.hpp"
#include "utils/EnumerableThreadLocal.hpp"

#include <atomic>

namespace leanstore {

struct WorkerCounters {
  static constexpr u64 max_researchy_counter = 10;

  // ATTENTION: buffer overflow if more than max_dt_id in system are registered
  static constexpr u64 max_dt_id = 1000;

  std::atomic<u64> t_id = 9999;               // used by tpcc
  std::atomic<u64> variable_for_workload = 0; // Used by tpcc

  std::atomic<u64> mWorkerId = -1;

  std::atomic<u64> hot_hit_counter = 0; // TODO: give it a try ?
  std::atomic<u64> cold_hit_counter = 0;
  std::atomic<u64> read_operations_counter = 0;
  std::atomic<u64> allocate_operations_counter = 0;
  std::atomic<u64> mNumContentions = 0;
  std::atomic<u64> tx = 0;
  std::atomic<u64> long_running_tx = 0;
  std::atomic<u64> olap_scanned_tuples = 0;
  std::atomic<u64> tx_abort = 0;
  std::atomic<u64> olap_tx_abort = 0;
  std::atomic<u64> tmp = 0;

  // Space and contention management
  std::atomic<u64> contention_split_succ_counter[max_dt_id] = {0};
  std::atomic<u64> contention_split_fail_counter[max_dt_id] = {0};
  std::atomic<u64> dt_split[max_dt_id] = {0};
  std::atomic<u64> dt_merge_succ[max_dt_id] = {0};
  std::atomic<u64> dt_merge_parent_succ[max_dt_id] = {0};
  std::atomic<u64> dt_merge_fail[max_dt_id] = {0};
  std::atomic<u64> dt_merge_parent_fail[max_dt_id] = {0};
  std::atomic<u64> xmerge_partial_counter[max_dt_id] = {0};
  std::atomic<u64> xmerge_full_counter[max_dt_id] = {0};

  std::atomic<u64> dt_page_reads[max_dt_id] = {0};
  std::atomic<u64> dt_page_writes[max_dt_id] = {0};

  // without structural change
  std::atomic<u64> dt_restarts_update_same_size[max_dt_id] = {0};

  // includes insert, remove, update with different size
  std::atomic<u64> dt_restarts_structural_change[max_dt_id] = {0};
  std::atomic<u64> dt_restarts_read[max_dt_id] = {0};

  // temporary counter used to track some value for an idea in my mind
  std::atomic<u64> dt_researchy[max_dt_id][max_researchy_counter] = {};

  std::atomic<u64> dt_find_parent[max_dt_id] = {0};
  std::atomic<u64> dt_find_parent_root[max_dt_id] = {0};
  std::atomic<u64> dt_find_parent_fast[max_dt_id] = {0};
  std::atomic<u64> dt_find_parent_slow[max_dt_id] = {0};

  std::atomic<u64> dt_empty_leaf[max_dt_id] = {0};
  std::atomic<u64> dt_goto_page_exec[max_dt_id] = {0};
  std::atomic<u64> dt_goto_page_shared[max_dt_id] = {0};
  std::atomic<u64> dt_next_tuple[max_dt_id] = {0};
  std::atomic<u64> dt_next_tuple_opt[max_dt_id] = {0};
  std::atomic<u64> dt_prev_tuple[max_dt_id] = {0};
  std::atomic<u64> dt_prev_tuple_opt[max_dt_id] = {0};
  std::atomic<u64> dt_inner_page[max_dt_id] = {0};
  std::atomic<u64> dt_scan_asc[max_dt_id] = {0};
  std::atomic<u64> dt_scan_desc[max_dt_id] = {0};
  std::atomic<u64> dt_scan_callback[max_dt_id] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<u64> dt_range_removed[max_dt_id] = {0};
  std::atomic<u64> dt_append[max_dt_id] = {0};
  std::atomic<u64> dt_append_opt[max_dt_id] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<u64> cc_read_versions_visited[max_dt_id] = {0};
  std::atomic<u64> cc_read_versions_visited_not_found[max_dt_id] = {0};
  std::atomic<u64> cc_read_chains_not_found[max_dt_id] = {0};
  std::atomic<u64> cc_read_chains[max_dt_id] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<u64> cc_update_versions_visited[max_dt_id] = {0};
  std::atomic<u64> cc_update_versions_removed[max_dt_id] = {0};
  std::atomic<u64> cc_update_versions_kept[max_dt_id] = {0};
  std::atomic<u64> cc_update_versions_kept_max[max_dt_id] = {0};
  std::atomic<u64> cc_update_versions_skipped[max_dt_id] = {0};
  std::atomic<u64> cc_update_versions_recycled[max_dt_id] = {0};
  std::atomic<u64> cc_update_versions_created[max_dt_id] = {0};
  std::atomic<u64> cc_update_chains[max_dt_id] = {0};
  std::atomic<u64> cc_update_chains_hwm[max_dt_id] = {0};
  std::atomic<u64> cc_update_chains_pgc[max_dt_id] = {0};
  std::atomic<u64> cc_update_chains_pgc_skipped[max_dt_id] = {0};
  std::atomic<u64> cc_update_chains_pgc_workers_visited[max_dt_id] = {0};
  std::atomic<u64> cc_update_chains_pgc_heavy[max_dt_id] = {0};
  std::atomic<u64> cc_update_chains_pgc_heavy_removed[max_dt_id] = {0};
  std::atomic<u64> cc_update_chains_pgc_light[max_dt_id] = {0};
  std::atomic<u64> cc_update_chains_pgc_light_removed[max_dt_id] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<u64> cc_versions_space_inserted[max_dt_id] = {0};
  std::atomic<u64> cc_versions_space_inserted_opt[max_dt_id] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<u64> cc_todo_removed[max_dt_id] = {0};
  std::atomic<u64> cc_todo_moved_gy[max_dt_id] = {0};
  std::atomic<u64> cc_todo_oltp_executed[max_dt_id] = {0};
  std::atomic<u64> cc_todo_olap_executed[max_dt_id] = {0};
  // -------------------------------------------------------------------------------------
  std::atomic<u64> cc_fat_tuple_triggered[max_dt_id] = {0};
  std::atomic<u64> cc_fat_tuple_convert[max_dt_id] = {0};
  std::atomic<u64> cc_fat_tuple_decompose[max_dt_id] = {0};

  // WAL
  std::atomic<u64> wal_write_bytes = 0;
  std::atomic<u64> wal_read_bytes = 0;
  std::atomic<u64> wal_buffer_hit = 0;
  std::atomic<u64> wal_buffer_miss = 0;

  // -------------------------------------------------------------------------------------
  WorkerCounters() {
    t_id = sNumWorkers++;
  }

  static std::atomic<u64> sNumWorkers;

  static utils::EnumerableThreadLocal<WorkerCounters> sCounters;

  static WorkerCounters& MyCounters() {
    return *sCounters.Local();
  }
};

} // namespace leanstore
