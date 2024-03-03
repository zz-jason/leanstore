#pragma once

#include "shared-headers/Units.hpp"
#include "utils/EnumerableThreadLocal.hpp"

#include <atomic>

namespace leanstore {

struct CRCounters {
  std::atomic<int64_t> mWorkerId = -1;
  std::atomic<uint64_t> written_log_bytes = 0;
  std::atomic<uint64_t> wal_reserve_blocked = 0;
  std::atomic<uint64_t> wal_reserve_immediate = 0;

  std::atomic<uint64_t> gct_total_ms = 0;
  std::atomic<uint64_t> gct_phase_1_ms = 0;
  std::atomic<uint64_t> gct_phase_2_ms = 0;
  std::atomic<uint64_t> gct_write_ms = 0;
  std::atomic<uint64_t> gct_write_bytes = 0;

  std::atomic<uint64_t> gct_rounds = 0;
  std::atomic<uint64_t> gct_committed_tx = 0;
  std::atomic<uint64_t> rfa_committed_tx = 0;

  std::atomic<uint64_t> cc_prepare_igc = 0;
  std::atomic<uint64_t> cc_cross_workers_visibility_check = 0;
  std::atomic<uint64_t> cc_versions_space_removed = {0};
  std::atomic<uint64_t> cc_snapshot_restart = 0;

  // Time
  std::atomic<uint64_t> cc_ms_snapshotting =
      0; // Everything related to commit log
  std::atomic<uint64_t> cc_ms_gc = 0;
  std::atomic<uint64_t> cc_ms_gc_graveyard = 0;
  std::atomic<uint64_t> cc_ms_gc_history_tree = 0;
  std::atomic<uint64_t> cc_ms_gc_cm = 0;
  std::atomic<uint64_t> cc_ms_committing = 0;
  std::atomic<uint64_t> cc_ms_history_tree_insert = 0;
  std::atomic<uint64_t> cc_ms_history_tree_retrieve = 0;
  std::atomic<uint64_t> cc_ms_refresh_global_state = 0;

  std::atomic<uint64_t> cc_ms_oltp_tx = 0;
  std::atomic<uint64_t> cc_ms_olap_tx = 0;
  std::atomic<uint64_t> cc_ms_fat_tuple = 0;
  std::atomic<uint64_t> cc_ms_fat_tuple_conversion = 0;

  std::atomic<uint64_t> cc_ms_start_tx = 0;
  std::atomic<uint64_t> cc_ms_commit_tx = 0;
  std::atomic<uint64_t> cc_ms_abort_tx = 0;

  // Latency
  // ATTENTION: buffer overflow if more than max_dt_id in system are
  // registered
  static constexpr uint64_t latency_tx_capacity = 1024;
  std::atomic<uint64_t> cc_ms_precommit_latency[latency_tx_capacity] = {0};
  std::atomic<uint64_t> cc_ms_commit_latency[latency_tx_capacity] = {0};
  std::atomic<uint64_t> cc_flushes_counter[latency_tx_capacity] = {0};
  std::atomic<uint64_t> cc_latency_cursor = {0};
  std::atomic<uint64_t> cc_rfa_ms_precommit_latency[latency_tx_capacity] = {0};
  std::atomic<uint64_t> cc_rfa_ms_commit_latency[latency_tx_capacity] = {0};
  std::atomic<uint64_t> cc_rfa_latency_cursor = {0};

  CRCounters() {
  }

  static utils::EnumerableThreadLocal<CRCounters> sCounters;

  static CRCounters& MyCounters() {
    return *sCounters.Local();
  }
};

} // namespace leanstore
