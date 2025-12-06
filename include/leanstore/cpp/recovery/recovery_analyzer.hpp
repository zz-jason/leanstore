#pragma once

#include "leanstore/common/types.h"
#include "leanstore/common/wal_record.h"
#include "leanstore/cpp/base/result.hpp"
#include "leanstore/cpp/recovery/recovery_context.hpp"

#include <unordered_map>

namespace leanstore {

/// The analysis phase of recovery.
///
/// The goal is to reconstruct the dirty page table (DPT) and the transaction
/// table (TT) as they were at the time of the crash.
///
/// - Dirty Page Table (DPT): maps page IDs to page version of the first WAL
///   record that caused that page to become dirty. In multi-file WALs, the
///   location is represented as a pair of (file index, offset).
///
///   PARTITION BY page_id
///   ORDER BY page_id, page_version
///
/// - Transaction Table (TT): maps active transaction IDs to the location of
///   their last created WAL record.
///
class RecoveryAnalyzer {
public:
  explicit RecoveryAnalyzer(const RecoveryContext& recovery_ctx)
      : recovery_ctx_(recovery_ctx),
        max_observed_page_version_(recovery_ctx_.GetLastCheckpointGsn()) {
  }

  ~RecoveryAnalyzer() = default;

  // no copy and assign
  RecoveryAnalyzer& operator=(const RecoveryAnalyzer&) = delete;
  RecoveryAnalyzer(const RecoveryAnalyzer&) = delete;

  /// The entry point for parallel analysis process.
  Result<void> Run();

  /// Get the max observed page version during analysis phase.
  lean_lid_t GetMaxObservedPageVersion() const {
    return max_observed_page_version_;
  }

  /// Get the dirty page table constructed in the analysis phase.
  const std::unordered_map<lean_pid_t, lean_lid_t>& GetDirtyPageTable() const {
    return dirty_page_table_;
  }

  /// Get the active transaction table constructed in the analysis phase.
  const std::unordered_map<lean_txid_t, lean_lid_t>& GetActiveTxTable() const {
    return active_tx_table_;
  }

private:
  bool AnalyzeRecord(lean_wal_record& record);
  void UpdateDPT(lean_pid_t page_id, lean_lid_t page_version);
  void UpdateATT(lean_txid_t tx_id, lean_lid_t lsn);
  void RemoveATT(lean_txid_t tx_id);

  /// Wal meta information read from persistent storage.
  const RecoveryContext& recovery_ctx_;

  /// Max observed page version during analysis phase, used to init global page
  /// version counter after redo phase. So that in the later undo phase, new wal
  /// records can be created with page version larger than all the existing ones
  /// in the wal files, which ensures the correctness of recoverability.
  lean_lid_t max_observed_page_version_;

  /// Maps page IDs to the LSN of the first WAL record that caused them to
  /// become dirty.
  ///
  /// WAL records of the same page may be distributed across multiple WAL files.
  std::unordered_map<lean_pid_t, lean_lid_t> dirty_page_table_;

  /// Maps active transaction IDs to the LSN of their last created WAL record.
  std::unordered_map<lean_txid_t, lean_lid_t> active_tx_table_;
};

} // namespace leanstore