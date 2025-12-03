#pragma once

#include "leanstore/common/types.h"
#include "leanstore/cpp/base/error.hpp"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/cpp/base/optional.hpp"
#include "leanstore/cpp/base/result.hpp"
#include "leanstore/cpp/wal/wal_cursor.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace leanstore {

class ParallelRecovery {
public:
  ParallelRecovery(std::vector<std::string>&& wal_files) : wal_file_paths_(std::move(wal_files)) {
  }

  ~ParallelRecovery() = default;

  // no copy and assign
  ParallelRecovery& operator=(const ParallelRecovery&) = delete;
  ParallelRecovery(const ParallelRecovery&) = delete;

  Optional<Error> Run();

  /// The goal of the analysis phase is to reconstruct the dirty page table
  /// (DPT) and the transaction table (TT) as they were at the time of the
  /// crash.
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
  Optional<Error> Analysis();

  /// WAL records are distributed across multiple files, the parallel
  /// partitioned redo process works as follows:
  ///
  /// - Redistribute WAL records by page ID, so that all WAL records of the same
  ///   page are processed by the same worker thread. Note: only the WAL records
  ///   of dirty pages are redistributed, other WAL records are ignored.
  ///
  /// - Start N worker threads to read the redistributed WAL records and redo
  ///   them to the corresponding pages.
  ///
  Optional<Error> Redo();

  Optional<Error> Undo();

  /// Get the dirty page table constructed in the analysis phase.
  const std::unordered_map<lean_pid_t, lean_lid_t>& GetDPT() const {
    return dirty_page_table_;
  }

  /// Get the active transaction table constructed in the analysis phase.
  const std::unordered_map<lean_txid_t, lean_lid_t>& GetATT() const {
    return active_tx_table_;
  }

private:
  bool AnalyzeRecord(lean_wal_record& record);

  void UpdateDPT(lean_pid_t page_id, lean_lid_t page_version) {
    auto it = dirty_page_table_.find(page_id);
    if (it == dirty_page_table_.end() || page_version < it->second) {
      dirty_page_table_[page_id] = page_version;
    }
  }

  void UpdateATT(lean_txid_t tx_id, lean_lid_t lsn) {
    auto it = active_tx_table_.find(tx_id);
    if (it == active_tx_table_.end()) {
      active_tx_table_.emplace(tx_id, lsn);
    } else {
      it->second = lsn;
    }
  }

  void RemoveATT(lean_txid_t tx_id) {
    auto it = active_tx_table_.find(tx_id);
    LEAN_DCHECK(it != active_tx_table_.end());
    if (it != active_tx_table_.end()) {
      active_tx_table_.erase(it);
    }
  }

  static Result<std::vector<std::unique_ptr<WalCursor>>> CreateWalCursors(
      const std::vector<std::string>& wal_file_paths);

  /// Paths to the WAL files to be processed in parallel.
  const std::vector<std::string> wal_file_paths_;

  /// Maps page IDs to the LSN of the first WAL record that caused them to
  /// become dirty.
  ///
  /// WAL records of the same page may be distributed across multiple WAL
  /// files.
  std::unordered_map<lean_pid_t, lean_lid_t> dirty_page_table_;

  /// Maps active transaction IDs to the LSN of their last created WAL record.
  std::unordered_map<lean_txid_t, lean_lid_t> active_tx_table_;
};

} // namespace leanstore