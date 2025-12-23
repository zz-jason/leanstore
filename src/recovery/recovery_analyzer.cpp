#include "leanstore/cpp/recovery/recovery_analyzer.hpp"

#include "leanstore/common/wal_record.h"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/cpp/base/result.hpp"
#include "leanstore/cpp/wal/wal_cast.hpp"
#include "leanstore/cpp/wal/wal_cursor.hpp"

#include <memory>
#include <utility>

namespace leanstore {

Result<void> RecoveryAnalyzer::Run() {
  auto cursors = WalCursor::NewWalCursors(recovery_ctx_.GetWalFilePaths());
  if (!cursors) {
    return std::move(cursors.error());
  }

  auto analyze_record = [&](lean_wal_record& record) -> Result<bool> {
    return AnalyzeRecord(record);
  };
  for (auto& cursor : cursors.value()) {
    auto res = cursor->Foreach(analyze_record);
    if (!res) {
      return res;
    }
  }

  return {};
}

bool RecoveryAnalyzer::AnalyzeRecord(lean_wal_record& record) {
  switch (record.type_) {
  case LEAN_WAL_TYPE_CARRIAGE_RETURN: {
    break; // Do nothing
  }
  case LEAN_WAL_TYPE_SMO_COMPLETE: {
    break; // Do nothing
  }
  case LEAN_WAL_TYPE_SMO_PAGENEW: {
    auto& actual_record = CastTo<lean_wal_smo_pagenew>(record);
    UpdateDPT(actual_record.page_id_, actual_record.page_version_);
    break;
  }
  case LEAN_WAL_TYPE_SMO_PAGESPLIT_ROOT: {
    auto& actual_record = CastTo<lean_wal_smo_pagesplit_root>(record);
    UpdateDPT(actual_record.page_id_, actual_record.page_version_);
    break;
  }
  case LEAN_WAL_TYPE_SMO_PAGESPLIT_NONROOT: {
    auto& actual_record = CastTo<lean_wal_smo_pagesplit_nonroot>(record);
    UpdateDPT(actual_record.page_id_, actual_record.page_version_);
    break;
  }
  case LEAN_WAL_TYPE_INSERT: {
    auto& actual_record = CastTo<lean_wal_insert>(record);
    UpdateDPT(actual_record.page_id_, actual_record.page_version_);
    break;
  }
  case LEAN_WAL_TYPE_UPDATE: {
    auto& actual_record = CastTo<lean_wal_update>(record);
    UpdateDPT(actual_record.page_id_, actual_record.page_version_);
    break;
  }
  case LEAN_WAL_TYPE_REMOVE: {
    auto& actual_record = CastTo<lean_wal_remove>(record);
    UpdateDPT(actual_record.page_id_, actual_record.page_version_);
    break;
  }
  case LEAN_WAL_TYPE_TX_ABORT: {
    auto& actual_record = CastTo<lean_wal_tx_abort>(record);
    UpdateATT(actual_record.tx_base_.txid_, actual_record.tx_base_.base_.lsn_);
    break;
  }
  case LEAN_WAL_TYPE_TX_COMPLETE: {
    auto& actual_record = CastTo<lean_wal_tx_complete>(record);
    RemoveATT(actual_record.tx_base_.txid_);
    break;
  }
  case LEAN_WAL_TYPE_TX_INSERT: {
    auto& actual_record = CastTo<lean_wal_tx_insert>(record);
    UpdateDPT(actual_record.page_id_, actual_record.page_version_);
    UpdateATT(actual_record.tx_base_.txid_, actual_record.tx_base_.base_.lsn_);
    break;
  }
  case LEAN_WAL_TYPE_TX_REMOVE: {
    auto& actual_record = CastTo<lean_wal_tx_remove>(record);
    UpdateDPT(actual_record.page_id_, actual_record.page_version_);
    UpdateATT(actual_record.tx_base_.txid_, actual_record.tx_base_.base_.lsn_);
    break;
  }
  case LEAN_WAL_TYPE_TX_UPDATE: {
    auto& actual_record = CastTo<lean_wal_tx_update>(record);
    UpdateDPT(actual_record.page_id_, actual_record.page_version_);
    UpdateATT(actual_record.tx_base_.txid_, actual_record.tx_base_.base_.lsn_);
    break;
  }
  default: {
    LEAN_DCHECK(false, "Unrecognized WAL record type: {}", static_cast<int>(record.type_));
    return false; // Unrecognized record type, stop processing
  }
  }

  return true;
}
void RecoveryAnalyzer::UpdateDPT(lean_pid_t page_id, lean_lid_t page_version) {
  // Skip wal records before or at the last checkpoint.
  // TODO: reduce the overhead, avoid reading every record.
  if (page_version <= recovery_ctx_.GetLastCheckpointGsn()) {
    return;
  }

  max_observed_page_version_ = std::max(max_observed_page_version_, page_version);

  auto it = dirty_page_table_.find(page_id);
  if (it == dirty_page_table_.end() || page_version < it->second) {
    dirty_page_table_[page_id] = page_version;
  }
}

void RecoveryAnalyzer::UpdateATT(lean_txid_t tx_id, lean_lid_t lsn) {
  auto it = active_tx_table_.find(tx_id);
  if (it == active_tx_table_.end()) {
    active_tx_table_.emplace(tx_id, lsn);
  } else {
    it->second = lsn;
  }
}

void RecoveryAnalyzer::RemoveATT(lean_txid_t tx_id) {
  auto it = active_tx_table_.find(tx_id);
  LEAN_DCHECK(it != active_tx_table_.end());
  if (it != active_tx_table_.end()) {
    active_tx_table_.erase(it);
  }
}

} // namespace leanstore