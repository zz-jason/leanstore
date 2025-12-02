#include "leanstore/cpp/wal/parallel_recovery.hpp"

#include "leanstore/common/wal_record.h"
#include "leanstore/cpp/base/optional.hpp"
#include "leanstore/cpp/base/result.hpp"
#include "leanstore/cpp/wal/wal_cast.hpp"
#include "leanstore/cpp/wal/wal_cursor.hpp"
#include "leanstore/utils/log.hpp"

#include <memory>
#include <optional>
#include <vector>

namespace leanstore {

Optional<Error> ParallelRecovery::Run() {
  return std::nullopt;
}

Optional<Error> ParallelRecovery::Analysis() {
  /// Create wal cursors for each wal file
  auto cursors = CreateWalCursors(wal_file_paths_);
  if (!cursors) {
    return std::move(cursors.error());
  }

  // Process each wal file
  auto analyze_record = [&](lean_wal_record& record) { return AnalyzeRecord(record); };
  for (auto& cursor : cursors.value()) {
    auto err = cursor->Foreach(analyze_record);
    if (err) {
      return err;
    }
  }

  return std::nullopt;
}

bool ParallelRecovery::AnalyzeRecord(lean_wal_record& record) {
  LEAN_DLOG("WAL Record Type: {}", static_cast<int>(record.type_));
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

Result<std::vector<std::unique_ptr<WalCursor>>> ParallelRecovery::CreateWalCursors(
    const std::vector<std::string>& wal_file_paths) {
  std::vector<std::unique_ptr<WalCursor>> wal_cursors;
  for (const auto& wal_file_path : wal_file_paths) {
    auto cursor = WalCursor::New(wal_file_path);
    if (cursor) {
      wal_cursors.push_back(std::move(cursor.value()));
    } else {
      return std::move(cursor.error());
    }
  }
  return wal_cursors;
}

} // namespace leanstore