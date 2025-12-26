#include "leanstore/cpp/recovery/recovery_redoer.hpp"

#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/core/b_tree_node.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/buffer-manager/guarded_buffer_frame.hpp"
#include "leanstore/common/types.h"
#include "leanstore/common/wal_record.h"
#include "leanstore/cpp/base/byte_buffer.hpp"
#include "leanstore/cpp/base/defer.hpp"
#include "leanstore/cpp/base/error.hpp"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/cpp/base/result.hpp"
#include "leanstore/cpp/config/store_paths.hpp"
#include "leanstore/cpp/io/file_writer.hpp"
#include "leanstore/cpp/wal/wal_cast.hpp"
#include "leanstore/cpp/wal/wal_cursor.hpp"
#include "leanstore/sync/hybrid_guard.hpp"

#include <cstdint>
#include <filesystem>
#include <format>
#include <string>
#include <utility>
#include <vector>

namespace leanstore {

Result<void> RecoveryRedoer::Run() {
  if (dirty_page_table_.empty()) {
    return {};
  }
  Log::Info("Starting REDO phase, store_dir={}, num_dirty_pages={}, num_partitions={}", store_dir_,
            dirty_page_table_.size(), num_partiions_);

  // partition wal logs by page id
  auto partitioned_logs = PartitionWalLogs(store_dir_, wal_file_paths_, num_partiions_);
  if (!partitioned_logs) {
    return std::move(partitioned_logs.error());
  }

  // sort each partitioned log by page version
  auto sorted_logs = SortPartitionedLogs(partitioned_logs.value());
  if (!sorted_logs) {
    return std::move(sorted_logs.error());
  }

  // redo each sorted partitioned log
  for (auto& sorted_log : sorted_logs.value()) {
    if (auto res = RedoOnePartition(sorted_log); !res) {
      return res;
    }

    if (auto res = CheckpointLoadedPages(); !res) {
      return res;
    }
  }

  return {};
}

Result<std::vector<std::string>> RecoveryRedoer::PartitionWalLogs(
    std::string_view store_dir, const std::vector<std::string>& wal_file_paths,
    size_t num_partiions) {
  static constexpr auto kPartitionedLogNameFmt = "partitioned_wal_{}.log";
  const auto tmp_partition_dir = StorePaths::TmpPartitionedWalDir(store_dir);
  Log::Info("Partitioning WAL logs into directory: {}", tmp_partition_dir);
  if (!std::filesystem::exists(tmp_partition_dir)) {
    Log::Info("Creating temporary partitioned WAL directory: {}", tmp_partition_dir);
    std::filesystem::create_directories(tmp_partition_dir);
  }

  // determine partitioned log files
  std::vector<std::string> partitioned_logs;
  partitioned_logs.reserve(num_partiions);
  for (auto i = 0U; i < num_partiions; i++) {
    auto filename = std::format(kPartitionedLogNameFmt, i);
    partitioned_logs.emplace_back(std::format("{}/{}", tmp_partition_dir, filename));
  }

  // open writers for partitioned log files
  std::vector<FileWriter> log_writers;
  LEAN_DEFER({
    for (auto& writer : log_writers) {
      auto bytes_written = writer.BytesWritten();
      Log::Info("Closing partitioned log writer, bytes_written={}", bytes_written);
      auto res = writer.Close();
      if (!res) {
        Log::Error("Failed to close partitioned log writer: {}", res.error().ToString());
      }
    }
  });

  log_writers.reserve(partitioned_logs.size());
  for (const auto& partitioned_log : partitioned_logs) {
    auto log_writer = FileWriter::New(partitioned_log);
    if (!log_writer) {
      return std::move(log_writer.error());
    }
    Log::Info("Created partitioned log file: {}", partitioned_log);
    log_writers.emplace_back(std::move(log_writer.value()));
  }

  // partition each wal log file
  for (const auto& wal_log : wal_file_paths) {
    if (auto res = PartitionOneLogFile(wal_log, log_writers); !res) {
      return std::move(res.error());
    }
  }

  return partitioned_logs;
}

Result<void> RecoveryRedoer::PartitionOneLogFile(std::string_view wal_log,
                                                 std::vector<FileWriter>& log_writers) {
  Log::Info("Partitioning WAL log file: {}", wal_log);
  auto cursor = WalCursor::New(wal_log);
  if (!cursor) {
    return std::move(cursor.error());
  }

  return cursor.value()->Foreach([&](lean_wal_record& record) -> Result<bool> {
    auto partition_res = PartitionOneRecord(record, log_writers);
    if (!partition_res) {
      return std::move(partition_res.error());
    }
    return true;
  });
}

namespace {

/// Helper to get the byte slice of a WAL record.
Slice GetRecordBytes(const lean_wal_record& record) {
  return Slice(reinterpret_cast<const uint8_t*>(&record), record.size_);
};

/// Template to partition WAL record of type T.
template <typename T>
Result<void> HandleWalRecordType(const lean_wal_record& record,
                                 std::vector<FileWriter>& log_writers) {
  auto& actual_record = CastTo<T>(record);
  auto partition_id = static_cast<size_t>(actual_record.page_id_) % log_writers.size();
  return log_writers[partition_id].Append(GetRecordBytes(record));
}

/// Helper to get the page version from a WAL record.
Result<lean_lid_t> GetPageVersion(const lean_wal_record& record) {
  switch (record.type_) {
  case LEAN_WAL_TYPE_SMO_PAGENEW: {
    return CastTo<lean_wal_smo_pagenew>(record).page_version_;
  }
  case LEAN_WAL_TYPE_SMO_PAGESPLIT_ROOT: {
    return CastTo<lean_wal_smo_pagesplit_root>(record).page_version_;
  }
  case LEAN_WAL_TYPE_SMO_PAGESPLIT_NONROOT: {
    return CastTo<lean_wal_smo_pagesplit_nonroot>(record).page_version_;
  }
  case LEAN_WAL_TYPE_INSERT: {
    return CastTo<lean_wal_insert>(record).page_version_;
  }
  case LEAN_WAL_TYPE_UPDATE: {
    return CastTo<lean_wal_update>(record).page_version_;
  }
  case LEAN_WAL_TYPE_REMOVE: {
    return CastTo<lean_wal_remove>(record).page_version_;
  }
  case LEAN_WAL_TYPE_TX_INSERT: {
    return CastTo<lean_wal_tx_insert>(record).page_version_;
  }
  case LEAN_WAL_TYPE_TX_REMOVE: {
    return CastTo<lean_wal_tx_remove>(record).page_version_;
  }
  case LEAN_WAL_TYPE_TX_UPDATE: {
    return CastTo<lean_wal_tx_update>(record).page_version_;
  }
  default: {
    return Error::General(std::format("Unsupported WAL record type for page version extraction: {}",
                                      std::to_string(record.type_)));
  }
  }
}

} // namespace

Result<void> RecoveryRedoer::PartitionOneRecord(const lean_wal_record& record,
                                                std::vector<FileWriter>& log_writers) {
  switch (record.type_) {
  case LEAN_WAL_TYPE_CARRIAGE_RETURN: {
    return {};
  }
  case LEAN_WAL_TYPE_SMO_COMPLETE: {
    // No need to partition
    return {};
  }
  case LEAN_WAL_TYPE_SMO_PAGENEW: {
    return HandleWalRecordType<lean_wal_smo_pagenew>(record, log_writers);
  }
  case LEAN_WAL_TYPE_SMO_PAGESPLIT_ROOT: {
    return HandleWalRecordType<lean_wal_smo_pagesplit_root>(record, log_writers);
  }
  case LEAN_WAL_TYPE_SMO_PAGESPLIT_NONROOT: {
    return HandleWalRecordType<lean_wal_smo_pagesplit_nonroot>(record, log_writers);
  }
  case LEAN_WAL_TYPE_INSERT: {
    return HandleWalRecordType<lean_wal_insert>(record, log_writers);
  }
  case LEAN_WAL_TYPE_UPDATE: {
    return HandleWalRecordType<lean_wal_update>(record, log_writers);
  }
  case LEAN_WAL_TYPE_REMOVE: {
    return HandleWalRecordType<lean_wal_remove>(record, log_writers);
  }
  case LEAN_WAL_TYPE_TX_ABORT: {
    // No need to partition
    return {};
  }
  case LEAN_WAL_TYPE_TX_COMPLETE: {
    // No need to partition
    return {};
  }
  case LEAN_WAL_TYPE_TX_INSERT: {
    return HandleWalRecordType<lean_wal_tx_insert>(record, log_writers);
  }
  case LEAN_WAL_TYPE_TX_REMOVE: {
    return HandleWalRecordType<lean_wal_tx_remove>(record, log_writers);
  }
  case LEAN_WAL_TYPE_TX_UPDATE: {
    return HandleWalRecordType<lean_wal_tx_update>(record, log_writers);
  }
  default: {
    return Error::General(std::format("Unknown WAL record type: {}", record.type_));
  }
  }
}

Result<std::vector<std::string>> RecoveryRedoer::SortPartitionedLogs(
    const std::vector<std::string>& partitioned_logs) {
  std::vector<std::string> sorted_logs;
  sorted_logs.reserve(partitioned_logs.size());
  for (const auto& partitioned_log : partitioned_logs) {
    auto res = SortOneLogFile(partitioned_log);
    if (!res) {
      return std::move(res.error());
    }
    sorted_logs.emplace_back(res.value());
  }
  return sorted_logs;
}

Result<std::string> RecoveryRedoer::SortOneLogFile(std::string_view partitioned_log) {
  auto cursor = WalCursor::New(partitioned_log);
  if (!cursor) {
    return std::move(cursor.error());
  }

  // collect all records with their page versions
  std::vector<std::pair<ByteBuffer, lean_lid_t>> records;
  auto res = cursor.value()->Foreach([&](lean_wal_record& record) -> Result<bool> {
    auto page_version = GetPageVersion(record);
    if (!page_version) {
      return std::move(page_version.error());
    }
    records.emplace_back(ByteBuffer(reinterpret_cast<const std::byte*>(&record), record.size_),
                         page_version.value());
    return true;
  });
  if (!res) {
    return std::move(res.error());
  }

  // sort by page version
  std::sort(records.begin(), records.end(),
            [](const auto& a, const auto& b) { return a.second < b.second; });

  // determine sorted log file path
  auto partitioned_log_path = std::filesystem::path(partitioned_log);
  auto partitioned_log_dir = partitioned_log_path.parent_path();
  auto partitioned_log_filename = partitioned_log_path.filename();
  static constexpr auto kSortedLogNameFmt = "{}.sorted";
  auto sorted_log_name = std::format(kSortedLogNameFmt, partitioned_log_filename.string());
  auto sorted_log = std::format("{}/{}", partitioned_log_dir.string(), sorted_log_name);

  // write sorted records to the sorted log file
  auto log_writer = FileWriter::New(sorted_log);
  if (!log_writer) {
    return std::move(log_writer.error());
  }
  LEAN_DEFER({
    auto bytes_written = log_writer.value().BytesWritten();
    Log::Info("Closing sorted partitioned log writer, bytes_written={}", bytes_written);
    auto res = log_writer.value().Close();
    if (!res) {
      Log::Error("Failed to close sorted partitioned log writer: {}", res.error().ToString());
    }
  });

  for (const auto& [record, _] : records) {
    auto append_res = log_writer.value().Append(record.slice());
    if (!append_res) {
      return std::move(append_res.error());
    }
  }

  return sorted_log;
}

Result<void> RecoveryRedoer::RedoOnePartition(std::string_view partitioned_log) {
  auto cursor = WalCursor::New(partitioned_log);
  if (!cursor) {
    return std::move(cursor.error());
  }

  return cursor.value()->Foreach([&](lean_wal_record& record) -> Result<bool> {
    auto res = RedoOneRecord(record);
    if (!res) {
      return std::move(res.error());
    }
    return true;
  });
}

Result<void> RecoveryRedoer::RedoOneRecord(const lean_wal_record& record) {
  switch (record.type_) {
  case LEAN_WAL_TYPE_CARRIAGE_RETURN: {
    return {};
  }
  case LEAN_WAL_TYPE_SMO_COMPLETE: {
    return Error::NotImplemented("Not Implemented");
  }
  case LEAN_WAL_TYPE_SMO_PAGENEW: {
    return Error::NotImplemented("Not Implemented");
  }
  case LEAN_WAL_TYPE_SMO_PAGESPLIT_ROOT: {
    return Error::NotImplemented("Not Implemented");
  }
  case LEAN_WAL_TYPE_SMO_PAGESPLIT_NONROOT: {
    return Error::NotImplemented("Not Implemented");
  }
  case LEAN_WAL_TYPE_INSERT: {
    // auto& actual_record = CastTo<lean_wal_insert>(record);
    // auto& bf = GetOrLoadPage(actual_record.page_id_);
    // HybridGuard guard(&bf.header_.latch_);
    // GuardedBufferFrame<BTreeNode> guarded_node(&buffer_manager_, std::move(guard), &bf);

    // int32_t slot_id = -1;
    // TransactionKV::InsertToNode(guarded_node,
    //   // key, Slice val, lean_wid_t worker_id, lean_txid_t tx_start_ts, int32_t &slot_id)

    return Error::NotImplemented("Not Implemented");
  }
  case LEAN_WAL_TYPE_UPDATE: {
    return Error::NotImplemented("Not Implemented");
  }
  case LEAN_WAL_TYPE_REMOVE: {
    return Error::NotImplemented("Not Implemented");
  }
  case LEAN_WAL_TYPE_TX_ABORT: {
    return Error::NotImplemented("Not Implemented");
  }
  case LEAN_WAL_TYPE_TX_COMPLETE: {
    return Error::NotImplemented("Not Implemented");
  }
  case LEAN_WAL_TYPE_TX_INSERT: {
    return Error::NotImplemented("Not Implemented");
  }
  case LEAN_WAL_TYPE_TX_REMOVE: {
    return Error::NotImplemented("Not Implemented");
  }
  case LEAN_WAL_TYPE_TX_UPDATE: {
    return Error::NotImplemented("Not Implemented");
  }
  default: {
    return Error::NotImplemented("Not Implemented");
  }
  }
}

BufferFrame& RecoveryRedoer::GetOrLoadPage(lean_pid_t page_id) {
  auto it = loaded_pages_.find(page_id);
  if (it != loaded_pages_.end()) {
    return *it->second;
  }

  auto& bf = buffer_manager_.ReadPageSync(page_id);
  bf.header_.keep_in_memory_ = true;
  loaded_pages_.emplace(page_id, &bf);
  return bf;
}

Result<void> RecoveryRedoer::CheckpointLoadedPages() {
  for (auto& [_, bf] : loaded_pages_) {
    auto res = buffer_manager_.WritePageSync(*bf);
    if (!res) {
      return res;
    }
  }
  loaded_pages_.clear();
  return {};
}

} // namespace leanstore