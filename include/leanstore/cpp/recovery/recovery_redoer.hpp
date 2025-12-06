#pragma once

#include "leanstore/common/types.h"
#include "leanstore/common/wal_record.h"
#include "leanstore/cpp/base/result.hpp"

#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace leanstore {

class FileWriter;

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
class RecoveryRedoer {
public:
  RecoveryRedoer(std::string_view store_dir, const std::vector<std::string>& wal_file_paths,
                 const std::unordered_map<lean_pid_t, lean_lid_t>& dirty_page_table,
                 size_t num_partiions)
      : store_dir_(store_dir),
        wal_file_paths_(wal_file_paths),
        dirty_page_table_(dirty_page_table),
        num_partiions_(num_partiions) {
  }

  ~RecoveryRedoer() = default;

  // no copy and assign
  RecoveryRedoer& operator=(const RecoveryRedoer&) = delete;
  RecoveryRedoer(const RecoveryRedoer&) = delete;

  /// The entry point for redo process.
  Result<void> Run();

private:
  // Helpers for partitioning WAL logs
  static Result<std::vector<std::string>> PartitionWalLogs(
      std::string_view store_dir, const std::vector<std::string>& wal_file_paths,
      size_t num_partiions);
  static Result<void> PartitionOneLogFile(std::string_view wal_log,
                                          std::vector<FileWriter>& log_writers);
  static Result<void> PartitionOneRecord(const lean_wal_record& record,
                                         std::vector<FileWriter>& log_writers);

  // Helpers for sorting partitioned WAL logs
  static Result<std::vector<std::string>> SortPartitionedLogs(
      const std::vector<std::string>& partitioned_logs);
  static Result<std::string> SortOneLogFile(std::string_view partitioned_log);

  // Helpers for redoing sorted WAL logs
  static Result<void> RedoOnePartition(std::string_view partitioned_log);
  static Result<void> RedoOneRecord(const lean_wal_record& record);

  // Inputs
  const std::string store_dir_;
  const std::vector<std::string>& wal_file_paths_;
  const std::unordered_map<lean_pid_t, lean_lid_t>& dirty_page_table_;
  const size_t num_partiions_;
};

} // namespace leanstore