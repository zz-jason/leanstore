#pragma once

#include "leanstore/common/types.h"

#include <string>
#include <string_view>
#include <vector>

namespace leanstore {

class RecoveryContext {
public:
  static RecoveryContext LoadFromFile(std::string_view wal_meta_file_path);

  RecoveryContext(lean_lid_t last_checkpoint_gsn, std::vector<std::string>&& wal_file_paths)
      : last_checkpoint_gsn_(last_checkpoint_gsn),
        wal_file_paths_(std::move(wal_file_paths)) {
  }

  ~RecoveryContext() = default;

  void SaveToFile(std::string_view wal_meta_file_path) const;

  lean_lid_t GetLastCheckpointGsn() const {
    return last_checkpoint_gsn_;
  }

  const std::vector<std::string>& GetWalFilePaths() const {
    return wal_file_paths_;
  }

private:
  /// The GSN (global sequence number) of the last checkpoint.
  lean_lid_t last_checkpoint_gsn_;

  /// Paths to the WAL files.
  const std::vector<std::string> wal_file_paths_;
};

} // namespace leanstore