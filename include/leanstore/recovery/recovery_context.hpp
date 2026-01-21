#pragma once

#include "leanstore/buffer/buffer_manager.hpp"
#include "leanstore/c/types.h"

#include <string>
#include <string_view>
#include <vector>

namespace leanstore {

class RecoveryContext {
public:
  static RecoveryContext LoadFromFile(std::string_view wal_meta_file_path);

  RecoveryContext(std::string_view store_dir, std::vector<std::string> wal_file_paths,
                  lean_lid_t last_checkpoint_gsn, size_t num_partitions)
      : store_dir_(store_dir),
        wal_file_paths_(std::move(wal_file_paths)),
        last_checkpoint_gsn_(last_checkpoint_gsn),
        num_partitions_(num_partitions) {
  }

  ~RecoveryContext() = default;

  void SaveToFile(std::string_view wal_meta_file_path) const;

  std::string_view GetStoreDir() const {
    return store_dir_;
  }

  const std::vector<std::string>& GetWalFilePaths() const {
    return wal_file_paths_;
  }

  lean_lid_t GetLastCheckpointGsn() const {
    return last_checkpoint_gsn_;
  }

  BufferManager& GetBufferManager() const {
    return *buffer_manager_;
  }

  size_t GetNumPartitions() const {
    return num_partitions_;
  }

private:
  const std::string store_dir_;
  const std::vector<std::string> wal_file_paths_;
  const lean_lid_t last_checkpoint_gsn_;
  BufferManager* buffer_manager_ = nullptr;
  const size_t num_partitions_;
};

} // namespace leanstore