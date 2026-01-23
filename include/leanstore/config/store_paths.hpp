#pragma once

#include <format>
#include <string>
#include <string_view>

namespace leanstore {

class StorePaths {
public:
  static std::string WalDir(std::string_view store_dir) {
    return std::format("{}/{}", store_dir, kWalDirName);
  }

  static std::string WalMetaFilePath(std::string_view store_dir) {
    return std::format("{}/{}", WalDir(store_dir), kWalMetaFileName);
  }

  static std::string LogFilePath(std::string_view store_dir) {
    return std::format("{}/{}", store_dir, kStoreLogFileName);
  }

  static std::string MetaFilePath(std::string_view store_dir) {
    return std::format("{}/{}", store_dir, kStoreMetaFileName);
  }

  static std::string PagesFilePath(std::string_view store_dir) {
    return std::format("{}/{}", store_dir, kStorePagesFileName);
  }

  static std::string WalFilePath(std::string_view store_dir) {
    return std::format("{}/{}", store_dir, kStoreWalFileName);
  }

  static std::string CheckpointDir(std::string_view store_dir) {
    return std::format("{}/{}", store_dir, kCheckpointDirName);
  }

  static std::string CheckpointMeta1FilePath(std::string_view store_dir) {
    return std::format("{}/{}", CheckpointDir(store_dir), kCheckpointMeta1FileName);
  }

  static std::string CheckpointMeta2FilePath(std::string_view store_dir) {
    return std::format("{}/{}", CheckpointDir(store_dir), kCheckpointMeta2FileName);
  }

  static std::string TmpDir(std::string_view store_dir) {
    return std::format("{}/{}", store_dir, kTmpDirName);
  }

  static std::string TmpPartitionedWalDir(std::string_view store_dir) {
    return std::format("{}/{}", TmpDir(store_dir), kTmpPartitionedWalDir);
  }

private:
  static constexpr auto kWalDirName = "wals";
  static constexpr auto kWalMetaFileName = "wals_meta.json";
  static constexpr auto kStoreWalFileName = "store.wal"; // for legacy thread model

  static constexpr auto kStoreLogFileName = "leanstore.log";
  static constexpr auto kStoreMetaFileName = "store_meta.json";
  static constexpr auto kStorePagesFileName = "store.pages";

  static constexpr auto kCheckpointDirName = "checkpoints";
  static constexpr auto kCheckpointMeta1FileName = "checkpoint.meta1.json";
  static constexpr auto kCheckpointMeta2FileName = "checkpoint.meta2.json";

  static constexpr auto kTmpDirName = "tmp";
  static constexpr auto kTmpPartitionedWalDir = "partitioned_wals";
};

} // namespace leanstore