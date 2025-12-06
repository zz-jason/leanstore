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

private:
  static constexpr auto kWalDirName = "wals";
  static constexpr auto kStoreLogFileName = "leanstore.log";
  static constexpr auto kStoreMetaFileName = "store_meta.json";
  static constexpr auto kStorePagesFileName = "store.pages";
  static constexpr auto kStoreWalFileName = "store.wal";
  static constexpr auto kWalMetaFileName = "wals_meta.json";
};

} // namespace leanstore