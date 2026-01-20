#include "tools/ycsb/console_logger.hpp"
#include "tools/ycsb/ycsb_bench_stats.hpp"
#include "tools/ycsb/ycsb_options.hpp"
#include "ycsb_backend.hpp"

#include <cassert>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace leanstore::ycsb {

class YcsbExecutor {
public:
  explicit YcsbExecutor(const YcsbOptions& options);

  void Execute() {
    if (options_.IsCmdLoad()) {
      HandleCmdLoad();
    } else if (options_.IsCmdRun()) {
      HandleCmdRun();
    } else {
      ConsoleFatal("Unknown command: " + options_.cmd_);
    }
  }

private:
  void HandleCmdLoad();
  void HandleCmdRun();
  void ReportBenchStats(std::atomic<bool>& keep_running);

  YcsbBenchStats& GetBenchStats(uint64_t i) {
    assert(i < bench_stats_.size());
    return *bench_stats_[i];
  }

  template <typename T>
  void UpdateBenchStats(YcsbBenchStats& stats, const Result<T>& res) {
    if (res) {
      stats.IncCommitted();
    } else {
      stats.IncAborted();
    }
  }

  std::pair<uint64_t, uint64_t> AggregateBenchStats() {
    uint64_t total_committed = 0;
    uint64_t total_aborted = 0;
    for (auto& stats : bench_stats_) {
      stats->MergeInto(total_committed, total_aborted);
    }
    return {total_committed, total_aborted};
  }

  /// Get the key for the given record ID.
  static void KeyAt(uint64_t rid, uint8_t* out, size_t size) {
    auto key_str = std::to_string(rid);
    if (key_str.size() >= size) {
      std::memcpy(out, key_str.data(), size);
      return;
    }
    auto prefix_size = size - key_str.size();
    std::memset(out, 'k', prefix_size);
    std::memcpy(out + prefix_size, key_str.data(), key_str.size());
  }

  /// Generate a random value.
  static void RandValue(uint8_t* out, size_t size) {
    utils::RandomGenerator::RandAlphString(out, size);
  }

  /// Remove directory if exists, equivalent to "rm -rf"
  static void RemoveDirIfExists(std::string_view dir) {
    if (std::filesystem::exists(dir)) {
      std::filesystem::remove_all(dir);
    }
  }

  /// Create directory if not exists, equivalent to "mkdir -p"
  static void MakeDirIfNotExists(std::string_view dir) {
    if (!std::filesystem::exists(dir)) {
      std::filesystem::create_directories(dir);
    }
  }

  /// Name of the KvSpace used in YCSB benchmarks, following the format
  /// requirements of WiredTiger.
  ///
  /// LeanStore has no restriction on KvSpace names, RocksDB allows any names,
  /// but for consistency we use the same name across all backends.
  static constexpr auto kKvSpaceName = "table:ycsb";

  const YcsbOptions options_;
  std::vector<std::unique_ptr<YcsbBenchStats>> bench_stats_;
  std::unique_ptr<YcsbDb> db_;
};

} // namespace leanstore::ycsb