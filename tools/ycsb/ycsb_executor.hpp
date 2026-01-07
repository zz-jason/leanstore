#include "tools/ycsb/general_server.hpp"
#include "tools/ycsb/terminal_log.hpp"
#include "tools/ycsb/ycsb_bench_stats.hpp"
#include "tools/ycsb/ycsb_options.hpp"

#include <cassert>
#include <cstdint>
#include <utility>
#include <vector>

namespace leanstore::ycsb {

class YcsbExecutor {
public:
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

  static void RandValue(uint8_t* out, size_t size) {
    utils::RandomGenerator::RandAlphString(out, size);
  }

  explicit YcsbExecutor(const YcsbOptions& options) : options_(options) {
    // Init bench stats
    auto num_bench_stats = options.threads_;
    if (options.IsCmdRun() && options_.clients_ > 0) {
      num_bench_stats = options_.clients_;
    }
    bench_stats_.reserve(num_bench_stats);
    for (auto i = 0U; i < num_bench_stats; i++) {
      bench_stats_.emplace_back(std::make_unique<YcsbBenchStats>());
    }

    // Init backend server
    auto data_dir = options_.DataDir();
    if (options_.IsCreateFromScratch()) {
      RemoveDirIfExists(data_dir);
      MakeDirIfNotExists(data_dir);
    }
    server_ = std::make_unique<GeneralServer>(options);
  }

  void Execute() {
    if (options_.IsCmdLoad()) {
      HandleCmdLoad();
    } else if (options_.IsCmdRun()) {
      HandleCmdRun();
    } else {
      TerminalLogFatal("Unknown command: " + options_.cmd_);
    }
  }

private:
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

  void HandleCmdLoad();

  void HandleCmdRun() {
    if (options_.clients_ > 0) {
      RunInClientServerMode();
    } else {
      RunInEmbeddedMode();
    }
  }

  void RunInEmbeddedMode();
  void RunInClientServerMode();

  void ReportBenchStats(std::atomic<bool>& keep_running);

  YcsbBenchStats& GetBenchStats(uint64_t i) {
    assert(i < bench_stats_.size());
    return *bench_stats_[i];
  }

  template <typename T>
  static void UpdateBenchStats(YcsbBenchStats& stats, const Result<T>& res) {
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

  const YcsbOptions options_;
  std::vector<std::unique_ptr<YcsbBenchStats>> bench_stats_;
  std::unique_ptr<GeneralServer> server_;
};

} // namespace leanstore::ycsb