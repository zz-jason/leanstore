#include "tools/ycsb/ycsb_executor.hpp"

#include "leanstore/base/range_splits.hpp"
#include "leanstore/utils/random_generator.hpp"
#include "leanstore/utils/zipfian_generator.hpp"
#include "tools/ycsb/console_logger.hpp"
#include "tools/ycsb/ycsb_options.hpp"
#include "ycsb_workload_spec.hpp"

#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <format>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <netinet/in.h>
#include <sys/types.h>

namespace leanstore::ycsb {

YcsbExecutor::YcsbExecutor(const YcsbOptions& options) : options_(options) {
  // Init bench stats
  bench_stats_.reserve(options_.clients_);
  for (auto i = 0U; i < options_.clients_; i++) {
    bench_stats_.emplace_back(std::make_unique<YcsbBenchStats>());
  }

  // Init backend
  auto data_dir = options_.DataDir();
  if (options_.IsCreateFromScratch()) {
    RemoveDirIfExists(data_dir);
    MakeDirIfNotExists(data_dir);
  }
  auto res = YcsbDb::Create(options);
  if (!res) {
    ConsoleFatal(std::format("Failed to create YCSB DB: {}", res.error().ToString()));
  }
  db_ = std::move(res.value());
}

void YcsbExecutor::HandleCmdLoad() {
  // Create KvSpace before loading data
  auto res = db_->NewSession().CreateKvSpace(kKvSpaceName);
  if (!res) {
    ConsoleFatal(std::format("Failed to create KvSpace: {}", res.error().ToString()));
  }

  // Client function to load data
  std::atomic<uint64_t> finished_clients = 0;
  auto client_func = [&](uint64_t cid, uint64_t rid_begin, uint64_t rid_end) {
    ConsoleInfo("YCSB loader thread started: " + std::to_string(cid));
    std::string thread_name = std::format("ycsb_loader_{}", cid);
    pthread_setname_np(pthread_self(), thread_name.c_str());

    auto session = db_->NewSession();
    auto kv_space_res = session.GetKvSpace(kKvSpaceName);
    if (!kv_space_res) {
      ConsoleFatal(std::format("Failed to get KvSpace: {}", kv_space_res.error().ToString()));
    }
    auto kv_space = std::move(kv_space_res.value());
    std::string key(options_.key_size_, '\0');
    std::string value(options_.val_size_, '\0');

    auto& bench_stats = GetBenchStats(cid);

    for (auto i = rid_begin; i < rid_end; i++) {
      KeyAt(i, reinterpret_cast<uint8_t*>(key.data()), key.size());
      RandValue(reinterpret_cast<uint8_t*>(value.data()), value.size());
      auto res = kv_space.Put(key, value);
      if (!res) {
        ConsoleFatal(
            std::format("Failed to insert key during loading: {}", res.error().ToString()));
        bench_stats.IncAborted();
      } else {
        bench_stats.IncCommitted();
      }
    }

    finished_clients.fetch_add(1);
    ConsoleInfo("YCSB loader thread finished: " + std::to_string(cid));
  };

  // Launch all loader threads to load data
  auto ranges = RangeSplits<uint64_t>(options_.rows_, options_.clients_);
  std::vector<std::thread> loader_threads;
  for (auto i = 0U; i < options_.clients_; i++) {
    auto range = ranges[i];
    loader_threads.emplace_back([&, cid = i, rid_begin = range.begin(), rid_end = range.end()]() {
      client_func(cid, rid_begin, rid_end);
    });
  }

  // Launch reporter thread
  std::atomic<bool> keep_running = true;
  std::thread reporter_thread([this, &keep_running]() { ReportBenchStats(keep_running); });

  // Wait for all loader threads to finish
  for (auto& thread : loader_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  // Stop reporter thread
  keep_running.store(false);
  reporter_thread.join();
}

void YcsbExecutor::HandleCmdRun() {
  std::atomic<bool> keep_running = true;

  // Client function to run workload
  auto client_func = [&](uint64_t cid) {
    ConsoleInfo("YCSB runner thread started: " + std::to_string(cid));
    std::string thread_name = std::format("ycsb_runner_{}", cid);
    pthread_setname_np(pthread_self(), thread_name.c_str());

    auto session = db_->NewSession();
    auto kv_space_res = session.GetKvSpace(kKvSpaceName);
    if (!kv_space_res) {
      ConsoleFatal(std::format("Failed to get KvSpace: {}", kv_space_res.error().ToString()));
    }
    auto kv_space = std::move(kv_space_res.value());
    std::string key(options_.key_size_, '\0');
    std::string value(options_.val_size_, '\0');

    auto& bench_stats = GetBenchStats(cid);
    utils::ScrambledZipfianGenerator gen(0, options_.rows_, options_.zipf_factor_);

    auto workload_type = options_.GetWorkloadType();
    auto workload_spec = options_.GetWorkloadSpec();

    while (keep_running) {
      switch (workload_type) {
      case YcsbWorkloadType::kA:
      case YcsbWorkloadType::kB:
      case YcsbWorkloadType::kC: {
        KeyAt(gen.Rand(), reinterpret_cast<uint8_t*>(key.data()), key.size());
        auto read_probability = utils::RandomGenerator::Rand(0, 100);

        // read
        if (read_probability <= workload_spec.read_proportion_ * 100) {
          auto res = kv_space.Get(key, value);
          UpdateBenchStats(bench_stats, res);
          break;
        }

        // update
        RandValue(reinterpret_cast<uint8_t*>(value.data()), value.size());
        auto res = kv_space.Update(key, value);
        UpdateBenchStats(bench_stats, res);
        break;
      }
      default: {
        ConsoleFatal(
            std::format("Unsupported workload type: {}", static_cast<uint8_t>(workload_type)));
      }
      }
    }

    ConsoleInfo("YCSB runner thread finished: " + std::to_string(cid));
  };

  // Launch all runner threads to run workload
  std::vector<std::thread> runner_threads;
  for (uint64_t i = 0; i < options_.clients_; i++) {
    runner_threads.emplace_back([&, cid = i]() { client_func(cid); });
  }

  // Launch reporter thread
  std::thread reporter_thread([this, &keep_running]() { ReportBenchStats(keep_running); });

  // Run for the specified duration
  sleep(options_.duration_);

  // Signal runner and reporter threads to stop
  keep_running.store(false);

  // Wait for all runner threads to finish
  for (auto& thread : runner_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  // Wait for reporter thread to finish
  reporter_thread.join();
}

void YcsbExecutor::ReportBenchStats(std::atomic<bool>& keep_running) {
  std::string thread_name = std::format("ycsb_reporter");
  pthread_setname_np(pthread_self(), thread_name.c_str());

  auto report_period = options_.GetReportPeriodSec();
  auto elapsed_sec = 0U;

  // Use a local vector to store per-report metrics for final summary
  std::vector<std::pair<uint64_t, uint64_t>> aggregated_metrics_per_report;

  do {
    sleep(report_period);
    elapsed_sec += report_period;
    uint64_t current_committed = 0;
    uint64_t current_aborted = 0;
    for (auto& stats : bench_stats_) {
      stats->MergeInto(current_committed, current_aborted);
    }

    auto abort_rate = (current_committed + current_aborted > 0)
                          ? (current_aborted) * 1.0 / (current_committed + current_aborted)
                          : 0.0;
    ConsoleInfo(std::format("[{}s] [TPS={}] [COMMITTED={}] [ABORTED={}] [ABORT_RATE={:.2f}%]",
                            elapsed_sec, (current_committed + current_aborted) / report_period,
                            current_committed, current_aborted,
                            abort_rate * 100)); // Display abort rate as percentage

    aggregated_metrics_per_report.emplace_back(
        current_committed, current_aborted); // Store committed and aborted for this period

  } while (keep_running.load());

  // Final report logic needs to sum up from aggregated_metrics_per_report
  uint64_t total_committed_overall = 0;
  uint64_t total_aborted_overall = 0;
  for (const auto& metric : aggregated_metrics_per_report) {
    total_committed_overall += metric.first;
    total_aborted_overall += metric.second;
  }

  double total_abort_rate_overall =
      (total_committed_overall + total_aborted_overall > 0)
          ? (total_aborted_overall) * 1.0 / (total_committed_overall + total_aborted_overall)
          : 0.0;

  ConsoleInfo(std::format("Summary:\n"
                          "  -clients: {}\n"
                          "  -total_time_sec: {}\n"
                          "  -avg_tps: {}\n"
                          "  -total_committed: {}\n"
                          "  -total_aborted: {}\n"
                          "  -total_abort_rate: {:.2f}%", // Display abort rate as percentage
                          options_.clients_, elapsed_sec,
                          (total_committed_overall + total_aborted_overall) / elapsed_sec,
                          total_committed_overall, total_aborted_overall,
                          total_abort_rate_overall * 100));
}
} // namespace leanstore::ycsb