#include "tools/ycsb/ycsb_executor.hpp"

#include "leanstore/cpp/base/range_splits.hpp"
#include "leanstore/cpp/base/slice.hpp"
#include "leanstore/cpp/base/small_vector.hpp"
#include "leanstore/utils/random_generator.hpp"
#include "leanstore/utils/zipfian_generator.hpp"
#include "tools/ycsb/general_kv_space.hpp"
#include "tools/ycsb/general_server.hpp"
#include "tools/ycsb/terminal_log.hpp"
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
#include <thread>
#include <vector>

#include <netinet/in.h>
#include <sys/types.h>

namespace leanstore::ycsb {

void YcsbExecutor::HandleCmdLoad() {
  // Create KvSpace before loading data
  server_->ExecOn(0, [&]() { server_->CreateKvSpace(); });

  auto insert_func = [&](uint64_t wid, uint64_t rid_begin, uint64_t rid_end) {
    auto kv_space = server_->GetKvSpace();

    TerminalLogInfo(std::format("Server thread started, wid={}", wid));
    SmallBuffer256 key_buffer(options_.key_size_);
    SmallBuffer256 val_buffer(options_.val_size_);
    auto& bench_stats = GetBenchStats(wid);

    for (auto i = rid_begin; i < rid_end; i++) {
      KeyAt(i, key_buffer.Data(), options_.key_size_);
      RandValue(val_buffer.Data(), options_.val_size_);
      auto res = kv_space->Insert(Slice(key_buffer.Data(), options_.key_size_),
                                  Slice(val_buffer.Data(), options_.val_size_));
      UpdateBenchStats(bench_stats, res);
      if (!res) {
        TerminalLogFatal(
            std::format("Failed to insert key during loading: {}", res.error().ToString()));
      }
    }
  };

  auto ranges = RangeSplits<uint64_t>(options_.record_count_, options_.threads_);
  for (auto i = 0U; i < options_.threads_; i++) {
    auto range = ranges[i];
    server_->AsyncExecOn(i, [wid = i, f = insert_func, rid_begin = range.begin(),
                             rid_end = range.end()]() { f(wid, rid_begin, rid_end); });
  }

  std::atomic<bool> keep_running = true;
  std::thread reporter_thread([this, &keep_running]() { ReportBenchStats(keep_running); });

  server_->WaitAll();

  keep_running.store(false);
  reporter_thread.join();
}

void YcsbExecutor::RunInEmbeddedMode() {
  std::atomic<bool> keep_running = true;
  auto workload_func = [&](uint64_t wid) {
    TerminalLogInfo(std::format("Server thread started, wid={}", wid));
    auto workload_type = options_.GetWorkloadType();
    auto workload_spec = options_.GetWorkloadSpec();
    auto kv_space = server_->GetKvSpace();
    utils::ScrambledZipfianGenerator gen(0, options_.record_count_, options_.zipf_factor_);
    SmallBuffer256 key_buffer(options_.key_size_);
    SmallBuffer256 val_buffer(options_.val_size_);
    ByteBuffer value_out;
    value_out.reserve(options_.val_size_);
    auto& bench_stats = GetBenchStats(wid);

    while (keep_running) {
      switch (workload_type) {
      case YcsbWorkloadType::kA:
      case YcsbWorkloadType::kB:
      case YcsbWorkloadType::kC: {
        KeyAt(gen.Rand(), key_buffer.Data(), options_.key_size_);
        auto read_probability = utils::RandomGenerator::Rand(0, 100);

        // read
        if (read_probability <= workload_spec.read_proportion_ * 100) {
          auto res = kv_space->Lookup(Slice(key_buffer.Data(), options_.key_size_), value_out);
          UpdateBenchStats(bench_stats, res);
          break;
        }

        // update
        RandValue(val_buffer.Data(), options_.val_size_);
        auto res = kv_space->Update(Slice(key_buffer.Data(), options_.key_size_),
                                    Slice(val_buffer.Data(), options_.val_size_));
        UpdateBenchStats(bench_stats, res);
        break;
      }
      default: {
        TerminalLogFatal(
            std::format("Unsupported workload type: {}", static_cast<uint8_t>(workload_type)));
      }
      }
    }
  };

  // Dispatch workload to all server threads
  for (uint64_t i = 0; i < options_.threads_; i++) {
    server_->AsyncExecOn(i, [wid = i, f = workload_func]() mutable { f(wid); });
  }

  // Launch reporter
  std::thread reporter_thread([this, &keep_running]() { ReportBenchStats(keep_running); });

  // Run for the specified duration
  sleep(options_.run_for_seconds_);

  // Signal workers and reporter to stop
  keep_running.store(false);

  // Wait for all workers to finish
  server_->WaitAll();
  reporter_thread.join();
}

void YcsbExecutor::RunInClientServerMode() {
  std::atomic<bool> keep_running = true;
  auto client_func = [&](uint64_t cid) {
    TerminalLogInfo(std::format("Client thread started, cid={}", cid));
    std::string thread_name = std::format("ycsb_cli_{}", cid);
    pthread_setname_np(pthread_self(), thread_name.c_str());

    auto wid = cid % options_.threads_;
    auto workload_type = options_.GetWorkloadType();
    auto workload_spec = options_.GetWorkloadSpec();

    // Get KvSpace reference from the server for benchmarking
    std::unique_ptr<GeneralKvSpace> kv_space = nullptr;
    server_->ExecOn(wid, [&kv_space, this]() { kv_space = server_->GetKvSpace(); });

    utils::ScrambledZipfianGenerator gen(0, options_.record_count_, options_.zipf_factor_);
    SmallBuffer256 key_buffer(options_.key_size_);
    SmallBuffer256 val_buffer(options_.val_size_);
    ByteBuffer value_out;
    value_out.reserve(options_.val_size_);
    auto& bench_stats = GetBenchStats(cid);

    while (keep_running) {
      switch (workload_type) {
      case YcsbWorkloadType::kA:
      case YcsbWorkloadType::kB:
      case YcsbWorkloadType::kC: {
        KeyAt(gen.Rand(), key_buffer.Data(), options_.key_size_);
        auto read_probability = utils::RandomGenerator::Rand(0, 100);

        // read
        if (read_probability <= workload_spec.read_proportion_ * 100) {
          auto res = server_->ExecOn(wid, [&]() {
            return kv_space->Lookup(Slice(key_buffer.Data(), options_.key_size_), value_out);
          });
          UpdateBenchStats(bench_stats, res);
          break;
        }

        // update
        RandValue(val_buffer.Data(), options_.val_size_);
        auto res = server_->ExecOn(wid, [&]() {
          return kv_space->Update(Slice(key_buffer.Data(), options_.key_size_),
                                  Slice(val_buffer.Data(), options_.val_size_));
        });
        UpdateBenchStats(bench_stats, res);
        break;
      }
      default: {
        TerminalLogFatal(
            std::format("Unsupported workload type: {}", static_cast<uint8_t>(workload_type)));
      }
      }
    }
  };

  // Start all client threads
  std::vector<std::thread> client_threads;
  for (uint64_t i = 0; i < options_.clients_; i++) {
    client_threads.emplace_back([&, cid = i]() { client_func(cid); });
  }

  // Launch reporter
  std::thread reporter_thread([this, &keep_running]() { ReportBenchStats(keep_running); });

  // Run for the specified duration
  sleep(options_.run_for_seconds_);

  // Signal clients and reporter to stop
  keep_running.store(false);

  // Wait for all clients to finish
  for (auto& thread : client_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  // Wait for reporter to finish
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
    TerminalLogInfo(std::format("[{}s] [TPS={}] [COMMITTED={}] [ABORTED={}] [ABORT_RATE={:.2f}%]",
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

  TerminalLogInfo(std::format(
      "Summary:\n"
      "  -threads: {}\n" // This should probably be options_.clients_ or options_.threads_
      "  -total_time_sec: {}\n"
      "  -avg_tps: {}\n"
      "  -total_committed: {}\n"
      "  -total_aborted: {}\n"
      "  -total_abort_rate: {:.2f}%", // Display abort rate as percentage
      options_.clients_ > 0 ? options_.clients_ : options_.threads_, // Display clients or threads
      elapsed_sec, (total_committed_overall + total_aborted_overall) / elapsed_sec,
      total_committed_overall, total_aborted_overall, total_abort_rate_overall * 100));
}
} // namespace leanstore::ycsb