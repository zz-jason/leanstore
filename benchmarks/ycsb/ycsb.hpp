#pragma once

#include "leanstore/utils/log.hpp"
#include "leanstore/utils/scrambled_zipf_generator.hpp"

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <format>
#include <iostream>
#include <string>
#include <vector>

#include <unistd.h>

// For the benchmark driver
DECLARE_string(ycsb_target);
DECLARE_string(ycsb_cmd);
DECLARE_string(ycsb_workload);
DECLARE_uint32(ycsb_threads);
DECLARE_uint32(ycsb_clients);
DECLARE_uint64(ycsb_mem_gb);
DECLARE_uint64(ycsb_run_for_seconds);

// For the data preparation
DECLARE_string(ycsb_data_dir);
DECLARE_uint64(ycsb_key_size);
DECLARE_uint64(ycsb_val_size);
DECLARE_uint64(ycsb_record_count);
DECLARE_double(ycsb_zipf_factor);

namespace leanstore::ycsb {

enum class Distrubition : uint8_t {
  kUniform = 0,
  kZipf = 1,
  kLatest = 2,
};

enum class Workload : uint8_t {
  kA = 0,
  kB = 1,
  kC = 2,
  kD = 3,
  kE = 4,
  kF = 5,
};

struct WorkloadSpec {
  double read_proportion_;
  double update_proportion_;
  double scan_proportion_;
  double insert_proportion_;
};

class YcsbExecutor {
public:
  virtual ~YcsbExecutor() = default;

  virtual void HandleCmdLoad() {
  }

  virtual void HandleCmdRun() {
  }

protected:
  void PrintTpsSummary(uint64_t report_period, uint64_t run_for_seconds, uint64_t num_threads,
                       std::vector<std::atomic<uint64_t>>& thread_committed,
                       std::vector<std::atomic<uint64_t>>& thread_aborted) {
    for (uint64_t i = 0; i < run_for_seconds; i += report_period) {
      sleep(report_period);
      auto committed = 0;
      auto aborted = 0;
      for (auto& c : thread_committed) {
        committed += c.exchange(0);
      }
      for (auto& a : thread_aborted) {
        aborted += a.exchange(0);
      }
      PrintTps(num_threads, i, committed, aborted, report_period);
    }
  }

  void PrintTps(uint64_t num_threads, uint64_t time_elasped_sec, uint64_t committed,
                uint64_t aborted, uint64_t report_period) {
    auto abort_rate = (aborted) * 1.0 / (committed + aborted);
    std::cout << std::format(
                     "[{} thds] [{:2}s] [tps={}] [committed={}] [conflicted={}] [conflict rate={}]",
                     num_threads, time_elasped_sec,
                     FormatWithSpaces((committed + aborted) / report_period),
                     FormatWithSpaces(committed), FormatWithSpaces(aborted), abort_rate)
              << std::endl;
  }

  std::string FormatWithSpaces(uint64_t n) {
    std::string num = std::to_string(n);
    int len = num.length();
    std::ostringstream oss;

    int first_group = len % 3;
    if (first_group == 0) {
      first_group = 3;
    }

    oss << num.substr(0, first_group);
    for (int i = first_group; i < len; i += 3) {
      oss << ' ' << num.substr(i, 3);
    }
    return oss.str();
  }
};

// Generate workload spec from workload type
inline WorkloadSpec GetWorkloadSpec(Workload workload) {
  switch (workload) {
  case Workload::kA:
    return {0.5, 0.5, 0.0, 0.0};
  case Workload::kB:
    return {0.95, 0.05, 0.0, 0.0};
  case Workload::kC:
    return {1.0, 0.0, 0.0, 0.0};
  case Workload::kD:
    return {0.95, 0.0, 0.0, 0.05};
  case Workload::kE:
    return {0.0, 0.0, 0.95, 0.05};
  case Workload::kF:
    return {0.5, 0.0, 0.0, 0.5};
  default:
    Log::Fatal("Unknown workload: {}", static_cast<uint8_t>(workload));
  }
  return {};
}

inline double CalculateTps(std::chrono::high_resolution_clock::time_point begin,
                           std::chrono::high_resolution_clock::time_point end,
                           uint64_t num_operations) {
  // calculate secondas elaspsed
  auto sec = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() / 1000.0;
  return num_operations / sec;
}

inline void GenKey(uint64_t key, uint8_t* key_buf) {
  auto key_str = std::to_string(key);
  auto prefix_size =
      FLAGS_ycsb_key_size - key_str.size() > 0 ? FLAGS_ycsb_key_size - key_str.size() : 0;
  std::memset(key_buf, 'k', prefix_size);
  std::memcpy(key_buf + prefix_size, key_str.data(), key_str.size());
}

inline void GenYcsbKey(utils::ScrambledZipfGenerator& zipf_random, uint8_t* key_buf) {
  GenKey(zipf_random.rand(), key_buf);
}

} // namespace leanstore::ycsb