#pragma once

#include "utils/Log.hpp"
#include "utils/ScrambledZipfGenerator.hpp"

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>

// For the benchmark driver
DECLARE_string(ycsb_target);
DECLARE_string(ycsb_cmd);
DECLARE_string(ycsb_workload);
DECLARE_uint32(ycsb_threads);
DECLARE_uint64(ycsb_run_for_seconds);

// For the data preparation
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
  double mReadProportion;
  double mUpdateProportion;
  double mScanProportion;
  double mInsertProportion;
};

class YcsbExecutor {
public:
  virtual ~YcsbExecutor() = default;

  virtual void HandleCmdLoad() = 0;

  virtual void HandleCmdRun() = 0;
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
                           uint64_t numOperations) {
  // calculate secondas elaspsed
  auto sec = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
                 .count() /
             1000.0;
  return numOperations / sec;
}

inline void GenKey(uint64_t key, uint8_t* keyBuf) {
  auto keyStr = std::to_string(key);
  auto prefixSize = FLAGS_ycsb_key_size - keyStr.size() > 0
                        ? FLAGS_ycsb_key_size - keyStr.size()
                        : 0;
  std::memset(keyBuf, 'k', prefixSize);
  std::memcpy(keyBuf + prefixSize, keyStr.data(), keyStr.size());
}

inline void GenYcsbKey(utils::ScrambledZipfGenerator& zipfRandom,
                       uint8_t* keyBuf) {
  GenKey(zipfRandom.rand(), keyBuf);
}

} // namespace leanstore::ycsb