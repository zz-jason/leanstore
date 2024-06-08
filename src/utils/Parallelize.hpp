#pragma once

#include <cstdint>
#include <functional>

namespace leanstore {
namespace utils {

class Parallelize {
public:
  static void Range(
      uint64_t numThreads, uint64_t numJobs,
      std::function<void(uint64_t threadId, uint64_t jobBegin, uint64_t jobEnd)> jobHandler);

  static void ParallelRange(uint64_t numJobs,
                            std::function<void(uint64_t jobBegin, uint64_t jobEnd)> jobHandler);
};

} // namespace utils
} // namespace leanstore
