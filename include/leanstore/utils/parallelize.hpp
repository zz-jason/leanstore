#pragma once

#include <cstdint>
#include <functional>

namespace leanstore::utils {

class Parallelize {
public:
  static void Range(
      uint64_t num_threads, uint64_t num_jobs,
      std::function<void(uint64_t thread_id, uint64_t job_begin, uint64_t job_end)> job_handler);

  static void ParallelRange(uint64_t num_jobs,
                            std::function<void(uint64_t job_begin, uint64_t job_end)> job_handler);
};

} // namespace leanstore::utils
