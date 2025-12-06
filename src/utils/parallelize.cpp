#include "leanstore/utils/parallelize.hpp"

#include "coroutine/coro_env.hpp"
#include "leanstore/cpp/base/likely.hpp"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/cpp/base/range_splits.hpp"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <thread>
#include <vector>

namespace leanstore::utils {

void Parallelize::Range(
    uint64_t num_threads, uint64_t num_jobs,
    std::function<void(uint64_t thread_id, uint64_t job_begin, uint64_t job_end)> job_handler) {
  auto& store = CoroEnv::CurStore();
  std::vector<std::thread> threads;
  LEAN_DCHECK(num_threads <= num_threads, "Too many threads for the given number of jobs");

  auto ranges = RangeSplits<uint64_t>(num_jobs, num_threads);
  for (auto i = 0u; i < num_threads; ++i) {
    threads.emplace_back(
        [&](uint64_t begin, uint64_t end) {
          CoroEnv::SetCurStore(&store);
          job_handler(i, begin, end);
        },
        ranges[i].begin(), ranges[i].end());
  }

  // wait all threads to finish
  for (auto& thread : threads) {
    thread.join();
  }
}

void Parallelize::ParallelRange(
    uint64_t num_jobs, std::function<void(uint64_t job_begin, uint64_t job_end)> job_handler) {
  if (LEAN_UNLIKELY(num_jobs == 0)) {
    return;
  }

  auto& store = CoroEnv::CurStore();
  uint64_t num_thread = std::max(1U, std::thread::hardware_concurrency());
  num_thread = std::min(num_thread, num_jobs);

  std::vector<std::thread> threads;
  threads.reserve(num_thread);

  RangeSplits<uint64_t> ranges(num_jobs, num_thread);

  for (auto i = 0u; i < num_thread; i++) {
    auto range = ranges[i];
    threads.emplace_back(
        [&](uint64_t begin, uint64_t end) {
          CoroEnv::SetCurStore(&store);
          job_handler(begin, end);
        },
        range.begin(), range.end());
  }

  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

} // namespace leanstore::utils
