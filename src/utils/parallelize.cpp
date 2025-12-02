#include "leanstore/utils/parallelize.hpp"

#include "coroutine/coro_env.hpp"
#include "leanstore/utils/log.hpp"

#include <functional>
#include <thread>
#include <vector>

namespace leanstore::utils {

void Parallelize::Range(
    uint64_t num_threads, uint64_t num_jobs,
    std::function<void(uint64_t thread_id, uint64_t job_begin, uint64_t job_end)> job_handler) {
  auto& store = CoroEnv::CurStore();
  std::vector<std::thread> threads;
  const uint64_t jobs_per_thread = num_jobs / num_threads;
  LEAN_DCHECK(jobs_per_thread > 0, "Jobs per thread must be > 0");

  for (uint64_t thread_id = 0; thread_id < num_threads; thread_id++) {
    uint64_t begin = (thread_id * jobs_per_thread);
    uint64_t end = begin + (jobs_per_thread);
    if (thread_id == num_threads - 1) {
      end = num_jobs;
    }

    threads.emplace_back(
        [&](uint64_t begin, uint64_t end) {
          CoroEnv::SetCurStore(&store);
          job_handler(thread_id, begin, end);
        },
        begin, end);
  }

  // wait all threads to finish
  for (auto& thread : threads) {
    thread.join();
  }
}

void Parallelize::ParallelRange(
    uint64_t num_jobs, std::function<void(uint64_t job_begin, uint64_t job_end)>&& job_handler) {
  auto& store = CoroEnv::CurStore();
  std::vector<std::thread> threads;
  uint64_t num_thread = std::thread::hardware_concurrency();
  uint64_t jobs_per_thread = num_jobs / num_thread;
  uint64_t num_remaining = num_jobs % num_thread;
  uint64_t num_proceed_tasks = 0;
  if (jobs_per_thread < num_thread) {
    num_thread = num_remaining;
  }

  // To balance the workload among all threads:
  // - the first numRemaining threads process jobsPerThread+1 tasks
  // - other threads process jobsPerThread tasks
  for (uint64_t i = 0; i < num_thread; i++) {
    uint64_t begin = num_proceed_tasks;
    uint64_t end = begin + jobs_per_thread;
    if (num_remaining > 0) {
      end++;
      num_remaining--;
    }
    num_proceed_tasks = end;
    threads.emplace_back(
        [&](uint64_t begin, uint64_t end) {
          CoroEnv::SetCurStore(&store);
          job_handler(begin, end);
        },
        begin, end);
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

} // namespace leanstore::utils
