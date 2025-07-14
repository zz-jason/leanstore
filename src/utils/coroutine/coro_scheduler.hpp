#pragma once

#include "leanstore/utils/log.hpp"
#include "utils/coroutine/coro_executor.hpp"
#include "utils/coroutine/coro_future.hpp"
#include "utils/scoped_timer.hpp"

#include <cassert>
#include <memory>
#include <type_traits>
#include <vector>

namespace leanstore {

/// The CoroScheduler is expected to be run on user threads, where each thread
/// can submit coroutines to be executed. It manages a pool of threads, each
/// capable of running coroutines concurrently. The scheduler ensures that
/// coroutines are executed in a non-blocking manner, allowing for efficient
/// parallel execution of user tasks.
class CoroScheduler {
public:
  CoroScheduler(LeanStore* store, int64_t num_threads)
      : num_threads_(num_threads),
        coro_executors_(num_threads) {
    assert(num_threads > 0 && "Number of threads must be greater than zero");
    for (int64_t i = 0; i < num_threads; ++i) {
      coro_executors_[i] = std::make_unique<CoroExecutor>(store, i);
    }
  }

  ~CoroScheduler() = default;

  CoroScheduler(const CoroScheduler&) = delete;
  CoroScheduler& operator=(const CoroScheduler&) = delete;
  CoroScheduler(CoroScheduler&&) = delete;
  CoroScheduler& operator=(CoroScheduler&&) = delete;

  void Init() {
    ScopedTimer timer([this](double elapsed_ms) {
      Log::Info("CoroScheduler inited, num_threads={}, elapsed={}ms", num_threads_, elapsed_ms);
    });

    // Start all threads
    for (auto& executor : coro_executors_) {
      executor->Start();
    }

    // Wait for all threads to be ready
    for (auto& executor : coro_executors_) {
      while (!executor->IsReady()) {
      }
    }
  }

  void Deinit() {
    ScopedTimer timer(
        [](double elapsed_ms) { Log::Info("CoroScheduler deinited, elapsed={}ms", elapsed_ms); });

    // Stop all threads
    for (auto& executor : coro_executors_) {
      executor->Stop();
    }

    // Wait for all threads to finish
    for (auto& executor : coro_executors_) {
      executor->Join();
    }
  }

  template <typename F, typename R = std::invoke_result_t<F>>
  std::shared_ptr<CoroFuture<R>> Submit(F&& coro_func, int64_t thread_id = -1) {
    if (thread_id < 0 || thread_id >= num_threads_) {
      // Default to the first thread if not specified
      // TODO: Implement a better thread selection strategy
      thread_id = 0;
    }

    auto coro_future = std::make_shared<CoroFuture<R>>();
    auto coro = std::make_unique<Coroutine>(
        [future = coro_future, f = std::forward<F>(coro_func)]() mutable {
          if constexpr (std::is_void_v<R>) {
            f();
            future->SetResult();
          } else {
            R result = f();
            future->SetResult(std::move(result));
          }
        });

    coro_executors_[thread_id]->EnqueueCoro(std::move(coro));
    return coro_future;
  }

  void ParallelRange(uint64_t num_jobs,
                     std::function<void(uint64_t job_begin, uint64_t job_end)>&& job_handler) {
    auto num_executors = coro_executors_.size();
    uint64_t jobs_per_thread = num_jobs / num_executors;
    uint64_t num_remaining = num_jobs % num_executors;
    uint64_t num_proceed_tasks = 0;
    if (jobs_per_thread < num_executors) {
      num_executors = num_remaining;
    }

    // To balance the workload among all threads:
    // - the first numRemaining threads process jobsPerThread+1 tasks
    // - other threads process jobsPerThread tasks
    std::vector<std::shared_ptr<CoroFuture<void>>> futures;
    for (uint64_t i = 0; i < num_executors; i++) {
      uint64_t begin = num_proceed_tasks;
      uint64_t end = begin + jobs_per_thread;
      if (num_remaining > 0) {
        end++;
        num_remaining--;
      }
      num_proceed_tasks = end;

      futures.emplace_back(Submit([begin, end, job_handler]() { job_handler(begin, end); }, i));
    }

    for (auto& future : futures) {
      future->Wait();
    }
  }

private:
  /// Number of threads in the thread pool.
  const int64_t num_threads_;

  /// CoroExecutor pool, each thread is responsible for executing coroutines submitted
  /// to it, runs its own coroutine scheduler.
  std::vector<std::unique_ptr<CoroExecutor>> coro_executors_;
};

} // namespace leanstore