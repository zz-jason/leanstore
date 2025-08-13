#pragma once

#include "leanstore/utils/log.hpp"
#include "utils/coroutine/coro_executor.hpp"
#include "utils/coroutine/coro_future.hpp"
#include "utils/scoped_timer.hpp"

#include <cassert>
#include <memory>
#include <type_traits>
#include <vector>

namespace leanstore::cr {
class TxManager;
} // namespace leanstore::cr

namespace leanstore {

class AutoCommitProtocol;

/// The CoroScheduler is expected to be run on user threads, where each thread
/// can submit coroutines to be executed. It manages a pool of threads, each
/// capable of running coroutines concurrently. The scheduler ensures that
/// coroutines are executed in a non-blocking manner, allowing for efficient
/// parallel execution of user tasks.
class CoroScheduler {
public:
  CoroScheduler(LeanStore* store, int64_t num_threads);
  ~CoroScheduler();

  // No copy or move semantics
  CoroScheduler(const CoroScheduler&) = delete;
  CoroScheduler& operator=(const CoroScheduler&) = delete;
  CoroScheduler(CoroScheduler&&) = delete;
  CoroScheduler& operator=(CoroScheduler&&) = delete;

  /// Initializes the CoroScheduler, starting all CoroExecutors and worker contexts.
  void Init() {
    ScopedTimer timer(
        [](double elapsed_ms) { Log::Info("CoroScheduler inited, elapsed={}ms", elapsed_ms); });

    InitCoroExecutors();
  }

  /// Deinitializes the CoroScheduler, stopping all CoroExecutors and worker contexts.
  void Deinit() {
    ScopedTimer timer(
        [](double elapsed_ms) { Log::Info("CoroScheduler deinited, elapsed={}ms", elapsed_ms); });

    DeinitCoroExecutors();
  }

  /// Submits a coroutine to be executed by the target CoroExecutor.
  template <typename F, typename R = std::invoke_result_t<F>>
  std::shared_ptr<CoroFuture<R>> Submit(F&& coro_func, int64_t thread_id = -1);

  /// Executes a parallel range of jobs, distributing the workload across available threads.
  void ParallelRange(uint64_t num_jobs,
                     std::function<void(uint64_t job_begin, uint64_t job_end)>&& job_handler);

private:
  /// Initializes the CoroExecutor background threads.
  void InitCoroExecutors();

  /// Deinitializes the CoroExecutor background threads.
  void DeinitCoroExecutors();

  /// Pointer to the LeanStore instance.
  LeanStore* store_ = nullptr;

  /// Number of threads in the thread pool.
  const int64_t num_threads_;

  /// CoroExecutor pool, responsible for executing coroutines on different threads.
  std::vector<std::unique_ptr<CoroExecutor>> coro_executors_;

  /// All the AutoCommitProtocol instances for each commit group.
  std::vector<std::unique_ptr<AutoCommitProtocol>> commit_protocols_;
};

template <typename F, typename R>
inline std::shared_ptr<CoroFuture<R>> CoroScheduler::Submit(F&& coro_func, int64_t thread_id) {
  // Default to the first thread if not specified
  // TODO: Implement a better thread selection strategy
  if (thread_id < 0 || thread_id >= num_threads_) {
    thread_id = 0;
  }

  auto coro_future = std::make_shared<CoroFuture<R>>();
  auto coro_job = [future = coro_future, f = std::forward<F>(coro_func)]() mutable {
    if constexpr (std::is_void_v<R>) {
      f();
      future->SetResult();
    } else {
      R result = f();
      future->SetResult(std::move(result));
    }
  };
  coro_executors_[thread_id]->EnqueueCoro(std::make_unique<Coroutine>(std::move(coro_job)));

  return coro_future;
}

inline void CoroScheduler::ParallelRange(
    uint64_t num_jobs, std::function<void(uint64_t job_begin, uint64_t job_end)>&& job_handler) {
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

} // namespace leanstore
