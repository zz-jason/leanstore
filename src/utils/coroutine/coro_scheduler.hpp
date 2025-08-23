#pragma once

#include "leanstore/utils/log.hpp"
#include "utils/coroutine/coro_env.hpp"
#include "utils/coroutine/coro_executor.hpp"
#include "utils/coroutine/coro_future.hpp"
#include "utils/coroutine/coro_session.hpp"
#include "utils/scoped_timer.hpp"

#include <cassert>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
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

  /// Attempts to reserve a CoroSession for the current thread without blocking.
  /// Returns nullptr if no session is available.
  CoroSession* TryReserveCoroSession(uint64_t runs_on) {
    assert(runs_on < coro_executors_.size());
    std::lock_guard<std::mutex> lock(session_pool_mutex_per_exec_[runs_on]);
    if (session_pool_per_exec_[runs_on].empty()) {
      return nullptr; // No available session
    }
    auto* session = session_pool_per_exec_[runs_on].front();
    session_pool_per_exec_[runs_on].pop();
    return session;
  }

  /// Reserves a CoroSession for the current thread, blocking until one is available.
  CoroSession* ReserveCoroSession(uint64_t runs_on) {
    while (true) {
      auto* session = TryReserveCoroSession(runs_on);
      if (session != nullptr) {
        return session;
      }
      std::this_thread::yield(); // yield since no session is available
    }
  }

  /// Releases a CoroSession back to the session pool.
  void ReleaseCoroSession(CoroSession* coro_session) {
    assert(coro_session != nullptr);
    assert(coro_session->GetRunsOn() < coro_executors_.size());
    std::lock_guard<std::mutex> lock(session_pool_mutex_per_exec_[coro_session->GetRunsOn()]);
    session_pool_per_exec_[coro_session->GetRunsOn()].push(coro_session);
  }

  /// Submits a coroutine to be executed by the target CoroExecutor.
  template <typename F, typename R = std::invoke_result_t<F>>
  std::shared_ptr<CoroFuture<R>> Submit(CoroSession* coro_session, F&& coro_func);

  /// Executes a parallel range of jobs, distributing the workload across available threads.
  void ParallelRange(uint64_t num_jobs,
                     std::function<void(uint64_t job_begin, uint64_t job_end)>&& job_handler);

private:
  /// Create session pool for each executor.
  void CreateSessionPool();

  /// Initializes the CoroExecutor background threads.
  void InitCoroExecutors();

  /// Deinitializes the CoroExecutor background threads.
  void DeinitCoroExecutors();

  /// Pointer to the LeanStore instance.
  LeanStore* store_ = nullptr;

  std::vector<std::mutex> session_pool_mutex_per_exec_;
  std::vector<std::queue<CoroSession*>> session_pool_per_exec_;
  std::vector<std::unique_ptr<CoroSession>> all_sessions_;

  /// Number of threads in the thread pool.
  const int64_t num_threads_;

  /// CoroExecutor pool, responsible for executing coroutines on different threads.
  std::vector<std::unique_ptr<CoroExecutor>> coro_executors_;

  /// All the AutoCommitProtocol instances for each commit group.
  std::vector<std::unique_ptr<AutoCommitProtocol>> commit_protocols_;
};

template <typename F, typename R>
inline std::shared_ptr<CoroFuture<R>> CoroScheduler::Submit(CoroSession* session, F&& coro_func) {
  auto runs_on = session->GetRunsOn();
  assert(runs_on < coro_executors_.size() && "Invalid thread ID for coroutine submission");

  auto coro_future = std::make_shared<CoroFuture<R>>();
  auto coro_job = [future = coro_future, f = std::forward<F>(coro_func), session]() mutable {
    CoroEnv::SetCurTxMgr(session->GetTxMgr());
    if constexpr (std::is_void_v<R>) {
      f();
      future->SetResult();
    } else {
      R result = f();
      future->SetResult(std::move(result));
    }
  };
  coro_executors_[runs_on]->EnqueueCoro(std::make_unique<Coroutine>(std::move(coro_job)));

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
  std::vector<CoroSession*> reserved_sessions;
  for (uint64_t i = 0; i < num_executors; i++) {
    uint64_t begin = num_proceed_tasks;
    uint64_t end = begin + jobs_per_thread;
    if (num_remaining > 0) {
      end++;
      num_remaining--;
    }
    num_proceed_tasks = end;

    reserved_sessions.push_back(TryReserveCoroSession(i));
    assert(reserved_sessions.back() != nullptr &&
           "Failed to reserve a CoroSession for parallel range execution");
    futures.emplace_back(
        Submit(reserved_sessions.back(), [begin, end, job_handler]() { job_handler(begin, end); }));
  }

  for (auto& future : futures) {
    future->Wait();
  }
  for (auto* session : reserved_sessions) {
    ReleaseCoroSession(session);
  }
}

} // namespace leanstore
