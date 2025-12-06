#pragma once

#include "coroutine/coro_env.hpp"
#include "coroutine/coro_executor.hpp"
#include "coroutine/coro_future.hpp"
#include "coroutine/coro_session.hpp"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/cpp/base/range_splits.hpp"
#include "utils/scoped_timer.hpp"

#include <cassert>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <type_traits>
#include <vector>

namespace leanstore {

/// Forward declarations
class TxManager;
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
  CoroSession* TryReserveCoroSession(uint64_t runs_on);

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
  void ReleaseCoroSession(CoroSession* coro_session);

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
  std::vector<std::shared_ptr<CoroFuture<void>>> futures;
  std::vector<CoroSession*> reserved_sessions;
  RangeSplits<uint64_t> splitted_ranges(num_jobs, coro_executors_.size());
  for (auto i = 0u; i < coro_executors_.size(); i++) {
    reserved_sessions.push_back(TryReserveCoroSession(i));
    assert(reserved_sessions.back() != nullptr &&
           "Failed to reserve a CoroSession for parallel range execution");
    auto range = splitted_ranges[i];
    futures.emplace_back(Submit(reserved_sessions.back(), [range, &job_handler]() {
      job_handler(range.begin(), range.end());
    }));
  }

  for (auto& future : futures) {
    future->Wait();
  }
  for (auto* session : reserved_sessions) {
    ReleaseCoroSession(session);
  }
}

} // namespace leanstore
