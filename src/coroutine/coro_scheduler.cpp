#include "coroutine/coro_scheduler.hpp"

#include "coroutine/auto_commit_protocol.hpp"
#include "coroutine/coro_future.hpp"
#include "coroutine/coro_session.hpp"
#include "coroutine/mvcc_manager.hpp"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/cpp/base/log.hpp"

#include <cassert>
#include <cstdint>
#include <format>
#include <memory>
#include <vector>

namespace leanstore {

CoroScheduler::CoroScheduler(LeanStore* store, int64_t num_threads)
    : store_(store),
      session_pool_mutex_per_exec_(num_threads),
      num_threads_(num_threads),
      coro_executors_(num_threads) {
  // create coro session pool
  CreateSessionPool();

  // create commit protocols for each commit group
  if (store != nullptr) {
    for (auto i = 0u; i < store->store_option_->worker_threads_; i++) {
      assert(store != nullptr && "Store must not be null when commit groups are enabled");
      commit_protocols_.emplace_back(std::make_unique<AutoCommitProtocol>(store, i));
    }
  }

  // create coroutine executors
  assert(num_threads > 0 && "Number of threads must be greater than zero");
  auto num_committers = commit_protocols_.size();
  for (int64_t i = 0; i < num_threads; ++i) {
    AutoCommitProtocol* committer = nullptr;
    if (num_committers > 0) {
      committer = commit_protocols_[i % num_committers].get();
    }
    coro_executors_[i] = std::make_unique<CoroExecutor>(store, committer, i);
  }
}

CoroScheduler::~CoroScheduler() {
}

CoroSession* CoroScheduler::TryReserveCoroSession(uint64_t runs_on) {
  assert(runs_on < coro_executors_.size());
  std::lock_guard<std::mutex> lock(session_pool_mutex_per_exec_[runs_on]);
  if (session_pool_per_exec_[runs_on].empty()) {
    return nullptr; // No available session
  }

  auto* session = session_pool_per_exec_[runs_on].front();
  session_pool_per_exec_[runs_on].pop();
  Log::Info("CoroSession reserved, runs_on={}, tx_mgr={}", session->GetRunsOn(),
            session->GetTxMgr() == nullptr ? -1 : session->GetTxMgr()->worker_id_);
  return session;
}

void CoroScheduler::ReleaseCoroSession(CoroSession* coro_session) {
  assert(coro_session != nullptr);
  assert(coro_session->GetRunsOn() < coro_executors_.size());
  std::lock_guard<std::mutex> lock(session_pool_mutex_per_exec_[coro_session->GetRunsOn()]);
  session_pool_per_exec_[coro_session->GetRunsOn()].push(coro_session);
  Log::Info("CoroSession released, runs_on={}, tx_mgr={}", coro_session->GetRunsOn(),
            coro_session->GetTxMgr() == nullptr ? -1 : coro_session->GetTxMgr()->worker_id_);
}

void CoroScheduler::InitCoroExecutors() {
  ScopedTimer timer([this](double elapsed_ms) {
    Log::Info("CoroExecutors inited, num_threads={}, elapsed={}ms", num_threads_, elapsed_ms);
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

  // set thread-local logging for each executor
  if (store_ != nullptr) {
    auto& loggings = store_->GetMvccManager().Loggings();
    LEAN_DCHECK(loggings.size() == coro_executors_.size(),
                "Number of loggings must match number of executors");
    for (auto i = 0u; i < loggings.size(); i++) {
      auto* logging = loggings[i].get();
      auto* coro_session = TryReserveCoroSession(i);
      assert(coro_session != nullptr && "Failed to reserve a CoroSession for coroutine execution");
      Submit(coro_session, [logging]() { CoroEnv::SetCurLogging(logging); })->Wait();
      ReleaseCoroSession(coro_session);
    }
  }
}

void CoroScheduler::CreateSessionPool() {
  auto num_exec = coro_executors_.size();
  auto num_session_per_exec = (uint64_t)CoroEnv::kMaxCoroutinesPerThread;
  if (store_ != nullptr) {
    num_session_per_exec = store_->store_option_->max_concurrent_transaction_per_worker_;
  }

  // create all coro sessions
  all_sessions_.reserve(num_exec * num_session_per_exec);
  session_pool_per_exec_.resize(num_exec);
  for (auto i = 0u; i < num_exec * num_session_per_exec; i++) {
    auto runs_on = i % num_exec;
    TxManager* tx_mgr = nullptr;
    if (store_ != nullptr) {
      assert(i < store_->GetMvccManager().TxMgrs().size() && "Invalid index for TxManager");
      tx_mgr = store_->GetMvccManager().TxMgrs()[i].get();
    }
    all_sessions_.emplace_back(std::make_unique<CoroSession>(runs_on, tx_mgr));
    session_pool_per_exec_[runs_on].push(all_sessions_.back().get());
  }
}

void CoroScheduler::DeinitCoroExecutors() {
  ScopedTimer timer([this](double elapsed_ms) {
    Log::Info("CoroExecutors deinited, num_threads={}, elapsed={}ms", num_threads_, elapsed_ms);
  });

  // Stop all threads
  for (auto& executor : coro_executors_) {
    executor->Stop();
  }

  // Wait for all threads to finish
  for (auto& executor : coro_executors_) {
    executor->Join();
  }
}

} // namespace leanstore