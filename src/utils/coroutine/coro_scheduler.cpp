#include "utils/coroutine/coro_scheduler.hpp"

#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/utils/log.hpp"
#include "utils/coroutine/auto_commit_protocol.hpp"
#include "utils/coroutine/coro_future.hpp"
#include "utils/coroutine/mvcc_manager.hpp"

#include <format>
#include <memory>
#include <vector>

namespace leanstore {

CoroScheduler::CoroScheduler(LeanStore* store, int64_t num_threads)
    : store_(store),
      num_threads_(num_threads),
      coro_executors_(num_threads) {

  // create commit protocols for each commit group
  auto commit_group_size = store ? store->store_option_->commit_group_size_ : 0;
  for (auto i = 0u; i < commit_group_size; i++) {
    commit_protocols_.emplace_back(
        std::make_unique<AutoCommitProtocol>(store, i, store->store_option_->worker_threads_));
  }

  // create coroutine executors
  assert(num_threads > 0 && "Number of threads must be greater than zero");
  for (int64_t i = 0; i < num_threads; ++i) {
    auto* commit_protocol =
        commit_group_size == 0 ? nullptr : commit_protocols_[i % commit_group_size].get();
    coro_executors_[i] = std::make_unique<CoroExecutor>(store, commit_protocol, i);
  }
}

CoroScheduler::~CoroScheduler() {
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
    auto& loggings = store_->MvccManager()->Loggings();
    LEAN_DCHECK(loggings.size() == coro_executors_.size(),
                "Number of loggings must match number of executors");
    for (auto i = 0u; i < loggings.size(); i++) {
      auto* logging = loggings[i].get();
      Submit([logging]() { CoroEnv::SetCurLogging(logging); }, i)->Wait();
    }
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