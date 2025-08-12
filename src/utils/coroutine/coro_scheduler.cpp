#include "utils/coroutine/coro_scheduler.hpp"

#include "leanstore/concurrency/tx_manager.hpp"
#include "utils/coroutine/auto_commit_protocol.hpp"
#include "utils/coroutine/coro_future.hpp"

#include <filesystem>
#include <format>
#include <memory>
#include <utility>
#include <vector>

namespace leanstore {

CoroScheduler::CoroScheduler(LeanStore* store, int64_t num_threads)
    : store_(store),
      num_threads_(num_threads),
      coro_executors_(num_threads),
      tx_mgrs_() {

  // create commit protocols for each commit group
  auto commit_group_size = store ? store->store_option_->commit_group_size_ : 0;
  for (auto i = 0u; i < commit_group_size; i++) {
    commit_protocols_.emplace_back(
        std::make_unique<AutoCommitProtocol>(i, store->store_option_->worker_threads_));
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

void CoroScheduler::InitWorkerCtxs() {
  if (!store_->store_option_->enable_wal_) {
    Log::Info("Skipping worker context initialization, WAL is disabled");
    return;
  }

  ScopedTimer timer([this](double elapsed_ms) {
    Log::Info("WorkerContexts inited, num_threads={}, elapsed={}ms", num_threads_, elapsed_ms);
  });

  tx_mgrs_.reserve(coro_executors_.size());
  for (auto i = 0u; i < coro_executors_.size(); i++) {
    tx_mgrs_.emplace_back(std::make_unique<cr::TxManager>(i, tx_mgrs_, store_));
  }

  std::vector<std::shared_ptr<CoroFuture<void>>> futures;
  for (auto i = 0u; i < coro_executors_.size(); i++) {
    auto coro_job = [i, this]() { cr::TxManager::s_tls_tx_manager = tx_mgrs_[i].get(); };
    futures.emplace_back(Submit(std::move(coro_job), i));
  }
  for (auto& future : futures) {
    future->Wait();
  }
}

void CoroScheduler::InitLogging() {
  if (!store_->store_option_->enable_wal_) {
    Log::Info("Skipping logging initialization, WAL is disabled");
    return;
  }

  std::string wal_dir = store_->GetWalDir();
  ScopedTimer timer([&](double elapsed_ms) {
    Log::Info("Logging inited, num_threads={}, dir={}, elapsed={}ms", num_threads_, wal_dir,
              elapsed_ms);
  });

  // create wal dir if not exists
  if (!std::filesystem::exists(wal_dir)) {
    std::filesystem::create_directories(wal_dir);
    Log::Info("Created WAL directory: {}", wal_dir);
  } else {
    Log::Info("WAL directory already exists: {}", wal_dir);
  }

  std::vector<std::shared_ptr<CoroFuture<void>>> futures;
  for (auto i = 0u; i < coro_executors_.size(); i++) {
    auto logging_init_job = [i, &wal_dir]() {
      std::string file_name = std::format(CoroExecutor::kCoroExecNamePattern, i);
      std::string file_path = std::format("{}/{}.wal", wal_dir, file_name);
      cr::TxManager::My().GetLogging().InitWalFd(file_path);
    };
    futures.emplace_back(Submit(std::move(logging_init_job), i));
  }
  for (auto& future : futures) {
    future->Wait();
  }
}

} // namespace leanstore