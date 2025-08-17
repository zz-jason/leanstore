#include "utils/coroutine/coro_executor.hpp"

#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/buffer-manager/page_evictor.hpp"
#include "leanstore/utils/log.hpp"
#include "utils/coroutine/auto_commit_protocol.hpp"
#include "utils/coroutine/coroutine.hpp"

#include <cassert>
#include <format>
#include <memory>
#include <utility>

namespace leanstore {

CoroExecutor::CoroExecutor(LeanStore* store, AutoCommitProtocol* commit_protocol, int64_t thread_id)
    : store_(store),
      commit_protocol_(commit_protocol),
      thread_id_(thread_id) {
  if (store_ != nullptr) {
    // init page evictor
    auto* buffer_manager = store_->buffer_manager_.get();
    page_evictor_ = std::make_unique<leanstore::storage::PageEvictor>(
        store_, "PageEvictor", 0, buffer_manager->num_bfs_, buffer_manager->buffer_pool_,
        buffer_manager->num_partitions_, buffer_manager->partitions_);
  } else {
    page_evictor_ = nullptr;
  }

  CreateSysCoros();
}

void CoroExecutor::CreateSysCoros() {
  // System coroutine for autonomous transaction commit
  if (commit_protocol_ != nullptr) {
    auto sys_coro = std::make_unique<Coroutine>([this]() {
      while (keep_running_) {
        commit_protocol_->Run();
        CurrentCoro()->Yield(CoroState::kRunning);
      }
    });
    Log::Info("Creating system coroutine for transaction commit");
    sys_tasks_.emplace_back(std::move(sys_coro));
  }

  // System coroutine for page eviction
  if (page_evictor_ != nullptr) {
    auto sys_coro = std::make_unique<Coroutine>([this]() {
      while (keep_running_) {
        page_evictor_->Run4Partitions(eviction_pending_partitions_);
        eviction_pending_partitions_.clear();
        CurrentCoro()->Yield(CoroState::kRunning);
      }
    });
    Log::Info("Creating system coroutine for page eviction");
    sys_tasks_.emplace_back(std::move(sys_coro));
  }

  // System coroutine for IO polling
  auto sys_io_coro = std::make_unique<Coroutine>([this]() {
    while (keep_running_) {
      coro_io_.Poll();
      CurrentCoro()->Yield(CoroState::kRunning);
    }
  });
  Log::Info("Creating system coroutine for IO polling");
  sys_tasks_.emplace_back(std::move(sys_io_coro));
}

CoroExecutor::~CoroExecutor() {
  Stop();
  if (thread_.joinable()) {
    thread_.join();
  }
}

void CoroExecutor::ThreadLoop() {
  static constexpr int kCoroutineRunsLimit = 1;
  std::unique_ptr<Coroutine> coro{nullptr};
  int user_coro_runs = 0;
  bool sys_coro_required = false;

  while (keep_running_) {
    DequeueCoro(coro);

    // Shutdown if required
    if (!keep_running_) {
      break;
    }

    // Whether the coroutine is ready to run
    if (IsCoroReadyToRun(coro, sys_coro_required)) {
      RunCoroutine(coro.get());
      user_coro_runs++;
      if (coro->GetState() != CoroState::kDone) {
        EnqueueCoro(std::move(coro));
      }
    }

    // Run system coroutines if needed
    if (sys_coro_required || user_coro_runs >= kCoroutineRunsLimit) {
      RunSystemCoros();
      sys_coro_required = false;
      user_coro_runs = 0;
    }
  }
}

bool CoroExecutor::IsCoroReadyToRun(std::unique_ptr<Coroutine>& coro, bool& sys_coro_required) {
  switch (coro->GetState()) {
  case CoroState::kReady:
  case CoroState::kRunning: {
    return true;
  }
  case CoroState::kWaitingMutex: {
    if (!coro->TryLock()) {
      EnqueueCoro(std::move(coro));
      return false;
    }
    return true;
  }
  case CoroState::kWaitingIo: {
    if (!coro->IsIoCompleted()) {
      EnqueueCoro(std::move(coro));
      sys_coro_required = true;
      return false;
    }
    return true;
  }
  case CoroState::kDone:
  default: {
    LEAN_DCHECK(false, std::format("Un expected coro state: {}", (int)coro->GetState()));
    return true;
  }
  }
  return true;
}

void CoroExecutor::RunSystemCoros() {
  for (auto& sys_task : sys_tasks_) {
    RunCoroutine(sys_task.get());
  }
}

} // namespace leanstore