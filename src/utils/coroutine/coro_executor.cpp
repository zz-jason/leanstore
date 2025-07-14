#include "utils/coroutine/coro_executor.hpp"

#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/buffer-manager/page_evictor.hpp"
#include "leanstore/utils/log.hpp"
#include "utils/coroutine/coroutine.hpp"

#include <cassert>
#include <memory>
#include <utility>

namespace leanstore {

CoroExecutor::CoroExecutor(LeanStore* store, int64_t thread_id)
    : store_(store),
      thread_id_(thread_id) {
  if (store_ != nullptr) {
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
  constexpr int kCoroutineRunsLimit = 1;
  std::unique_ptr<Coroutine> coro{nullptr};
  int user_task_runs = 0;

  while (keep_running_) {
    user_task_runs = 0;
    while (user_task_runs < kCoroutineRunsLimit) {
      DequeueCoro(coro);

      // Mutex is not available, push back to the queue
      if (coro->GetState() == CoroState::kWaitingMutex) {
        if (!coro->TryLock()) {
          EnqueueCoro(std::move(coro));
          break;
        }
      }

      if (coro->GetState() == CoroState::kWaitingIo && !coro->IsIoCompleted()) {
        EnqueueCoro(std::move(coro));
        break;
      }

      // Run the coro
      RunCoroutine(coro.get());

      // Check coro status after ThreadLoop is resuming
      assert(coro->IsWaiting() || coro->IsDone());
      if (coro->IsWaiting()) {
        EnqueueCoro(std::move(coro));
      }

      user_task_runs++;
    }

    // Run system tasks if any
    for (auto& sys_task : sys_tasks_) {
      RunCoroutine(sys_task.get());
    }
  }
}

} // namespace leanstore