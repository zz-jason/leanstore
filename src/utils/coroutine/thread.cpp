#include "utils/coroutine/thread.hpp"

#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/buffer-manager/page_evictor.hpp"
#include "leanstore/utils/log.hpp"
#include "utils/coroutine/coroutine.hpp"

#include <cassert>
#include <memory>
#include <utility>

namespace leanstore {

Thread::Thread(LeanStore* store, int64_t thread_id) : store_(store), thread_id_(thread_id) {
  LS_DCHECK(store_ != nullptr,
            std::format("Thread created with null store, thread_id={}", thread_id_));

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

void Thread::CreateSysCoros() {
  // System coroutine for page eviction
  if (page_evictor_ != nullptr) {
    sys_tasks_.emplace_back(std::make_unique<Coroutine>([this]() {
      while (keep_running_) {
        page_evictor_->Run4Partitions(eviction_pending_partitions_);
        eviction_pending_partitions_.clear();
        CurrentCoro()->Yield(CoroState::kRunning);
      }
    }));
  }

  // System coroutine for IO polling
  sys_tasks_.emplace_back(std::make_unique<Coroutine>([this]() {
    while (keep_running_) {
      coro_io_.Poll();
      CurrentCoro()->Yield(CoroState::kRunning);
    }
  }));
}

Thread::~Thread() {
  Stop();
  if (thread_.joinable()) {
    thread_.join();
  }
}

void Thread::ThreadLoop() {
  constexpr int kCoroutineRunsLimit = 1;
  std::unique_ptr<Coroutine> coroutine{nullptr};
  int user_task_runs = 0;

  while (keep_running_) {
    user_task_runs = 0;
    while (user_task_runs < kCoroutineRunsLimit && PopFront(coroutine)) {
      // Mutex is not available, push back to the queue
      if (coroutine->GetState() == CoroState::kWaitingMutex && !coroutine->TryLock()) {
        PushBack(std::move(coroutine));
        break;
      }

      if (coroutine->GetState() == CoroState::kWaitingIo && !coroutine->IsIoCompleted()) {
        PushBack(std::move(coroutine));
        break;
      }

      // Run the coroutine
      RunCoroutine(coroutine.get());

      // Check coroutine status after ThreadLoop is resuming
      assert(coroutine->IsWaiting() || coroutine->IsDone());
      if (coroutine->IsWaiting()) {
        PushBack(std::move(coroutine));
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