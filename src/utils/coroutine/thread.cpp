#include "utils/coroutine/thread.hpp"

#include "utils/coroutine/coroutine.hpp"

#include <cassert>
#include <memory>
#include <utility>

namespace leanstore {

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