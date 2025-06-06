#include "utils/coroutine/thread.hpp"

#include "utils/coroutine/coroutine.hpp"

#include <cassert>
#include <memory>

namespace leanstore {

void Thread::ThreadLoop() {
  constexpr int kCoroutineRunsLimit = 1;
  std::unique_ptr<Coroutine> coroutine{nullptr};
  int coroutine_runs = 0;

  while (keep_running_) {
    coroutine_runs = 0;
    while (coroutine_runs < kCoroutineRunsLimit && PopFront(coroutine)) {
      // Mutex is not available, push back to the queue
      if (coroutine->GetState() == CoroState::kWaitingMutex &&
          !coroutine->GetWaitingMutex()->TryLock()) {
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

      coroutine_runs++;
    }
  }
}

} // namespace leanstore