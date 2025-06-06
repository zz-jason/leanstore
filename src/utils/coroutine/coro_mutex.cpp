#include "utils/coroutine/coro_mutex.hpp"

#include "utils/coroutine/thread.hpp"

#include <cassert>

namespace leanstore {

void CoroMutex::Lock() {
  if (!TryLock()) {
    auto* current_coro = Thread::CurrentCoroutine();
    current_coro->SetWaitingMutex(this);
    current_coro->Yield(CoroState::kWaitingMutex);

    // resume after the mutex is available
    current_coro->SetWaitingMutex(nullptr);
  }
}

void CoroMutex::Unlock() {
  assert(lock_flag_.test_and_set(std::memory_order_acquire) &&
         "Mutex must be locked before unlocking");
  lock_flag_.clear(std::memory_order_release);
}

} // namespace leanstore