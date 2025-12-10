#include "coroutine/coro_mutex.hpp"

#include "coroutine/coro_executor.hpp"
#include "coroutine/coroutine.hpp"

#include <cassert>

namespace leanstore {

// -----------------------------------------------------------------------------
// CoroMutex
// -----------------------------------------------------------------------------

void CoroMutex::lock() {
  if (!try_lock()) {
    auto* current_coro = CoroExecutor::CurrentCoro();
    current_coro->SetTryLockFunc([this]() { return this->try_lock(); });
    current_coro->Yield(CoroState::kWaitingMutex);

    // The current coroutine only resumes if the TryLockFunc returns true (succeed)
    current_coro->ClearTryLockFunc();
  }
}

// -----------------------------------------------------------------------------
// CoroSharedMutex
// -----------------------------------------------------------------------------

void CoroSharedMutex::lock() {
  if (!try_lock()) {
    auto* current_coro = CoroExecutor::CurrentCoro();
    current_coro->SetTryLockFunc([this]() { return this->try_lock(); });
    current_coro->Yield(CoroState::kWaitingMutex);

    // The current coroutine only resumes if the TryLockFunc returns true (succeed)
    current_coro->ClearTryLockFunc();
  }

  assert(state_ == kLockedExclusively);
}

void CoroSharedMutex::lock_shared() {
  if (!try_lock_shared()) {
    auto* current_coro = CoroExecutor::CurrentCoro();
    current_coro->SetTryLockFunc([this]() { return this->try_lock_shared(); });
    current_coro->Yield(CoroState::kWaitingMutex);

    // The current coroutine only resumes if the TryLockFunc returns true (succeed)
    current_coro->ClearTryLockFunc();
  }

  assert(state_ != kLockedExclusively);
}

} // namespace leanstore