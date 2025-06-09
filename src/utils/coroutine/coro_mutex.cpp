#include "utils/coroutine/coro_mutex.hpp"

#include "utils/coroutine/thread.hpp"

#include <cassert>

namespace leanstore {

// -----------------------------------------------------------------------------
// CoroMutex
// -----------------------------------------------------------------------------

void CoroMutex::Lock() {
  if (!TryLock()) {
    auto* current_coro = Thread::CurrentCoro();
    current_coro->SetTryLockFunc([this]() { return this->TryLock(); });
    current_coro->Yield(CoroState::kWaitingMutex);

    // The current coroutine only resumes if the TryLockFunc returns true (succeed)
    current_coro->ClearTryLockFunc();
  }
}

// -----------------------------------------------------------------------------
// CoroSharedMutex
// -----------------------------------------------------------------------------

void CoroSharedMutex::Lock() {
  if (!TryLock()) {
    auto* current_coro = Thread::CurrentCoro();
    current_coro->SetTryLockFunc([this]() { return this->TryLock(); });
    current_coro->Yield(CoroState::kWaitingMutex);

    // The current coroutine only resumes if the TryLockFunc returns true (succeed)
    current_coro->ClearTryLockFunc();
  }

  assert(state_ == kLockedExclusively);
}

void CoroSharedMutex::LockShared() {
  if (!TryLockShared()) {
    auto* current_coro = Thread::CurrentCoro();
    current_coro->SetTryLockFunc([this]() { return this->TryLockShared(); });
    current_coro->Yield(CoroState::kWaitingMutex);

    // The current coroutine only resumes if the TryLockFunc returns true (succeed)
    current_coro->ClearTryLockFunc();
  }

  assert(state_ != kLockedExclusively);
}

} // namespace leanstore