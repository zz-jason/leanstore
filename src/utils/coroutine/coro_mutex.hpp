#pragma once

#include "leanstore/utils/log.hpp"

#include <atomic>
#include <cassert>
#include <cstdint>

#include <emmintrin.h>

namespace leanstore {

// -----------------------------------------------------------------------------
// CoroMutex
// -----------------------------------------------------------------------------

/// Similar to std::mutex, but designed for use in coroutine environments. It
/// yields the current coroutine when the mutex is held by another coroutine,
/// preventing deadlocks and allowing other coroutines to run while waiting for
/// the mutex to become available.
class CoroMutex {
public:
  /// Tries to acquire the mutex without blocking.
  /// Returns true if the mutex was acquired successfully, false otherwise.
  bool TryLock() {
    return !lock_flag_.test_and_set(std::memory_order_acquire);
  }

  /// Locks the mutex, yield the current coroutine if the mutex is already held
  /// by another coroutine.
  void Lock();

  /// Unlocks the mutex, allowing other coroutines to acquire it.
  /// It is an error to unlock a mutex that is not held by the current coroutine.
  void Unlock() {
    assert(lock_flag_.test_and_set(std::memory_order_acquire) &&
           "Mutex must be locked before unlocking");
    lock_flag_.clear(std::memory_order_release);
  }

private:
  /// Exclusive lock flag. Indicates if the mutex is held exclusively.
  std::atomic_flag lock_flag_{ATOMIC_FLAG_INIT};
};

// -----------------------------------------------------------------------------
// CoroSharedMutex
// -----------------------------------------------------------------------------

/// Similar to std::shared_mutex, but designed for use in coroutine
/// environments. It allows multiple coroutines to hold a shared lock while
/// yielding the current coroutine when an exclusive lock is held by another
/// coroutine.
class CoroSharedMutex {
public:
  /// Tries to acquire an exclusive lock without blocking.
  /// Returns true if the exclusive lock was acquired successfully, false
  /// otherwise.
  bool TryLock() {
    int64_t expected = kUnlocked;
    return state_.compare_exchange_strong(expected, kLockedExclusively, std::memory_order_acquire);
  }

  /// Locks the mutex exclusively, yielding the current coroutine if the
  /// mutex is already held by another coroutine.
  void Lock();

  /// Unlocks the mutex, allowing other coroutines to acquire it.
  ///
  /// Used together with TryLock(), Lock().
  void Unlock() {
    assert(state_ == kLockedExclusively && "Exclusive lock must be held to unlock");
    state_.store(kUnlocked, std::memory_order_release);
  }

  /// Tries to acquire a shared lock without blocking.
  /// Returns true if the shared lock was acquired successfully, false otherwise.
  bool TryLockShared() {
    int64_t expected = kUnlocked;
    while (!state_.compare_exchange_strong(expected, expected + 1, std::memory_order_acquire)) {
      if (expected == kLockedExclusively) {
        return false; // Cannot acquire shared lock if exclusive lock is held
      }
    }
    return true;
  }

  /// Locks the mutex in shared mode, yielding the current coroutine if the
  /// mutex is already held exclusively by another coroutine. This allows
  /// multiple coroutines to hold a shared lock concurrently.
  void LockShared();

  /// Unlocks the mutex from shared mode, allowing other coroutines to
  /// acquire it in shared mode or exclusively.
  ///
  /// Used together with TryLockShared(), LockShared().
  void UnlockShared() {
    assert(state_ != kLockedExclusively && "Cannot unlock shared on exclusive lock");
    state_.fetch_add(-1, std::memory_order_release);
  }

private:
  static constexpr int64_t kLockedExclusively = 1LL << 63;
  static constexpr int64_t kUnlocked = 0LL;

  std::atomic<int64_t> state_{kUnlocked};
};

// -----------------------------------------------------------------------------
// CoroHybridMutex
// -----------------------------------------------------------------------------

class alignas(64) CoroHybridMutex {
public:
  CoroHybridMutex(uint64_t version = 0) : version_(version) {
  }

  void LockExclusively() {
    mutex_.Lock();
    version_.fetch_add(kLatchExclusiveBit, std::memory_order_release);
    LS_DCHECK(IsLockedExclusively());
  }

  void UnlockExclusively() {
    LS_DCHECK(IsLockedExclusively());
    version_.fetch_add(kLatchExclusiveBit, std::memory_order_release);
    mutex_.Unlock();
  }

  uint64_t GetOptimisticVersion() {
    return version_.load();
  }

  bool IsLockedExclusively() {
    return (version_.load() & kLatchExclusiveBit) == kLatchExclusiveBit;
  }

private:
  constexpr static uint64_t kLatchExclusiveBit = 1ull;

  /// The optimistic version.
  std::atomic<uint64_t> version_ = 0;

  /// The pessimistic shared mutex.
  CoroSharedMutex mutex_;
};

} // namespace leanstore