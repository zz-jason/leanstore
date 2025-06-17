#pragma once

#include "leanstore/utils/portable.hpp"
#include "utils/coroutine/coroutine.hpp"
#include "utils/coroutine/thread.hpp"

#include <atomic>
#include <cassert>
#include <cstdint>

#include <emmintrin.h>
#include <sys/types.h>

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
  // NOLINTBEGIN

  /// Tries to acquire the mutex without blocking.
  /// Returns true if the mutex was acquired successfully, false otherwise.
  bool try_lock() {
    return !lock_flag_.test_and_set(std::memory_order_acquire);
  }

  /// Locks the mutex, yield the current coroutine if the mutex is already held
  /// by another coroutine.
  void lock();

  /// Unlocks the mutex, allowing other coroutines to acquire it.
  /// It is an error to unlock a mutex that is not held by the current coroutine.
  void unlock() {
    assert(lock_flag_.test_and_set(std::memory_order_acquire) &&
           "Mutex must be locked before unlocking");
    lock_flag_.clear(std::memory_order_release);
  }

  // NOLINTEND

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
  // NOLINTBEGIN

  /// Tries to acquire an exclusive lock without blocking.
  /// Returns true if the exclusive lock was acquired successfully, false
  /// otherwise.
  bool try_lock() {
    int64_t expected = kUnlocked;
    return state_.compare_exchange_strong(expected, kLockedExclusively, std::memory_order_acquire);
  }

  /// Locks the mutex exclusively, yielding the current coroutine if the
  /// mutex is already held by another coroutine.
  void lock();

  /// Unlocks the mutex, allowing other coroutines to acquire it.
  ///
  /// Used together with try_lock(), lock().
  void unlock() {
    assert(state_ == kLockedExclusively && "Exclusive lock must be held to unlock");
    state_.store(kUnlocked, std::memory_order_release);
  }

  /// Tries to acquire a shared lock without blocking.
  /// Returns true if the shared lock was acquired successfully, false otherwise.
  bool try_lock_shared() {
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
  void lock_shared();

  /// Unlocks the mutex from shared mode, allowing other coroutines to
  /// acquire it in shared mode or exclusively.
  ///
  /// Used together with try_lock_shared(), lock_shared().
  void unlock_shared() {
    assert(state_ != kLockedExclusively && "Cannot unlock shared on exclusive lock");
    state_.fetch_add(-1, std::memory_order_release);
  }

  // NOLINTEND

private:
  static constexpr int64_t kLockedExclusively = 1LL << 63;
  static constexpr int64_t kUnlocked = 0LL;

  std::atomic<int64_t> state_{kUnlocked};
};

// -----------------------------------------------------------------------------
// CoroHybridMutex
// -----------------------------------------------------------------------------

/// A hybrid mutex that combines optimistic and pessimistic locking strategies.
/// All the APIs are designed to be used together with CoroHybridLockGuard.
class ALIGNAS(64) CoroHybridMutex {
public:
  constexpr static uint64_t kLatchExclusiveBit = 1ull;

  CoroHybridMutex() = default;

  /// Lock the mutex exclusively.
  /// Returns the new version after locking.
  uint64_t Lock() {
    mutex_.lock();

    auto old_version = version_.load(std::memory_order_acquire);
    auto new_version = old_version + kLatchExclusiveBit;
    version_.store(new_version, std::memory_order_release);
    assert((new_version & kLatchExclusiveBit) != 0);

    return new_version;
  }

  /// Unlock the mutex exclusively with the given old version, which must match
  /// the version when the mutex was locked.
  /// Returns the new version after unlocking.
  uint64_t Unlock(uint64_t old_version) {
    assert(old_version == version_.load(std::memory_order_acquire));
    auto new_version = old_version + kLatchExclusiveBit;
    version_.store(new_version, std::memory_order_release);
    assert((new_version & kLatchExclusiveBit) == 0);

    mutex_.unlock();
    return new_version;
  }

  uint64_t LockSharedOptimistic() {
    auto version_on_lock = GetVersion();
    while (version_on_lock & kLatchExclusiveBit) {
      Thread::CurrentCoro()->Yield(CoroState::kRunning);
      version_on_lock = GetVersion();
    }
    return version_on_lock;
  }

  /// Lock the mutex in shared mode.
  /// Returns the current version of the mutex after acquiring the shared lock.
  uint64_t LockShared() {
    mutex_.lock_shared();
    return version_.load(std::memory_order_acquire);
  }

  /// Unlock the mutex from shared mode.
  void UnlockShared() {
    mutex_.unlock_shared();
  }

  /// Get the current version of the mutex.
  uint64_t GetVersion() {
    return version_.load(std::memory_order_acquire);
  }

private:
  /// The optimistic version.
  std::atomic<uint64_t> version_ = 0;

  /// The pessimistic shared mutex.
  CoroSharedMutex mutex_;
};

} // namespace leanstore