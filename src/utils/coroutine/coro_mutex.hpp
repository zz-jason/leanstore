#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>

#include <emmintrin.h>

namespace leanstore {

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
  void Unlock();

private:
  /// Exclusive lock flag. Indicates if the mutex is held exclusively.
  std::atomic_flag lock_flag_{ATOMIC_FLAG_INIT};
};

class CoroSharedMutex {
public:
  bool TryLock() {
    int64_t expected = 0;
    int64_t desired = kExclusiveBit;
    return lock_.compare_exchange_strong(expected, desired, std::memory_order_acquire);
  }

  void Lock() {
    int64_t spin = 0;
    while (!TryLock()) {
      spin++;
      if (spin > 40) {
        // jumpmu::jump(leanstore::UserJumpReason::Lock); // Uncomment if using jumpmu
      }
      _mm_pause();
      _mm_pause();
      _mm_pause();
    }
  }

  void Unlock() {
    assert(lock_ == kExclusiveBit && "Exclusive lock must be held to unlock");
    lock_.store(0, std::memory_order_release);
  }

  bool TryLockShared() {
    int64_t expected = 0;
    while (!lock_.compare_exchange_strong(expected, expected + 1, std::memory_order_acquire)) {
      if (expected == kExclusiveBit) {
        return false; // Cannot acquire shared lock if exclusive lock is held
      }
    }
    return true;
  }

  void LockShared() {
    while (!TryLockShared()) {
      _mm_pause();
      _mm_pause();
      _mm_pause();
    }
  }

  void UnlockShared() {
    assert(lock_ != kExclusiveBit && "Cannot unlock shared on exclusive lock");
    lock_.fetch_add(-1, std::memory_order_release);
  }

private:
  static constexpr int64_t kExclusiveBit = 1LL << 63;

  std::atomic<int64_t> lock_{0};
};

} // namespace leanstore