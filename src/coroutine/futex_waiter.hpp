#pragma once

#include <atomic>
#include <cassert>
#include <cerrno>

#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace leanstore {

/// Futex-based waiter for coroutine synchronization. Uses Linux futex system calls
/// for efficient blocking and waking of coroutines.
class FutexWaiter {
public:
  /// Constructor initializing the futex value.
  explicit FutexWaiter(int initial = 0) : value_(initial) {
  }

  /// Sets the futex value.
  void store(int v) { // NOLINT, mimics std::atomic store
    value_.store(v, std::memory_order_release);
  }

  /// Gets the futex value.
  int load() const { // NOLINT, mimics std::atomic load
    return value_.load(std::memory_order_acquire);
  }

  /// Blocking wait until value_ != expected.
  void wait(int expected) { // NOLINT
    while (value_.load(std::memory_order_acquire) == expected) {
      syscall(SYS_futex, reinterpret_cast<int*>(&value_), FUTEX_WAIT, expected, NULL, NULL, 0);
    }
  }

  /// Notify one waiting thread.
  void notify_one() { // NOLINT, mimics std::condition_variable
    syscall(SYS_futex, reinterpret_cast<int*>(&value_), FUTEX_WAKE, 1, NULL, NULL, 0);
  }

  /// Notify all waiting threads.
  void notify_all() { // NOLINT, mimics std::condition_variable
    syscall(SYS_futex, reinterpret_cast<int*>(&value_), FUTEX_WAKE, INT_MAX, NULL, NULL, 0);
  }

private:
  /// The atomic integer value used for futex operations.
  std::atomic<int> value_;
};

} // namespace leanstore