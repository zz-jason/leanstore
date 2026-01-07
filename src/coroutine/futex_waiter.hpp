#pragma once

#include <atomic>
#include <cassert>

#if defined(__linux__)
#include <linux/futex.h>
#include <sys/syscall.h>
#endif

namespace leanstore {

/// A lightweight waiter for coroutine synchronization.
/// - For Linux, it uses futex system calls for efficient blocking and waking up.
/// - For other platforms, it falls back to using std::atomic.
class FutexWaiter {
public:
  /// Constructor initializing the futex value.
  explicit FutexWaiter(int initial = 0) : value_(initial) {
  }

  /// Sets the futex value.
  void store(int v) { // NOLINT, mimic std::atomic
    value_.store(v, std::memory_order_release);
  }

  /// Gets the futex value.
  int load() const { // NOLINT, mimic std::atomic
    return value_.load(std::memory_order_acquire);
  }

  /// Blocking wait until value_ != expected.
  void wait(int expected) { // NOLINT, mimic std::atomic
#if defined(__linux__)
    while (value_.load(std::memory_order_acquire) == expected) {
      syscall(SYS_futex, reinterpret_cast<int*>(&value_), FUTEX_WAIT, expected, NULL, NULL, 0);
    }
#else
    value_.wait(expected);
#endif
  }

  /// Notify one waiting thread.
  void notify_one() { // NOLINT, mimic std::atomic
#if defined(__linux__)
    syscall(SYS_futex, reinterpret_cast<int*>(&value_), FUTEX_WAKE, 1, NULL, NULL, 0);
#else
    value_.notify_one();
#endif
  }

  /// Notify all waiting threads.
  void notify_all() { // NOLINT, mimic std::atomic
#if defined(__linux__)
    syscall(SYS_futex, reinterpret_cast<int*>(&value_), FUTEX_WAKE, INT_MAX, NULL, NULL, 0);
#else
    value_.notify_all();
#endif
  }

private:
  /// The atomic integer value used for futex operations.
  std::atomic<int> value_;
};

} // namespace leanstore