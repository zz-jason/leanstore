#pragma once

#include <atomic>
#include <cassert>
#include <cerrno>

#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace leanstore {

class FutexWaiter {
public:
  explicit FutexWaiter(int initial = 0) : value_(initial) {
  }

  /// NOLINTNEXTLINE
  void store(int v) {
    value_.store(v, std::memory_order_release);
  }

  /// NOLINTNEXTLINE
  int load() const {
    return value_.load(std::memory_order_acquire);
  }

  /// Blocking wait until value_ != expected.
  /// NOLINTNEXTLINE
  void wait(int expected) {
    while (value_.load(std::memory_order_acquire) == expected) {
      syscall(SYS_futex, reinterpret_cast<int*>(&value_), FUTEX_WAIT, expected, NULL, NULL, 0);
    }
  }

  /// Notify one waiting thread.
  /// NOLINTNEXTLINE
  void notify_one() {
    syscall(SYS_futex, reinterpret_cast<int*>(&value_), FUTEX_WAKE, 1, NULL, NULL, 0);
  }

  /// Notify all waiting threads.
  /// NOLINTNEXTLINE
  void notify_all() {
    syscall(SYS_futex, reinterpret_cast<int*>(&value_), FUTEX_WAKE, INT_MAX, NULL, NULL, 0);
  }

private:
  std::atomic<int> value_;
};

} // namespace leanstore