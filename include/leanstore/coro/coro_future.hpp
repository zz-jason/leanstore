#pragma once

#include "leanstore/coro/futex_waiter.hpp"

#include <cassert>

#include <linux/futex.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <unistd.h>

namespace leanstore {

template <typename T>
class CoroFuture {
public:
  CoroFuture() = default;
  ~CoroFuture() = default;

  // Disable copy and move semantics
  CoroFuture(const CoroFuture&) = delete;
  CoroFuture& operator=(const CoroFuture&) = delete;
  CoroFuture(CoroFuture&&) = delete;
  CoroFuture& operator=(CoroFuture&&) = delete;

  /// Wait for the future to be ready, blocks until the result is set.
  void Wait() {
    waiter_.wait(kNotReady);
  }

  /// Get the result of the future, should be called after Wait().
  T& GetResult() {
    return result_;
  }

  /// Set the result of the future, should be called by the Coroutine.
  void SetResult(T&& value) {
    result_ = std::move(value);
    waiter_.store(kReady);
    waiter_.notify_one();
  }

private:
  static constexpr auto kNotReady = 0;
  static constexpr auto kReady = 1;

  T result_;
  FutexWaiter waiter_{kNotReady};
};

template <>
class CoroFuture<void> {
public:
  CoroFuture() = default;
  ~CoroFuture() = default;

  // Disable copy and move semantics
  CoroFuture(const CoroFuture&) = delete;
  CoroFuture& operator=(const CoroFuture&) = delete;
  CoroFuture(CoroFuture&&) = delete;
  CoroFuture& operator=(CoroFuture&&) = delete;

  /// Wait for the future to be ready, blocks until the result is set.
  void Wait() {
    waiter_.wait(kNotReady);
  }

  /// Get the result of the future, should be called after Wait().
  /// No result to return for void futures.
  void GetResult() {
  }

  /// Set the result of the future, should be called by the Coroutine.
  void SetResult() {
    waiter_.store(kReady);
    waiter_.notify_one();
  }

private:
  static constexpr auto kNotReady = 0;
  static constexpr auto kReady = 1;

  FutexWaiter waiter_{kNotReady};
};

} //  namespace leanstore