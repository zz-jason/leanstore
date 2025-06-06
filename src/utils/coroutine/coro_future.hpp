#pragma once

#include <atomic>

namespace leanstore {

class Coroutine;
class CoroScheduler;

template <typename T>
class CoroFuture {
public:
  CoroFuture() = default;

  // Disable copy and move semantics
  CoroFuture(const CoroFuture&) = delete;
  CoroFuture& operator=(const CoroFuture&) = delete;
  CoroFuture(CoroFuture&&) = delete;
  CoroFuture& operator=(CoroFuture&&) = delete;

  /// Destructor
  ~CoroFuture() = default;

  /// Wait for the future to be ready, blocks until the result is set.
  void Wait() {
    ready_.wait(false);
  }

  /// Get the result of the future, should be called after Wait().
  T& GetResult() {
    return result_;
  }

private:
  /// Set the result of the future, should be called by the Coroutine.
  void SetResult(T&& value) {
    result_ = std::move(value);
    ready_.store(true, std::memory_order_release);
    ready_.notify_one();
  }

  friend class CoroScheduler;

  std::atomic<bool> ready_{false};
  T result_;
};

template <>
class CoroFuture<void> {
public:
  CoroFuture() = default;

  // Disable copy and move semantics
  CoroFuture(const CoroFuture&) = delete;
  CoroFuture& operator=(const CoroFuture&) = delete;
  CoroFuture(CoroFuture&&) = delete;
  CoroFuture& operator=(CoroFuture&&) = delete;

  /// Destructor
  ~CoroFuture() = default;

  /// Wait for the future to be ready, blocks until the result is set.
  void Wait() {
    ready_.wait(false);
  }

  /// Get the result of the future, should be called after Wait().
  void GetResult() {
    // No result to return for void futures
  }

private:
  /// Set the result of the future, should be called by the Coroutine.
  void SetResult() {
    ready_.store(true, std::memory_order_release);
    ready_.notify_one();
  }

  friend class CoroScheduler;

  std::atomic<bool> ready_{false};
};

} //  namespace leanstore