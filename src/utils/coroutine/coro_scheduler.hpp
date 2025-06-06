#pragma once

#include "coro_future.hpp"
#include "thread.hpp"

#include <cassert>
#include <memory>
#include <type_traits>
#include <vector>

namespace leanstore {

/// The CoroScheduler is expected to be run on user threads, where each thread
/// can submit coroutines to be executed. It manages a pool of threads, each
/// capable of running coroutines concurrently. The scheduler ensures that
/// coroutines are executed in a non-blocking manner, allowing for efficient
/// parallel execution of user tasks.
class CoroScheduler {
public:
  CoroScheduler(int64_t num_threads) : num_threads_(num_threads), threads_(num_threads) {
    assert(num_threads > 0 && "Number of threads must be greater than zero");
    for (int64_t i = 0; i < num_threads; ++i) {
      threads_[i] = std::make_unique<Thread>(i);
    }
  }

  ~CoroScheduler() = default;

  CoroScheduler(const CoroScheduler&) = delete;
  CoroScheduler& operator=(const CoroScheduler&) = delete;
  CoroScheduler(CoroScheduler&&) = delete;
  CoroScheduler& operator=(CoroScheduler&&) = delete;

  void Init() {
    // Start all threads
    for (auto& thread : threads_) {
      thread->Start();
    }

    // Wait for all threads to be ready
    for (auto& thread : threads_) {
      while (!thread->IsReady()) {
      }
    }
  }

  void Deinit() {
    // Stop all threads
    for (auto& thread : threads_) {
      thread->Stop();
    }

    // Wait for all threads to finish
    for (auto& thread : threads_) {
      thread->Join();
    }
  }

  template <typename F, typename R = std::invoke_result_t<F>>
  std::shared_ptr<CoroFuture<R>> Submit(F&& coro_func, int64_t thread_id = -1) {
    if (thread_id < 0 || thread_id >= num_threads_) {
      // Default to the first thread if not specified
      thread_id = 0;
    }

    auto coro_future = std::make_shared<CoroFuture<R>>();
    auto coro = std::make_unique<Coroutine>(
        [future = coro_future, f = std::forward<F>(coro_func)]() mutable {
          if constexpr (std::is_void_v<R>) {
            f();
            future->SetResult();
          } else {
            R result = f();
            future->SetResult(std::move(result));
          }
        });

    threads_[thread_id]->PushBack(std::move(coro));
    return coro_future;
  }

private:
  const int64_t num_threads_;
  std::vector<std::unique_ptr<Thread>> threads_;
};

} // namespace leanstore