#pragma once

#include "coroutine/blocking_queue_mpsc.hpp"
#include "coroutine/coro_future.hpp"

#include <atomic>
#include <cstdint>
#include <format>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

namespace leanstore {

class TaskExecutor {
public:
  explicit TaskExecutor(uint64_t workers, uint64_t worker_task_limit,
                        std::function<void(uint64_t)> custom_thread_init = nullptr,
                        std::function<void(uint64_t)> custom_thread_deinit = nullptr)
      : keep_running_(false),
        started_threads_(0),
        custom_thread_init_(std::move(custom_thread_init)),
        custom_thread_deinit_(std::move(custom_thread_deinit)),
        thds_(workers),
        queues_(workers) {
    for (auto i = 0U; i < queues_.size(); i++) {
      queues_[i] = std::make_unique<BlockingQueueMpsc<std::function<void()>>>(worker_task_limit);
    }
  }

  ~TaskExecutor() {
    Stop();
  }

  void Start() {
    keep_running_ = true;
    for (auto i = 0U; i < thds_.size(); i++) {
      thds_[i] = std::make_unique<std::thread>(&TaskExecutor::ThreadMain, this, i);
    }
    while (started_threads_.load() < thds_.size()) {
      std::this_thread::yield();
    }
  }

  void Stop() {
    keep_running_.store(false);
    for (auto& queue : queues_) {
      if (queue) {
        queue->Shutdown();
      }
    }
    for (auto& thd : thds_) {
      if (thd && thd->joinable()) {
        thd->join();
      }
    }
  }

  /// Submit a task to be executed on a specific worker thread.
  template <typename F, typename... Args>
  auto SubmitOn(uint64_t wid, F&& task, Args&&... args)
      -> std::shared_ptr<CoroFuture<std::invoke_result_t<F, Args...>>> {
    assert(wid < thds_.size() && "Invalid worker id for task submission");
    using R = std::invoke_result_t<F, Args...>;

    // create a future to hold the result
    auto task_future = std::make_shared<CoroFuture<R>>();

    // wrap task to std::function<void()> and push to the worker queue
    queues_[wid]->PushBack([future = task_future, f = std::forward<F>(task),
                            args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
      if constexpr (std::is_void_v<R>) {
        std::apply(f, args);
        future->SetResult();
      } else {
        R result = std::apply(f, args);
        future->SetResult(std::move(result));
      }
    });
    return task_future;
  }

protected:
  void ThreadMain(uint64_t wid) {
    std::string thread_name = std::format("TaskExec-{}", wid);
    pthread_setname_np(pthread_self(), thread_name.c_str());

    if (custom_thread_init_ != nullptr) {
      custom_thread_init_(wid);
    }

    started_threads_.fetch_add(1);

    while (keep_running_) {
      std::function<void()> task;
      if (!queues_[wid]->PopFront(task)) {
        continue;
      }
      task();
    }

    if (custom_thread_deinit_ != nullptr) {
      custom_thread_deinit_(wid);
    }
  }

private:
  std::atomic<bool> keep_running_;
  std::atomic<uint64_t> started_threads_;
  std::function<void(uint64_t)> custom_thread_init_;
  std::function<void(uint64_t)> custom_thread_deinit_;
  std::vector<std::unique_ptr<std::thread>> thds_;
  std::vector<std::unique_ptr<BlockingQueueMpsc<std::function<void()>>>> queues_;
};

} // namespace leanstore