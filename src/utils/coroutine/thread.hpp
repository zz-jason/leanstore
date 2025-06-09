#pragma once

#include "utils/coroutine/blocking_queue_mpsc.hpp"
#include "utils/coroutine/coro_io.hpp"
#include "utils/coroutine/coroutine.hpp"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <format>
#include <memory>
#include <thread>
#include <vector>

#include <pthread.h>
#include <unistd.h>

namespace leanstore {

class Thread {
public:
  static constexpr int64_t kMaxCoroutinesPerThread = 256;

  Thread(int64_t thread_id = -1) : thread_id_(thread_id) {
    // Create a system task for IO polling
    sys_tasks_.emplace_back(std::make_unique<Coroutine>([this]() {
      while (keep_running_) {
        coro_io_.Poll();
        CurrentCoro()->Yield(CoroState::kRunning);
      }
    }));
  }

  ~Thread() {
    Stop();
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  Thread(const Thread&) = delete;
  Thread& operator=(const Thread&) = delete;
  Thread(Thread&&) = delete;
  Thread& operator=(Thread&&) = delete;

  /// Starts the worker thread.
  void Start() {
    keep_running_ = true;
    thread_ = std::thread(&Thread::ThreadMain, this);
  }

  bool IsReady() {
    return ready_.load(std::memory_order_acquire);
  }

  /// Stops the worker thread.
  void Stop() {
    keep_running_ = false;
    user_task_queue_.Shutdown();
  }

  void Join() {
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  bool IsRunning() const {
    return keep_running_.load(std::memory_order_acquire);
  }

  void PushBack(std::unique_ptr<Coroutine> coroutine) {
    user_task_queue_.PushBack(std::move(coroutine));
  }

  bool PopFront(std::unique_ptr<Coroutine>& coroutine) {
    return user_task_queue_.PopFront(coroutine);
  }

  void RunCoroutine(Coroutine* coroutine) {
    assert(coroutine != nullptr);
    current_coroutine_ = coroutine;
    coroutine->Run();
    current_coroutine_ = nullptr;
  }

  static CoroIo* CurrentCoroIo() {
    return &CurrentThread()->coro_io_;
  }

  static Thread* CurrentThread() {
    assert(s_current_thread != nullptr);
    return s_current_thread;
  }

  static Coroutine* CurrentCoro() {
    return CurrentThread()->current_coroutine_;
  }

private:
  void ThreadMain() {
    ThreadInit();
    ThreadLoop();
  }

  void ThreadInit() {
    if (thread_id_ == -1) {
      static std::atomic<int64_t> thread_count{0};
      thread_id_ = thread_count.fetch_add(1, std::memory_order_relaxed);
    }
    std::string thread_name = std::format("worker_{}", thread_id_);
    pthread_setname_np(pthread_self(), thread_name.c_str());
    s_current_thread = this;
    ready_ = true;
  }

  void ThreadLoop();

private:
  /// Pointer to the currently running coroutine.
  Coroutine* current_coroutine_ = nullptr;

  /// Ring buffer to hold coroutines that are ready to run.
  BlockingQueueMpsc<std::unique_ptr<Coroutine>> user_task_queue_{kMaxCoroutinesPerThread};

  /// Task queue for system tasks, e.g. IO operations.
  std::vector<std::unique_ptr<Coroutine>> sys_tasks_;

  CoroIo coro_io_{kMaxCoroutinesPerThread};

  std::thread thread_;

  int64_t thread_id_ = -1;

  std::atomic<bool> keep_running_{false};

  std::atomic<bool> ready_{false};

  inline static thread_local Thread* s_current_thread = nullptr;
};

} // namespace leanstore