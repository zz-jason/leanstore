#pragma once

#include "utils/coroutine/blocking_queue_mpsc.hpp"
#include "utils/coroutine/coroutine.hpp"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <format>
#include <memory>
#include <thread>

#include <pthread.h>
#include <unistd.h>

namespace leanstore {

class Thread {
public:
  static constexpr int64_t kMaxCoroutinesPerThread = 256;

  Thread(int64_t thread_id = -1) : thread_id_(thread_id) {
  }

  ~Thread() {
    Stop();
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
    coroutine_queue_.Shutdown();
  }

  void Join() {
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  void PushBack(std::unique_ptr<Coroutine> coroutine) {
    coroutine_queue_.PushBack(std::move(coroutine));
  }

  bool PopFront(std::unique_ptr<Coroutine>& coroutine) {
    return coroutine_queue_.PopFront(coroutine);
  }

  void RunCoroutine(Coroutine* coroutine) {
    assert(coroutine != nullptr);
    current_coroutine_ = coroutine;
    coroutine->Run();
    current_coroutine_ = nullptr;
  }

  static Thread* CurrentThread() {
    assert(s_current_thread != nullptr);
    return s_current_thread;
  }

  static Coroutine* CurrentCoroutine() {
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
  BlockingQueueMpsc<std::unique_ptr<Coroutine>> coroutine_queue_{kMaxCoroutinesPerThread};

  std::thread thread_;

  int64_t thread_id_ = -1;

  std::atomic<bool> keep_running_{false};

  std::atomic<bool> ready_{false};

  inline static thread_local Thread* s_current_thread = nullptr;
};

} // namespace leanstore