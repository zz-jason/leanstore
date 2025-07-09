#pragma once

#include "leanstore//utils/jump_mu.hpp"
#include "utils/coroutine/blocking_queue_mpsc.hpp"
#include "utils/coroutine/coro_io.hpp"
#include "utils/coroutine/coroutine.hpp"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <format>
#include <memory>
#include <thread>
#include <unordered_set>
#include <vector>

#include <pthread.h>
#include <unistd.h>

namespace leanstore {

class LeanStore;
namespace storage {
class PageEvictor;
} // namespace storage

class Thread {
public:
  static constexpr int64_t kMaxCoroutinesPerThread = 256;

  Thread(LeanStore* store, int64_t thread_id = -1);

  ~Thread();

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
    assert(coroutine != nullptr && "Coroutine cannot be null");
    assert(!coroutine->IsDone() && "Coroutine must not be done before running");

    current_coroutine_ = coroutine;
    JumpContext::SetCurrent(coroutine->GetJumpContext());

    coroutine->Run();

    current_coroutine_ = nullptr;
    JumpContext::SetCurrent(&def_jump_context_);
  }

  void AddEvictionPendingPartition(uint64_t partition_id) {
    eviction_pending_partitions_.insert(partition_id);
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
  void CreateSysCoros();

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
    JumpContext::SetCurrent(&def_jump_context_);

    ready_ = true;
  }

  void ThreadLoop();

private:
  /// The LeanStore instance.
  LeanStore* store_ = nullptr;

  /// Pointer to the currently running coroutine.
  Coroutine* current_coroutine_ = nullptr;

  /// Ring buffer to hold coroutines that are ready to run.
  BlockingQueueMpsc<std::unique_ptr<Coroutine>> user_task_queue_{kMaxCoroutinesPerThread};

  /// Task queue for system tasks, e.g. IO operations.
  std::vector<std::unique_ptr<Coroutine>> sys_tasks_;

  CoroIo coro_io_{kMaxCoroutinesPerThread};

  std::unordered_set<uint64_t> eviction_pending_partitions_;
  std::unique_ptr<leanstore::storage::PageEvictor> page_evictor_;

  std::thread thread_;

  int64_t thread_id_ = -1;

  std::atomic<bool> keep_running_{false};

  std::atomic<bool> ready_{false};

  /// Jump context for the thread, used for setjmp/longjmp operations.
  JumpContext def_jump_context_;

  inline static thread_local Thread* s_current_thread = nullptr;
};

} // namespace leanstore