#pragma once

#include "leanstore//utils/jump_mu.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/managed_thread.hpp"
#include "utils/coroutine/auto_commit_protocol.hpp"
#include "utils/coroutine/blocking_queue_mpsc.hpp"
#include "utils/coroutine/coro_env.hpp"
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
} // namespace leanstore

namespace leanstore::storage {
class PageEvictor;
} // namespace leanstore::storage

namespace leanstore::cr {
class Logging;
} // namespace leanstore::cr

namespace leanstore {

class CoroExecutor {
public:
  static constexpr auto kCoroExecNamePattern = "coro_exec_{}";

  CoroExecutor(LeanStore* store, AutoCommitProtocol* commit_protocol, int64_t thread_id = -1);
  ~CoroExecutor();

  // No copy or move semantics
  CoroExecutor(const CoroExecutor&) = delete;
  CoroExecutor& operator=(const CoroExecutor&) = delete;

  /// Starts the worker thread.
  void Start() {
    keep_running_ = true;
    thread_ = std::thread(&CoroExecutor::ThreadMain, this);
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

  void EnqueueCoro(std::unique_ptr<Coroutine>&& coroutine) {
    user_task_queue_.PushBack(std::move(coroutine));
  }

  void DequeueCoro(std::unique_ptr<Coroutine>& coroutine) {
    user_task_queue_.PopFront(coroutine);
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

  AutoCommitProtocol* AutoCommitter() const {
    return commit_protocol_;
  }

  static CoroIo* CurrentCoroIo() {
    return &CurrentThread()->coro_io_;
  }

  static CoroExecutor* CurrentThread() {
    // assert(s_current_thread != nullptr);
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
    std::string thread_name = std::format(kCoroExecNamePattern, thread_id_);
    pthread_setname_np(pthread_self(), thread_name.c_str());
    s_current_thread = this;
    JumpContext::SetCurrent(&def_jump_context_);
    CoroEnv::SetCurStore(store_);

    ready_ = true;
    Log::Info("Coro executor inited, thread_name={}", thread_name);
  }

  void ThreadLoop();

  void RunSystemCoros();

  /// The LeanStore instance.
  LeanStore* store_ = nullptr;

  AutoCommitProtocol* commit_protocol_;

  /// Pointer to the currently running coroutine.
  Coroutine* current_coroutine_ = nullptr;

  BlockingQueueMpsc<std::unique_ptr<Coroutine>> user_task_queue_{CoroEnv::kMaxCoroutinesPerThread};

  /// Task queue for system tasks, e.g. IO operations.
  std::vector<std::unique_ptr<Coroutine>> sys_tasks_;

  CoroIo coro_io_{CoroEnv::kMaxCoroutinesPerThread};

  std::unordered_set<uint64_t> eviction_pending_partitions_;
  std::unique_ptr<leanstore::storage::PageEvictor> page_evictor_;

  std::thread thread_;

  int64_t thread_id_ = -1;

  std::atomic<bool> keep_running_{false};

  std::atomic<bool> ready_{false};

  /// Jump context for the thread, used for setjmp/longjmp operations.
  JumpContext def_jump_context_;

  inline static thread_local CoroExecutor* s_current_thread = nullptr;
};

} // namespace leanstore