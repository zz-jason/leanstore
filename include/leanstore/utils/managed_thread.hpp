#pragma once

#include "coroutine/coro_env.hpp"
#include "leanstore/cpp/base/defer.hpp"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/utils/misc.hpp"

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include <pthread.h>

namespace leanstore {

class LeanStore;

} // namespace leanstore

namespace leanstore::utils {

inline thread_local std::string tls_thread_name = "";

/// User thread with custom thread name.
class ManagedThread {
protected:
  LeanStore* store_ = nullptr;

  std::string thread_name_ = "";

  int running_cpu_ = -1;

  std::unique_ptr<std::thread> thread_ = nullptr;

  std::atomic<bool> keep_running_ = false;

public:
  ManagedThread(LeanStore* store, const std::string& name, int running_cpu = -1)
      : store_(store),
        thread_name_(name),
        running_cpu_(running_cpu) {
    if (thread_name_.size() > 15) {
      Log::Error("thread_name_ should be restricted to 15 characters, name={}, size={}", name,
                 name.size());
    }
  }

  virtual ~ManagedThread() {
    Stop();
  }

  /// Start executing the thread.
  void Start() {
    if (thread_ == nullptr) {
      keep_running_ = true;
      thread_ = std::make_unique<std::thread>(&ManagedThread::Run, this);
    }
  }

  /// Stop executing the thread.
  virtual void Stop() {
    keep_running_ = false;
    if (thread_ && thread_->joinable()) {
      thread_->join();
    }
    thread_ = nullptr;
  }

  bool IsStarted() {
    return thread_ != nullptr && thread_->joinable();
  }

protected:
  void Run() {
    CoroEnv::SetCurStore(store_);

    // set thread-local thread name at the very beging so that logs printed by
    // the thread can get it.
    tls_thread_name = thread_name_;

    // log info about thread start and stop events
    Log::Info("{} thread started", thread_name_);
    LEAN_DEFER(Log::Info("{} thread stopped", thread_name_));

    // setup thread name
    pthread_setname_np(pthread_self(), thread_name_.c_str());

    // pin the thread to a specific CPU
    if (running_cpu_ != -1) {
      utils::PinThisThread(running_cpu_);
      Log::Info("{} pined to CPU {}", thread_name_, running_cpu_);
    }

    // run custom thread loop
    RunImpl();
  }

  /// Custom thread loop
  virtual void RunImpl() = 0;
};

} // namespace leanstore::utils
