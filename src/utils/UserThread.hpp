#pragma once

#include "Defer.hpp"
#include "Misc.hpp"
#include "utils/Log.hpp"

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include <pthread.h>

namespace leanstore {

class LeanStore;

} // namespace leanstore

namespace leanstore::utils {

inline thread_local LeanStore* tlsStore = nullptr;
inline thread_local std::string tlsThreadName = "";

/// User thread with custom thread name.
class UserThread {
protected:
  LeanStore* mStore = nullptr;

  std::string mThreadName = "";

  int mRunningCPU = -1;

  std::unique_ptr<std::thread> mThread = nullptr;

  std::atomic<bool> mKeepRunning = false;

public:
  UserThread(LeanStore* store, const std::string& name, int runningCPU = -1)
      : mStore(store),
        mThreadName(name),
        mRunningCPU(runningCPU) {
    if (mThreadName.size() > 15) {
      Log::Error(
          "Thread name should be restricted to 15 characters, name={}, size={}",
          name, name.size());
    }
  }

  virtual ~UserThread() {
    Stop();
  }

public:
  /// Start executing the thread.
  void Start() {
    if (mThread == nullptr) {
      mKeepRunning = true;
      mThread = std::make_unique<std::thread>(&UserThread::run, this);
    }
  }

  /// Stop executing the thread.
  virtual void Stop() {
    mKeepRunning = false;
    if (mThread && mThread->joinable()) {
      mThread->join();
    }
    mThread = nullptr;
  }

  bool IsStarted() {
    return mThread != nullptr && mThread->joinable();
  }

protected:
  void run() {
    tlsStore = mStore;

    // set thread-local thread name at the very beging so that logs printed by
    // the thread can get it.
    tlsThreadName = mThreadName;

    // log info about thread start and stop events
    Log::Info("{} thread started", mThreadName);
    SCOPED_DEFER(Log::Info("{} thread stopped", mThreadName));

    // setup thread name
    pthread_setname_np(pthread_self(), mThreadName.c_str());

    // pin the thread to a specific CPU
    if (mRunningCPU != -1) {
      utils::PinThisThread(mRunningCPU);
      Log::Info("{} pined to CPU {}", mThreadName, mRunningCPU);
    }

    // run custom thread loop
    runImpl();
  }

  /// Custom thread loop
  virtual void runImpl() = 0;
};

} // namespace leanstore::utils
