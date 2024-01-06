#pragma once

#include "Defer.hpp"
#include "Misc.hpp"

#include <glog/logging.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include <pthread.h>

namespace leanstore {
namespace utils {

/// User thread with custom thread name.
class UserThread {
protected:
  std::string mThreadName = "";
  int mRunningCPU = -1;
  std::unique_ptr<std::thread> mThread = nullptr;
  std::atomic<bool> mKeepRunning = false;

public:
  UserThread() = default;

  UserThread(const std::string& name, int runningCPU = -1)
      : mThreadName(name), mRunningCPU(runningCPU) {
    LOG_IF(ERROR, mThreadName.size() > 15)
        << "Thread name should be restricted to 15 characters"
        << ", name=" << name << ", size=" << name.size();
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
  void Stop() {
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
    // log info about thread start and stop events
    LOG(INFO) << mThreadName << " thread started";
    SCOPED_DEFER(LOG(INFO) << mThreadName << " thread stopped");

    // setup thread name
    pthread_setname_np(pthread_self(), mThreadName.c_str());

    // pin the thread to a specific CPU
    if (mRunningCPU != -1) {
      utils::pinThisThread(mRunningCPU);
      LOG(INFO) << mThreadName << " pined to CPU " << mRunningCPU;
    }

    // run custom thread loop
    runImpl();
  }

  /// Custom thread loop
  virtual void runImpl() = 0;
};

} // namespace utils
} // namespace leanstore
