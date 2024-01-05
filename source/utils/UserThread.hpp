#pragma once

#include <glog/logging.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include <pthread.h>

namespace leanstore {
namespace utils {

/// User thread with custom thread name, started by expicitly calling Start().
class UserThread {
protected:
  std::string mThreadName = "";
  std::unique_ptr<std::thread> mThread = nullptr;
  std::atomic<bool> mKeepRunning = false;

public:
  UserThread() = default;

  UserThread(const std::string& name) : mThreadName(name) {
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
    LOG(INFO) << "Thread started, name=" << mThreadName;
    // setup thread name
    pthread_setname_np(pthread_self(), mThreadName.c_str());
    // run custom thread loop
    runImpl();
    LOG(INFO) << "Thread stopped, name=" << mThreadName;
  }

  /// Custom thread loop
  virtual void runImpl() = 0;
};

} // namespace utils
} // namespace leanstore
