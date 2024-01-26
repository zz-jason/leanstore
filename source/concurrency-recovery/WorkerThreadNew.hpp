#pragma once

#include "concurrency-recovery/Worker.hpp"
#include "profiling/counters/CPUCounters.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "shared-headers/Units.hpp"
#include "utils/UserThread.hpp"

#include <condition_variable>
#include <functional>
#include <string>

namespace leanstore::cr {

class WorkerThreadNew : public utils::UserThread {
public:
  WORKERID mWorkerId;

  std::mutex mMutex;

  std::condition_variable mCv;

  std::function<void()> mJob = nullptr;

  std::atomic<bool> mIsJobDone = true; // Job done

public:
  WorkerThreadNew(WORKERID workerId, int cpu)
      : utils::UserThread("Worker" + std::to_string(workerId), cpu),
        mWorkerId(workerId),
        mJob(nullptr),
        mIsJobDone(true) {
  }

  ~WorkerThreadNew() {
    Stop();
  }

protected:
  void runImpl() override;

public:
  void Stop() override;

  void SetJob(std::function<void()> job);

  void JoinJob();
};

inline void WorkerThreadNew::runImpl() {
  if (FLAGS_cpu_counters) {
    CPUCounters::registerThread(mThreadName, false);
  }

  WorkerCounters::MyCounters().mWorkerId = mWorkerId;
  CRCounters::MyCounters().mWorkerId = mWorkerId;

  // // wait group committer thread to run
  // while (FLAGS_wal && !mGroupCommitterStarted) {
  // }

  while (mKeepRunning) {
    // wait until there is a job
    std::unique_lock guard(mMutex);
    mCv.wait(guard, [&]() { return !mKeepRunning || mJob != nullptr; });

    // check thread status
    if (!mKeepRunning) {
      break;
    }

    // execute the job
    mJob();

    // clear the executed job
    mIsJobDone = true;
    mJob = nullptr;

    // notify the job sender
    mCv.notify_one();
  }
};

inline void WorkerThreadNew::Stop() {
  if (!(mThread && mThread->joinable())) {
    return;
  }

  mKeepRunning = false;
  mCv.notify_one();
  if (mThread && mThread->joinable()) {
    mThread->join();
  }
  mThread = nullptr;
}

inline void WorkerThreadNew::SetJob(std::function<void()> job) {
  // wait the previous job to finish
  std::unique_lock guard(mMutex);
  mCv.wait(guard, [&]() { return mIsJobDone && mJob == nullptr; });

  // set a new job
  mJob = job;
  mIsJobDone = false;

  // notify the worker thread to run
  guard.unlock();
  mCv.notify_one();
}

inline void WorkerThreadNew::JoinJob() {
  std::unique_lock guard(mMutex);
  mCv.wait(guard, [&]() { return mIsJobDone.load(); });
}

} // namespace leanstore::cr
