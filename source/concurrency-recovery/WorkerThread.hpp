#pragma once

#include "profiling/counters/CPUCounters.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "shared-headers/Units.hpp"
#include "utils/UserThread.hpp"

#include <condition_variable>
#include <functional>
#include <string>

namespace leanstore::cr {

class WorkerThread : public utils::UserThread {
public:
  /// The id of the worker thread.
  WORKERID mWorkerId;

  /// The mutex to guard the job.
  std::mutex mMutex;

  /// The condition variable to notify the worker thread and job sender.
  std::condition_variable mCv;

  /// The job to be executed by the worker thread.
  std::function<void()> mJob = nullptr;

public:
  /// Constructor.
  WorkerThread(WORKERID workerId, int cpu)
      : utils::UserThread("Worker" + std::to_string(workerId), cpu),
        mWorkerId(workerId),
        mJob(nullptr) {
  }

  /// Destructor.
  ~WorkerThread() {
    Stop();
  }

  /// Stop the worker thread.
  /// API for the job sender.
  void Stop() override;

  /// Set a job to the worker thread and notify it to run.
  /// API for the job sender.
  void SetJob(std::function<void()> job);

  /// Wait until the job is done.
  /// API for the job sender.
  void Wait();

protected:
  /// The main loop of the worker thread.
  void runImpl() override;
};

inline void WorkerThread::runImpl() {
  if (FLAGS_cpu_counters) {
    CPUCounters::registerThread(mThreadName, false);
  }

  WorkerCounters::MyCounters().mWorkerId = mWorkerId;
  CRCounters::MyCounters().mWorkerId = mWorkerId;

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
    mJob = nullptr;

    // notify the job sender
    mCv.notify_one();
  }
};

inline void WorkerThread::Stop() {
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

inline void WorkerThread::SetJob(std::function<void()> job) {
  // wait the previous job to finish
  std::unique_lock guard(mMutex);
  mCv.wait(guard, [&]() { return mJob == nullptr; });

  // set a new job
  mJob = job;

  // notify the worker thread to run
  guard.unlock();
  mCv.notify_one();
}

inline void WorkerThread::Wait() {
  std::unique_lock guard(mMutex);
  mCv.wait(guard, [&]() { return mJob == nullptr; });
}

} // namespace leanstore::cr
