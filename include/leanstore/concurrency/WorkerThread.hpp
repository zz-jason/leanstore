#pragma once

#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/UserThread.hpp"

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <string>

namespace leanstore::cr {

//! WorkerThread contains the context of a worker thread. There can be multiple job senders, and
//! each job sender can send a job to the worker thread. The worker thread state is represented by a
//! pair of (jobSet, jobDone), a typical state transition is:
//!
//!    (!jobSet, !jobDone): a new job sender is wake up, set a new job, notify the worker thread
//! -> ( jobSet, !jobDone): the worker thread is wake up, execute the job, set job done, notify the
//!                         original job sender
//! -> ( jobSet,  jobDone): the original job sender is wake up, clear the job, notify other
//!                         job senders.
class WorkerThread : public utils::UserThread {
public:
  enum JobStatus : uint8_t {
    kJobIsEmpty = 0,
    kJobIsSet,
    kJobIsFinished,
  };

  //! The id of the worker thread.
  const WORKERID mWorkerId;

  //! The mutex to guard the job.
  std::mutex mMutex;

  //! The condition variable to notify the worker thread and job sender.
  std::condition_variable mCv;

  //! The job to be executed by the worker thread.
  std::function<void()> mJob;

  //! Whether the current job is done.
  JobStatus mJobStatus;

public:
  //! Constructor.
  WorkerThread(LeanStore* store, WORKERID workerId, int cpu)
      : utils::UserThread(store, "Worker" + std::to_string(workerId), cpu),
        mWorkerId(workerId),
        mJob(nullptr),
        mJobStatus(kJobIsEmpty) {
  }

  //! Destructor.
  ~WorkerThread() override {
    Stop();
  }

  //! Stop the worker thread.
  //! API for the job sender.
  void Stop() override;

  //! Set a job to the worker thread and notify it to run.
  //! API for the job sender.
  void SetJob(std::function<void()> job);

  //! Wait until the job is done.
  //! API for the job sender.
  void Wait();

protected:
  //! The main loop of the worker thread.
  void runImpl() override;
};

inline void WorkerThread::runImpl() {
  if (utils::tlsStore->mStoreOption->mEnableCpuCounters) {
    CPUCounters::registerThread(mThreadName, false);
  }

  WorkerCounters::MyCounters().mWorkerId = mWorkerId;
  CRCounters::MyCounters().mWorkerId = mWorkerId;

  while (mKeepRunning) {
    // wait until there is a job
    std::unique_lock guard(mMutex);
    mCv.wait(guard, [&]() { return !mKeepRunning || (mJobStatus == kJobIsSet); });

    // check thread status
    if (!mKeepRunning) {
      break;
    }

    // execute the job
    mJob();

    // Set job done, change the worker state to (jobSet, jobDone), notify the job sender
    mJobStatus = kJobIsFinished;

    guard.unlock();
    mCv.notify_all();
  }
};

inline void WorkerThread::Stop() {
  if (!(mThread && mThread->joinable())) {
    return;
  }

  mKeepRunning = false;
  mCv.notify_all();
  if (mThread && mThread->joinable()) {
    mThread->join();
  }
  mThread = nullptr;
}

inline void WorkerThread::SetJob(std::function<void()> job) {
  // wait the previous job to finish
  std::unique_lock guard(mMutex);
  mCv.wait(guard, [&]() { return mJobStatus == kJobIsEmpty; });

  // set a new job, change the worker state to (jobSet, jobNotDone), notify the worker thread
  mJob = std::move(job);
  mJobStatus = kJobIsSet;

  guard.unlock();
  mCv.notify_all();
}

inline void WorkerThread::Wait() {
  std::unique_lock guard(mMutex);
  mCv.wait(guard, [&]() { return mJobStatus == kJobIsFinished; });

  // reset the job, change the worker state to (jobNotSet, jobDone), notify other job senders
  mJob = nullptr;
  mJobStatus = kJobIsEmpty;

  guard.unlock();
  mCv.notify_all();
}

} // namespace leanstore::cr
