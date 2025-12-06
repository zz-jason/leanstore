#pragma once

#include "leanstore/common/types.h"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/managed_thread.hpp"

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <string>

namespace leanstore {

/// WorkerThread contains the context of a worker thread. There can be multiple job senders, and
/// each job sender can send a job to the worker thread. The worker thread state is represented by a
/// pair of (jobSet, jobDone), a typical state transition is:
///
///    (!jobSet, !jobDone): a new job sender is wake up, set a new job, notify the worker thread
/// -> ( jobSet, !jobDone): the worker thread is wake up, execute the job, set job done, notify the
///                         original job sender
/// -> ( jobSet,  jobDone): the original job sender is wake up, clear the job, notify other
///                         job senders.
class WorkerThread : public utils::ManagedThread {
public:
  enum JobStatus : uint8_t {
    kJobIsEmpty = 0,
    kJobIsSet,
    kJobIsFinished,
  };

  /// The id of the worker thread.
  const lean_wid_t worker_id_;

  /// The mutex to guard the job.
  std::mutex mutex_;

  /// The condition variable to notify the worker thread and job sender.
  std::condition_variable cv_;

  /// The job to be executed by the worker thread.
  std::function<void()> job_;

  /// Whether the current job is done.
  JobStatus job_status_;

  /// Constructor.
  WorkerThread(LeanStore* store, lean_wid_t worker_id, int cpu)
      : utils::ManagedThread(store, "Worker" + std::to_string(worker_id), cpu),
        worker_id_(worker_id),
        job_(nullptr),
        job_status_(kJobIsEmpty) {
  }

  /// Destructor.
  ~WorkerThread() override {
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
  void RunImpl() override;
};

inline void WorkerThread::RunImpl() {
  while (keep_running_) {
    // wait until there is a job
    std::unique_lock guard(mutex_);
    cv_.wait(guard, [&]() { return !keep_running_ || (job_status_ == kJobIsSet); });

    // check thread status
    if (!keep_running_) {
      break;
    }

    // execute the job
    job_();

    // Set job done, change the worker state to (jobSet, jobDone), notify the job sender
    job_status_ = kJobIsFinished;

    guard.unlock();
    cv_.notify_all();
  }
};

inline void WorkerThread::Stop() {
  if (!(thread_ && thread_->joinable())) {
    return;
  }

  keep_running_ = false;
  cv_.notify_all();
  if (thread_ && thread_->joinable()) {
    thread_->join();
  }
  thread_ = nullptr;
}

inline void WorkerThread::SetJob(std::function<void()> job) {
  // wait the previous job to finish
  std::unique_lock guard(mutex_);
  cv_.wait(guard, [&]() { return job_status_ == kJobIsEmpty; });

  // set a new job, change the worker state to (jobSet, jobNotDone), notify the worker thread
  job_ = std::move(job);
  job_status_ = kJobIsSet;

  guard.unlock();
  cv_.notify_all();
}

inline void WorkerThread::Wait() {
  std::unique_lock guard(mutex_);
  cv_.wait(guard, [&]() { return job_status_ == kJobIsFinished; });

  // reset the job, change the worker state to (jobNotSet, jobDone), notify other job senders
  job_ = nullptr;
  job_status_ = kJobIsEmpty;

  guard.unlock();
  cv_.notify_all();
}

} // namespace leanstore
