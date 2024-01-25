#pragma once

#include "HistoryTreeInterface.hpp"
#include "Worker.hpp"
#include "shared-headers/Units.hpp"
#include "utils/ThreadHolder.hpp"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <vector>

namespace leanstore {

class LeanStore;
namespace cr {

struct WorkerThread {
  std::mutex mutex;
  std::condition_variable cv;
  std::function<void()> job = nullptr;
  std::atomic<bool> mIsJobDone = true; // Job done
};

class GroupCommitter;

/// Manages a fixed number of worker threads, each one gets a partition. CR is
/// short for "concurrent resource"
class CRManager {
public:
  /// The LeanStore instance.
  leanstore::LeanStore* mStore;

  /// The group committer thread, created and started if WAL is enabled when the
  /// CRManager instance is created.
  ///
  /// NOTE: It should be created after all the worker threads are created and
  /// started.
  std::unique_ptr<GroupCommitter> mGroupCommitter;

  /// Whether the group committer thread is started. Worker threads can serve
  /// user transactions only whent the group committer thread is started.
  std::atomic<bool> mGroupCommitterStarted = false;

  std::unique_ptr<HistoryTreeInterface> mHistoryTreePtr;

  std::atomic<u64> mRunningThreads = 0;

  std::atomic<bool> mWorkerKeepRunning = true;

  std::vector<utils::ThreadHolder> mWorkerThreads;

  std::vector<WorkerThread> mWorkerThreadsMeta;

  /// All the thread-local worker references
  std::vector<Worker*> mWorkers;

public:
  CRManager(leanstore::LeanStore* store, s32 ssdFd);
  ~CRManager();

public:
  /// Schedules one job asynchron on specific worker.
  /// @param workerId worker to compute job
  /// @param job job
  void ScheduleJobAsync(u64 workerId, std::function<void()> job);

  /// Schedules one job on one specific worker and waits for completion.
  /// @param workerId worker to compute job
  /// @param job job
  void ScheduleJobSync(u64 workerId, std::function<void()> job);

  /// Waits for all Workers to complete.
  void JoinAll();

  // State Serialization
  StringMap Serialize();

  void Deserialize(StringMap map);

  void Stop();

private:
  void runGroupCommiter();

  void runWorker(u64 workerId);

  /// Set the Job to specific worker.
  /// @param workerId specific worker
  /// @param job job
  void setJob(u64 workerId, std::function<void()> job);

  /// Wait for one worker to complete.
  /// @param workerId worker id.
  /// @param condition what is the completion condition?
  void joinOne(u64 workerId, std::function<bool(WorkerThread&)> condition);

  void setupHistoryTree();
};

} // namespace cr
} // namespace leanstore
