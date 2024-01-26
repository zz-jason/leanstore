#pragma once

#include "HistoryTreeInterface.hpp"
#include "Worker.hpp"
#include "WorkerThreadNew.hpp"
#include "shared-headers/Units.hpp"

#include <atomic>
#include <functional>
#include <memory>
#include <vector>

namespace leanstore {

class LeanStore;

namespace cr {

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

  /// History tree should be created after worker thread and group committer are
  /// started.
  std::unique_ptr<HistoryTreeInterface> mHistoryTreePtr;

  std::atomic<u64> mRunningThreads = 0;

  std::vector<std::unique_ptr<WorkerThreadNew>> mWorkerThreadsNew;

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
  void setupHistoryTree();
};

} // namespace cr
} // namespace leanstore
