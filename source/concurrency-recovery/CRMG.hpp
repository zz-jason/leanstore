#pragma once

#include "HistoryTreeInterface.hpp"
#include "Worker.hpp"
#include "WorkerThread.hpp"
#include "shared-headers/Units.hpp"

#include <memory>
#include <vector>

namespace leanstore {

class LeanStore;

namespace cr {

struct WaterMarkInfo;
class GroupCommitter;

/// Manages a fixed number of worker threads, each one gets a partition. CR is
/// short for "concurrent resource"
class CRManager {
public:
  /// The LeanStore instance.
  leanstore::LeanStore* mStore;

  /// All the worker threads
  std::vector<std::unique_ptr<WorkerThread>> mWorkerThreads;

  /// All the thread-local worker references
  std::vector<Worker*> mWorkers;

  /// History tree should be created after worker thread and group committer are
  /// started.
  std::unique_ptr<HistoryTreeInterface> mHistoryTreePtr;

  WaterMarkInfo mGlobalWmkInfo;

  /// The group committer thread, created and started if WAL is enabled when the
  /// CRManager instance is created.
  ///
  /// NOTE: It should be created after all the worker threads are created and
  /// started.
  std::unique_ptr<GroupCommitter> mGroupCommitter;

public:
  CRManager(leanstore::LeanStore* store);

  ~CRManager();

public:
  // State Serialization
  StringMap Serialize();

  /// Deserialize the state of the CRManager from a StringMap.
  void Deserialize(StringMap map);

  /// Stop all the worker threads and the group committer thread.
  void Stop();

private:
  void setupHistoryTree();
};

} // namespace cr
} // namespace leanstore
