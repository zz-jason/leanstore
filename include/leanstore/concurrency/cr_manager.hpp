#pragma once

#include "leanstore/concurrency/worker_context.hpp"
#include "leanstore/concurrency/worker_thread.hpp"
#include "leanstore/units.hpp"

#include <memory>
#include <vector>

namespace leanstore {

class LeanStore;

namespace cr {

struct WaterMarkInfo;
class GroupCommitter;

/// Manages a fixed number of worker threads and group committer threads.
class CRManager {
public:
  /// The LeanStore instance.
  leanstore::LeanStore* store_;

  /// All the worker threads
  std::vector<std::unique_ptr<WorkerThread>> worker_threads_;

  /// All the thread-local worker references
  std::vector<WorkerContext*> worker_ctxs_;

  WaterMarkInfo global_wmk_info_;

  /// The group committer thread, created and started if WAL is enabled when the
  /// CRManager instance is created.
  ///
  /// NOTE: It should be created after all the worker threads are created and
  /// started.
  std::unique_ptr<GroupCommitter> group_committer_;

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
  void setup_history_storage4_each_worker();
};

} // namespace cr
} // namespace leanstore
