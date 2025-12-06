#pragma once

#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/concurrency/worker_thread.hpp"
#include "utils/json.hpp"

#include <memory>
#include <vector>

namespace leanstore {

/// Forward declarations
class LeanStore;
struct WatermarkInfo;
class GroupCommitter;

/// Manages a fixed number of worker threads and group committer threads.
class CRManager {
public:
  /// The LeanStore instance.
  LeanStore* store_;

  /// All the worker threads
  std::vector<std::unique_ptr<WorkerThread>> worker_threads_;

  /// The group committer thread, created and started if WAL is enabled when the
  /// CRManager instance is created.
  ///
  /// NOTE: It should be created after all the worker threads are created and
  /// started.
  std::unique_ptr<GroupCommitter> group_committer_;

  explicit CRManager(LeanStore* store);

  ~CRManager();

  // State Serialization
  utils::JsonObj Serialize() const;

  /// Deserialize the state of the CRManager.
  void Deserialize(const utils::JsonObj& json_obj);

  /// Stop all the worker threads and the group committer thread.
  void Stop();

private:
  void StartWorkerThreads(uint64_t num_worker_threads);

  void StartGroupCommitter(int cpu);
};

} // namespace leanstore
