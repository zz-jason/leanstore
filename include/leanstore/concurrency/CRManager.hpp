#pragma once

#include "leanstore/Units.hpp"
#include "leanstore/concurrency/WorkerContext.hpp"
#include "leanstore/concurrency/WorkerThread.hpp"

#include <memory>
#include <vector>

namespace leanstore {

class LeanStore;

namespace cr {

struct WaterMarkInfo;

//! Manages a fixed number of worker threads and group committer threads.
class CRManager {
public:
  //! The LeanStore instance.
  leanstore::LeanStore* mStore;

  //! All the worker threads
  std::vector<std::unique_ptr<WorkerThread>> mWorkerThreads;

  //! All the thread-local worker references
  std::vector<WorkerContext*> mWorkerCtxs;

  WaterMarkInfo mGlobalWmkInfo;

public:
  //! Construct a CRManager.
  CRManager(leanstore::LeanStore* store);

  //! Destruct a CRManager.
  ~CRManager();

  // State Serialization
  StringMap Serialize();

  //! Deserialize the state of the CRManager from a StringMap.
  void Deserialize(StringMap map);

  //! Stop all the worker threads and the group committer thread.
  void Stop();

private:
  void setupHistoryStorage4EachWorker();
};

} // namespace cr
} // namespace leanstore
