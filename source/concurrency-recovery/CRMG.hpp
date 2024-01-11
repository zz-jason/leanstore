#pragma once

#include "Config.hpp"
#include "Exceptions.hpp"
#include "HistoryTreeInterface.hpp"
#include "Units.hpp"
#include "Worker.hpp"
#include "utils/ThreadHolder.hpp"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <thread>
#include <vector>

namespace leanstore {
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
  //---------------------------------------------------------------------------
  // Constructor and Destructors
  //---------------------------------------------------------------------------
  CRManager(s32 ssdFd);
  ~CRManager();

public:
  //---------------------------------------------------------------------------
  // Public Object Utils
  //---------------------------------------------------------------------------

  /**
   * @brief Schedule same job on specific amount of workers.
   *
   * @param numWorkers amount of workers
   * @param job Job to do. Same for each worker.
   */
  void scheduleJobs(u64 numWorkers, std::function<void()> job);

  /**
   * @brief Schedule specific job on specific amount of workers.
   *
   * @param workers amount of workers
   * @param job Job to do. Different for each worker.
   */
  void scheduleJobs(u64 numWorkers, std::function<void(u64 workerId)> job);

  /**
   * @brief Schedules one job asynchron on specific worker.
   *
   * @param workerId worker to compute job
   * @param job job
   */
  void scheduleJobAsync(u64 workerId, std::function<void()> job);

  /**
   * @brief Schedules one job on one specific worker and waits for completion.
   *
   * @param workerId worker to compute job
   * @param job job
   */
  void scheduleJobSync(u64 workerId, std::function<void()> job);

  /**
   * @brief Waits for all Workers to complete.
   *
   */
  void joinAll();

  // State Serialization
  StringMap serialize();

  void deserialize(StringMap map);

  void stop();

private:
  static std::atomic<u64> sFsyncCounter;
  static std::atomic<u64> sSsdOffset;

  void runGroupCommiter();
  void runWorker(u64 workerId);

  /**
   * @brief Set the Job to specific worker.
   *
   * @param workerId specific worker
   * @param job job
   */
  void setJob(u64 workerId, std::function<void()> job);

  /**
   * @brief Wait for one worker to complete.
   *
   * @param workerId worker id.
   * @param condition what is the completion condition?
   */
  void joinOne(u64 workerId, std::function<bool(WorkerThread&)> condition);

  void setupHistoryTree();

public:
  //----------------------------------------------------------------------------
  // static members
  //----------------------------------------------------------------------------
  static std::unique_ptr<CRManager> sInstance;
};

} // namespace cr
} // namespace leanstore
