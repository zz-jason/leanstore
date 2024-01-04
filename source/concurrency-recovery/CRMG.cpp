#include "CRMG.hpp"

#include "concurrency-recovery/HistoryTree.hpp"
#include "profiling/counters/CPUCounters.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "utils/ThreadHolder.hpp"

#include <cassert>
#include <glog/logging.h>

#include <mutex>
#include <stdexcept>

namespace leanstore {
namespace cr {

/// Threads id order:
///   Workers (#workers)
///   Group Committer Thread (1)
///   Page Provider Threads (#pageProviders)
std::unique_ptr<CRManager> CRManager::sInstance = nullptr;

std::atomic<u64> CRManager::sFsyncCounter = 0;
std::atomic<u64> CRManager::sSsdOffset = 1 * 1024 * 1024 * 1024;

CRManager::CRManager(s32 walFd)
    : mWalFd(walFd), mWalSize(0), mHistoryTreePtr(nullptr),
      mNumWorkerThreads(FLAGS_worker_threads),
      mWorkerThreadsMeta(FLAGS_worker_threads) {

  // create worker threads to handle user transactions
  mWorkerThreads.reserve(mNumWorkerThreads);
  mWorkers.resize(mNumWorkerThreads);
  for (u64 workerId = 0; workerId < mNumWorkerThreads; workerId++) {
    auto workerThreadMain = [&](u64 workerId) { runWorker(workerId); };
    mWorkerThreads.emplace_back(workerThreadMain, workerId);
  }

  // wait until all worker threads are initialized
  while (mRunningThreads < mNumWorkerThreads) {
  }

  // setup group commit worker if WAL is enabled
  if (FLAGS_wal) {
    utils::ThreadHolder groupCommitWorker([&]() {
      if (FLAGS_enable_pin_worker_threads) {
        utils::pinThisThread(mNumWorkerThreads);
      }
      runGroupCommiter();
    });
    mGroupCommitterThread =
        std::make_unique<utils::ThreadHolder>(std::move(groupCommitWorker));
  }

  // setup history tree
  scheduleJobSync(0, [&]() { setupHistoryTree(); });
}

CRManager::~CRManager() {
  mGroupCommitterKeepRunning = false;
  mGroupCommitterThread.release();

  mWorkerKeepRunning = false;
  for (auto& meta : mWorkerThreadsMeta) {
    meta.cv.notify_one();
  }

  mWorkerThreads.clear();
  while (mRunningThreads > 1) {
  }
}

void CRManager::runWorker(u64 workerId) {
  // set name of the worker thread
  std::string workerName("leanstore_worker_" + std::to_string(workerId));
  pthread_setname_np(pthread_self(), workerName.c_str());

  // pin the worker thread by need
  if (FLAGS_enable_pin_worker_threads) {
    utils::pinThisThread(workerId);
  }

  if (FLAGS_cpu_counters) {
    CPUCounters::registerThread(workerName, false);
  }
  WorkerCounters::myCounters().mWorkerId = workerId;
  CRCounters::myCounters().mWorkerId = workerId;

  Worker::sTlsWorker = std::make_unique<Worker>(
      workerId, mWorkers, mNumWorkerThreads, *mHistoryTreePtr.get());
  mWorkers[workerId] = Worker::sTlsWorker.get();
  mRunningThreads++;

  // wait untile all the threads are actively running
  while (mRunningThreads != (mNumWorkerThreads + FLAGS_wal)) {
  }

  auto& meta = mWorkerThreadsMeta[workerId];
  while (mWorkerKeepRunning) {
    std::unique_lock guard(meta.mutex);
    meta.cv.wait(guard,
                 [&]() { return !mWorkerKeepRunning || meta.job != nullptr; });
    if (!mWorkerKeepRunning) {
      break;
    }

    meta.job();
    meta.mIsJobDone = true;
    meta.job = nullptr;
    meta.cv.notify_one();
  }
  mRunningThreads--;
}

void CRManager::setupHistoryTree() {
  auto historyTree = std::make_unique<HistoryTree>();
  historyTree->update_btrees =
      std::make_unique<leanstore::storage::btree::BTreeLL*[]>(
          mNumWorkerThreads);
  historyTree->remove_btrees =
      std::make_unique<leanstore::storage::btree::BTreeLL*[]>(
          mNumWorkerThreads);

  for (u64 i = 0; i < mNumWorkerThreads; i++) {
    std::string name = "_history_tree_" + std::to_string(i);
    storage::btree::BTreeGeneric::Config config = {.mEnableWal = false,
                                                   .mUseBulkInsert = true};
    // setup update tree
    std::string updateBtreeName = name + "_updates";
    auto updateBtree = storage::btree::BTreeLL::Create(updateBtreeName, config);
    if (updateBtree == nullptr) {
      LOG(FATAL) << "Failed to set up _updates tree"
                 << ", treeName=" << name
                 << ", updateBTreeName=" << updateBtreeName
                 << ", workerId=" << i;
      // TODO(jian.z): error handling
    }
    historyTree->update_btrees[i] = updateBtree;

    // setup delete tree
    std::string removeBtreeName = name + "_removes";
    auto removeBtree = storage::btree::BTreeLL::Create(removeBtreeName, config);
    if (removeBtree == nullptr) {
      LOG(FATAL) << "Failed to set up _removes tree"
                 << ", treeName=" << name
                 << ", removeBtreeName=" << removeBtreeName
                 << ", workerId=" << i;
      // TODO(jian.z): error handling
    }
    historyTree->remove_btrees[i] = removeBtree;
  }

  mHistoryTreePtr = std::move(historyTree);
}

void CRManager::scheduleJobSync(u64 workerId, std::function<void()> job) {
  setJob(workerId, job);
  joinOne(workerId, [&](WorkerThread& meta) { return meta.mIsJobDone.load(); });
}

void CRManager::scheduleJobAsync(u64 workerId, std::function<void()> job) {
  setJob(workerId, job);
}

void CRManager::scheduleJobs(u64 numWorkers, std::function<void()> job) {
  for (u32 workerId = 0; workerId < numWorkers; workerId++) {
    setJob(workerId, job);
  }
}
void CRManager::scheduleJobs(u64 numWorkers,
                             std::function<void(u64 workerId)> job) {
  for (u32 workerId = 0; workerId < numWorkers; workerId++) {
    setJob(workerId, [=]() { return job(workerId); });
  }
}

void CRManager::joinAll() {
  for (u32 i = 0; i < mNumWorkerThreads; i++) {
    joinOne(i, [&](WorkerThread& meta) {
      return meta.mIsJobDone && meta.job == nullptr;
    });
  }
}

void CRManager::setJob(u64 workerId, std::function<void()> job) {
  DCHECK(workerId < mNumWorkerThreads);
  auto& meta = mWorkerThreadsMeta[workerId];
  std::unique_lock guard(meta.mutex);
  meta.cv.wait(guard, [&]() { return meta.mIsJobDone && meta.job == nullptr; });
  meta.job = job;
  meta.mIsJobDone = false;
  guard.unlock();
  meta.cv.notify_one();
}

void CRManager::joinOne(u64 workerId,
                        std::function<bool(WorkerThread&)> condition) {
  DCHECK(workerId < mNumWorkerThreads);
  auto& meta = mWorkerThreadsMeta[workerId];
  std::unique_lock guard(meta.mutex);
  meta.cv.wait(guard, [&]() { return condition(meta); });
}

constexpr char KEY_WAL_SIZE[] = "wal_size";
constexpr char KEY_GLOBAL_LOGICAL_CLOCK[] = "global_logical_clock";

StringMap CRManager::serialize() {
  StringMap map;
  u64 val = ConcurrencyControl::sGlobalClock.load();
  map[KEY_WAL_SIZE] = std::to_string(mWalSize);
  map[KEY_GLOBAL_LOGICAL_CLOCK] = std::to_string(val);
  return map;
}

void CRManager::deserialize(StringMap map) {
  u64 val = std::stoull(map[KEY_GLOBAL_LOGICAL_CLOCK]);
  ConcurrencyControl::sGlobalClock = val;
  Worker::sAllLwm = val;

  mWalSize = std::stoull(map[KEY_WAL_SIZE]);
}

} // namespace cr
} // namespace leanstore
