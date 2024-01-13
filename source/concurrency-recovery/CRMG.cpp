#include "CRMG.hpp"

#include "GroupCommitter.hpp"
#include "concurrency-recovery/HistoryTree.hpp"
#include "profiling/counters/CPUCounters.hpp"
#include "profiling/counters/WorkerCounters.hpp"

#include <glog/logging.h>

#include <mutex>

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
    : mGrouopCommitter(nullptr),
      mHistoryTreePtr(nullptr),
      mWorkerThreadsMeta(FLAGS_worker_threads) {

  // create worker threads to handle user transactions
  mWorkerThreads.reserve(FLAGS_worker_threads);
  mWorkers.resize(FLAGS_worker_threads);
  for (u64 workerId = 0; workerId < FLAGS_worker_threads; workerId++) {
    auto workerThreadMain = [&](u64 workerId) { runWorker(workerId); };
    mWorkerThreads.emplace_back(workerThreadMain, workerId);
  }

  // wait until all worker threads are initialized
  while (mRunningThreads < FLAGS_worker_threads) {
  }

  // setup group commit worker if WAL is enabled
  if (FLAGS_wal) {
    const int cpu = FLAGS_enable_pin_worker_threads ? FLAGS_worker_threads : -1;
    mGrouopCommitter = std::make_unique<GroupCommitter>(walFd, mWorkers, cpu);
    mGrouopCommitter->Start();
  }

  // setup history tree
  scheduleJobSync(0, [&]() { setupHistoryTree(); });
  for (u64 workerId = 0; workerId < FLAGS_worker_threads; workerId++) {
    mWorkers[workerId]->cc.mHistoryTree = mHistoryTreePtr.get();
  }
}

CRManager::~CRManager() {
  mGrouopCommitter = nullptr;

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
  std::string workerName("worker_" + std::to_string(workerId));
  DCHECK(workerName.size() < 16);
  pthread_setname_np(pthread_self(), workerName.c_str());

  // pin the worker thread by need
  if (FLAGS_enable_pin_worker_threads) {
    utils::PinThisThread(workerId);
  }

  if (FLAGS_cpu_counters) {
    CPUCounters::registerThread(workerName, false);
  }
  WorkerCounters::MyCounters().mWorkerId = workerId;
  CRCounters::MyCounters().mWorkerId = workerId;

  Worker::sTlsWorker =
      std::make_unique<Worker>(workerId, mWorkers, FLAGS_worker_threads);
  mWorkers[workerId] = Worker::sTlsWorker.get();
  mRunningThreads++;

  // wait other worker threads to run
  while (mRunningThreads < FLAGS_worker_threads) {
  }

  // wait group committer thread to run
  while (FLAGS_wal &&
         (mGrouopCommitter == nullptr || !mGrouopCommitter->IsStarted())) {
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
          FLAGS_worker_threads);
  historyTree->remove_btrees =
      std::make_unique<leanstore::storage::btree::BTreeLL*[]>(
          FLAGS_worker_threads);

  for (u64 i = 0; i < FLAGS_worker_threads; i++) {
    std::string name = "_history_tree_" + std::to_string(i);
    storage::btree::BTreeGeneric::Config config = {.mEnableWal = false,
                                                   .mUseBulkInsert = true};
    // setup update tree
    std::string updateBtreeName = name + "_updates";
    auto res = storage::btree::BTreeLL::Create(updateBtreeName, config);
    if (!res) {
      LOG(FATAL) << "Failed to set up _updates tree"
                 << ", treeName=" << name
                 << ", updateBTreeName=" << updateBtreeName
                 << ", workerId=" << i << ", error=" << res.error().mMessage;
    }
    historyTree->update_btrees[i] = res.value();

    // setup delete tree
    std::string removeBtreeName = name + "_removes";
    res = storage::btree::BTreeLL::Create(removeBtreeName, config);
    if (!res) {
      LOG(FATAL) << "Failed to set up _removes tree"
                 << ", treeName=" << name
                 << ", removeBtreeName=" << removeBtreeName
                 << ", workerId=" << i << ", error=" << res.error().mMessage;
    }
    historyTree->remove_btrees[i] = res.value();
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
  for (u32 i = 0; i < FLAGS_worker_threads; i++) {
    joinOne(i, [&](WorkerThread& meta) {
      return meta.mIsJobDone && meta.job == nullptr;
    });
  }
}

void CRManager::setJob(u64 workerId, std::function<void()> job) {
  DCHECK(workerId < FLAGS_worker_threads);
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
  DCHECK(workerId < FLAGS_worker_threads);
  auto& meta = mWorkerThreadsMeta[workerId];
  std::unique_lock guard(meta.mutex);
  meta.cv.wait(guard, [&]() { return condition(meta); });
}

constexpr char kKeyWalSize[] = "wal_size";
constexpr char kKeyGlobalLogicalClock[] = "global_logical_clock";

StringMap CRManager::serialize() {
  StringMap map;
  u64 val = ConcurrencyControl::sGlobalClock.load();
  map[kKeyWalSize] = std::to_string(mGrouopCommitter->mWalSize);
  map[kKeyGlobalLogicalClock] = std::to_string(val);
  return map;
}

void CRManager::deserialize(StringMap map) {
  u64 val = std::stoull(map[kKeyGlobalLogicalClock]);
  ConcurrencyControl::sGlobalClock = val;
  Worker::sAllLwm = val;
  mGrouopCommitter->mWalSize = std::stoull(map[kKeyWalSize]);
}

} // namespace cr
} // namespace leanstore
