#include "CRMG.hpp"

#include "GroupCommitter.hpp"
#include "LeanStore.hpp"
#include "WorkerThreadNew.hpp"
#include "concurrency-recovery/HistoryTree.hpp"
#include "profiling/counters/CPUCounters.hpp"
#include "profiling/counters/WorkerCounters.hpp"

#include <glog/logging.h>

#include <memory>
#include <mutex>

namespace leanstore {
namespace cr {

/// Threads id order:
///   Workers (#workers)
///   Group Committer Thread (1)
///   Page Provider Threads (#pageProviders)
CRManager::CRManager(leanstore::LeanStore* store, s32 walFd)
    : mStore(store),
      mGroupCommitter(nullptr),
      mHistoryTreePtr(nullptr) {
  // start all worker threads
  mWorkers.resize(FLAGS_worker_threads);
  mWorkerThreadsNew.reserve(FLAGS_worker_threads);
  for (u64 workerId = 0; workerId < FLAGS_worker_threads; workerId++) {
    auto workerThreadNew =
        std::make_unique<WorkerThreadNew>(workerId, workerId);
    workerThreadNew->Start();

    // create thread-local transaction executor on each worker thread
    workerThreadNew->SetJob([&]() {
      Worker::sTlsWorker = std::make_unique<Worker>(
          workerId, mWorkers, FLAGS_worker_threads, mStore);
      mWorkers[workerId] = Worker::sTlsWorker.get();
    });
    workerThreadNew->JoinJob();
    mWorkerThreadsNew.emplace_back(std::move(workerThreadNew));
  }

  // start group commit thread
  if (FLAGS_wal) {
    const int cpu = FLAGS_enable_pin_worker_threads ? FLAGS_worker_threads : -1;
    mGroupCommitter = std::make_unique<GroupCommitter>(walFd, mWorkers, cpu);
    mGroupCommitter->Start();
  }

  // create history tree for each worker
  ScheduleJobSync(0, [&]() { setupHistoryTree(); });
  for (u64 workerId = 0; workerId < FLAGS_worker_threads; workerId++) {
    mWorkers[workerId]->cc.mHistoryTree = mHistoryTreePtr.get();
  }
}

void CRManager::Stop() {
  mGroupCommitter->Stop();
  mWorkerKeepRunning = false;
  for (auto& workerThread : mWorkerThreadsNew) {
    workerThread->mCv.notify_one();
  }
  mWorkerThreadsNew.clear();
}

CRManager::~CRManager() {
  Stop();
}

void CRManager::setupHistoryTree() {
  auto historyTree = std::make_unique<HistoryTree>();
  historyTree->mUpdateBTrees =
      std::make_unique<leanstore::storage::btree::BasicKV*[]>(
          FLAGS_worker_threads);
  historyTree->mRemoveBTrees =
      std::make_unique<leanstore::storage::btree::BasicKV*[]>(
          FLAGS_worker_threads);

  for (u64 i = 0; i < FLAGS_worker_threads; i++) {
    std::string name = "_history_tree_" + std::to_string(i);
    storage::btree::BTreeConfig config = {.mEnableWal = false,
                                          .mUseBulkInsert = true};
    // setup update tree
    std::string updateBtreeName = name + "_updates";
    auto res = storage::btree::BasicKV::Create(mStore, updateBtreeName, config);
    if (!res) {
      LOG(FATAL) << "Failed to set up _updates tree"
                 << ", treeName=" << name
                 << ", updateBTreeName=" << updateBtreeName
                 << ", workerId=" << i << ", error=" << res.error().ToString();
    }
    historyTree->mUpdateBTrees[i] = res.value();

    // setup delete tree
    std::string removeBtreeName = name + "_removes";
    res = storage::btree::BasicKV::Create(mStore, removeBtreeName, config);
    if (!res) {
      LOG(FATAL) << "Failed to set up _removes tree"
                 << ", treeName=" << name
                 << ", removeBtreeName=" << removeBtreeName
                 << ", workerId=" << i << ", error=" << res.error().ToString();
    }
    historyTree->mRemoveBTrees[i] = res.value();
  }

  mHistoryTreePtr = std::move(historyTree);
}

void CRManager::ScheduleJobSync(u64 workerId, std::function<void()> job) {
  mWorkerThreadsNew[workerId]->SetJob(job);
  mWorkerThreadsNew[workerId]->JoinJob();
}

void CRManager::ScheduleJobAsync(u64 workerId, std::function<void()> job) {
  mWorkerThreadsNew[workerId]->SetJob(job);
}

void CRManager::JoinAll() {
  for (u32 i = 0; i < FLAGS_worker_threads; i++) {
    mWorkerThreadsNew[i]->JoinJob();
  }
}

constexpr char kKeyWalSize[] = "wal_size";
constexpr char kKeyGlobalLogicalClock[] = "global_logical_clock";

StringMap CRManager::Serialize() {
  StringMap map;
  u64 val = mStore->mTimestampOracle.load();
  map[kKeyWalSize] = std::to_string(mGroupCommitter->mWalSize);
  map[kKeyGlobalLogicalClock] = std::to_string(val);
  return map;
}

void CRManager::Deserialize(StringMap map) {
  u64 val = std::stoull(map[kKeyGlobalLogicalClock]);
  mStore->mTimestampOracle = val;
  Worker::sWmkOfAllTx = val;
  mGroupCommitter->mWalSize = std::stoull(map[kKeyWalSize]);
}

} // namespace cr
} // namespace leanstore
