#include "CRMG.hpp"

#include "GroupCommitter.hpp"
#include "WorkerThread.hpp"
#include "concurrency-recovery/HistoryTree.hpp"
#include "concurrency-recovery/Worker.hpp"
#include "leanstore/LeanStore.hpp"

#include <glog/logging.h>

#include <memory>
#include <vector>

namespace leanstore::cr {

CRManager::CRManager(leanstore::LeanStore* store)
    : mStore(store),
      mHistoryTreePtr(nullptr),
      mGroupCommitter(nullptr) {
  auto& storeOption = store->mStoreOption;
  // start all worker threads
  mWorkers.resize(storeOption.mNumTxWorkers);
  mWorkerThreads.reserve(storeOption.mNumTxWorkers);
  for (uint64_t workerId = 0; workerId < storeOption.mNumTxWorkers;
       workerId++) {
    auto workerThread = std::make_unique<WorkerThread>(workerId, workerId);
    workerThread->Start();

    // create thread-local transaction executor on each worker thread
    workerThread->SetJob([&]() {
      Worker::sTlsWorker = std::make_unique<Worker>(workerId, mWorkers, mStore);
      Worker::sTlsWorkerRaw = Worker::sTlsWorker.get();
      mWorkers[workerId] = Worker::sTlsWorker.get();
    });
    workerThread->Wait();
    mWorkerThreads.emplace_back(std::move(workerThread));
  }

  // start group commit thread
  if (FLAGS_wal) {
    const int cpu = storeOption.mNumTxWorkers;
    mGroupCommitter =
        std::make_unique<GroupCommitter>(mStore->mWalFd, mWorkers, cpu);
    mGroupCommitter->Start();
  }

  // create history tree for each worker
  mWorkerThreads[0]->SetJob([&]() { setupHistoryTree(); });
  mWorkerThreads[0]->Wait();
  for (uint64_t workerId = 0; workerId < storeOption.mNumTxWorkers;
       workerId++) {
    mWorkers[workerId]->cc.mHistoryTree = mHistoryTreePtr.get();
  }
}

void CRManager::Stop() {
  mGroupCommitter->Stop();
  mWorkerThreads.clear();
}

CRManager::~CRManager() {
  Stop();
}

void CRManager::setupHistoryTree() {
  auto historyTree =
      std::make_unique<HistoryTree>(mStore->mStoreOption.mNumTxWorkers);

  for (uint64_t i = 0; i < mStore->mStoreOption.mNumTxWorkers; i++) {
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

constexpr char kKeyWalSize[] = "wal_size";
constexpr char kKeyGlobalLogicalClock[] = "global_logical_clock";

StringMap CRManager::Serialize() {
  StringMap map;
  uint64_t val = mStore->mTimestampOracle.load();
  map[kKeyWalSize] = std::to_string(mGroupCommitter->mWalSize);
  map[kKeyGlobalLogicalClock] = std::to_string(val);
  return map;
}

void CRManager::Deserialize(StringMap map) {
  uint64_t val = std::stoull(map[kKeyGlobalLogicalClock]);
  mStore->mTimestampOracle = val;
  mStore->mCRManager->mGlobalWmkInfo.mWmkOfAllTx = val;
  mGroupCommitter->mWalSize = std::stoull(map[kKeyWalSize]);
}

} // namespace leanstore::cr
