#include "concurrency/CRManager.hpp"

#include "btree/BasicKV.hpp"
#include "concurrency/GroupCommitter.hpp"
#include "concurrency/HistoryStorage.hpp"
#include "concurrency/Worker.hpp"
#include "concurrency/WorkerThread.hpp"
#include "leanstore/LeanStore.hpp"

#include <glog/logging.h>

#include <memory>
#include <vector>

namespace leanstore::cr {

CRManager::CRManager(leanstore::LeanStore* store)
    : mStore(store),
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

  // create history storage for each worker
  // History tree should be created after worker thread and group committer are
  // started.
  mWorkerThreads[0]->SetJob([&]() { setupHistoryStorage4EachWorker(); });
  mWorkerThreads[0]->Wait();
}

void CRManager::Stop() {
  mGroupCommitter->Stop();
  mWorkerThreads.clear();
}

CRManager::~CRManager() {
  Stop();
}

void CRManager::setupHistoryStorage4EachWorker() {
  for (uint64_t i = 0; i < mStore->mStoreOption.mNumTxWorkers; i++) {
    storage::btree::BTreeConfig config = {.mEnableWal = false,
                                          .mUseBulkInsert = true};
    // setup update tree
    std::string updateBtreeName = std::format("_history_tree_{}_updates", i);
    auto res = storage::btree::BasicKV::Create(mStore, updateBtreeName, config);
    if (!res) {
      LOG(FATAL) << "Failed to set up update history tree"
                 << ", updateBTreeName=" << updateBtreeName
                 << ", error=" << res.error().ToString();
    }
    auto* updateIndex = res.value();

    // setup delete tree
    std::string removeBtreeName = std::format("_history_tree_{}_removes", i);
    res = storage::btree::BasicKV::Create(mStore, removeBtreeName, config);
    if (!res) {
      LOG(FATAL) << "Failed to set up remove history tree"
                 << ", removeBtreeName=" << removeBtreeName
                 << ", error=" << res.error().ToString();
    }
    auto* removeIndex = res.value();
    mWorkers[i]->mCc.mHistoryStorage.SetUpdateIndex(updateIndex);
    mWorkers[i]->mCc.mHistoryStorage.SetRemoveIndex(removeIndex);
  }
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
