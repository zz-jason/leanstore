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
        std::make_unique<GroupCommitter>(mStore, mStore->mWalFd, mWorkers, cpu);
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
constexpr char kKeyUsrTxTso[] = "usr_tx_tso";
constexpr char kKeySysTxTso[] = "sys_tx_tso";

StringMap CRManager::Serialize() {
  StringMap map;
  map[kKeyWalSize] = std::to_string(mGroupCommitter->mWalSize);
  map[kKeyUsrTxTso] = std::to_string(mStore->mUsrTxTso.load());
  map[kKeySysTxTso] = std::to_string(mStore->mSysTxTso.load());
  return map;
}

void CRManager::Deserialize(StringMap map) {
  uint64_t usrTxTso = std::stoull(map[kKeyUsrTxTso]);
  mStore->mUsrTxTso = usrTxTso;
  mStore->mCRManager->mGlobalWmkInfo.mWmkOfAllTx = usrTxTso;

  uint64_t sysTxTso = std::stoull(map[kKeySysTxTso]);
  mStore->mSysTxTso = sysTxTso;

  mGroupCommitter->mWalSize = std::stoull(map[kKeyWalSize]);
}

} // namespace leanstore::cr
