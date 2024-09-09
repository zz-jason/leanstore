#include "leanstore/concurrency/CRManager.hpp"

#include "leanstore/LeanStore.hpp"
#include "leanstore/btree/BasicKV.hpp"
#include "leanstore/concurrency/HistoryStorage.hpp"
#include "leanstore/concurrency/WorkerContext.hpp"
#include "leanstore/concurrency/WorkerThread.hpp"
#include "leanstore/utils/Log.hpp"

#include <memory>
#include <vector>

namespace leanstore::cr {

CRManager::CRManager(leanstore::LeanStore* store) : mStore(store) {
  auto* storeOption = store->mStoreOption;
  // start all worker threads
  mWorkerCtxs.resize(storeOption->mWorkerThreads);
  mWorkerThreads.reserve(storeOption->mWorkerThreads);
  for (uint64_t workerId = 0; workerId < storeOption->mWorkerThreads; workerId++) {
    auto workerThread = std::make_unique<WorkerThread>(store, workerId, workerId);
    workerThread->Start();

    // create thread-local transaction executor on each worker thread
    workerThread->SetJob([&]() {
      WorkerContext::sTlsWorkerCtx = std::make_unique<WorkerContext>(workerId, mWorkerCtxs, mStore);
      WorkerContext::sTlsWorkerCtxRaw = WorkerContext::sTlsWorkerCtx.get();
      mWorkerCtxs[workerId] = WorkerContext::sTlsWorkerCtx.get();
    });
    workerThread->Wait();
    mWorkerThreads.emplace_back(std::move(workerThread));
  }

  // create history storage for each worker
  // History tree should be created after worker thread and group committer are
  // started.
  if (storeOption->mWorkerThreads > 0) {
    mWorkerThreads[0]->SetJob([&]() { setupHistoryStorage4EachWorker(); });
    mWorkerThreads[0]->Wait();
  }
}

void CRManager::Stop() {
  for (auto& workerThread : mWorkerThreads) {
    workerThread->Stop();
  }
  mWorkerThreads.clear();
}

CRManager::~CRManager() {
  Stop();
}

void CRManager::setupHistoryStorage4EachWorker() {
  for (uint64_t i = 0; i < mStore->mStoreOption->mWorkerThreads; i++) {
    // setup update tree
    std::string updateBtreeName = std::format("_history_tree_{}_updates", i);
    auto res = storage::btree::BasicKV::Create(
        mStore, updateBtreeName, BTreeConfig{.mEnableWal = false, .mUseBulkInsert = true});
    if (!res) {
      Log::Fatal("Failed to set up update history tree, updateBtreeName={}, error={}",
                 updateBtreeName, res.error().ToString());
    }
    auto* updateIndex = res.value();

    // setup delete tree
    std::string removeBtreeName = std::format("_history_tree_{}_removes", i);
    res = storage::btree::BasicKV::Create(mStore, removeBtreeName,
                                          BTreeConfig{.mEnableWal = false, .mUseBulkInsert = true});
    if (!res) {
      Log::Fatal("Failed to set up remove history tree, removeBtreeName={}, error={}",
                 removeBtreeName, res.error().ToString());
    }
    auto* removeIndex = res.value();
    mWorkerCtxs[i]->mCc.mHistoryStorage.SetUpdateIndex(updateIndex);
    mWorkerCtxs[i]->mCc.mHistoryStorage.SetRemoveIndex(removeIndex);
  }
}

constexpr char kKeyGlobalUsrTso[] = "global_user_tso";
constexpr char kKeyGlobalSysTso[] = "global_system_tso";

StringMap CRManager::Serialize() {
  StringMap map;
  map[kKeyGlobalUsrTso] = std::to_string(mStore->mUsrTso.load());
  map[kKeyGlobalSysTso] = std::to_string(mStore->mSysTso.load());
  return map;
}

void CRManager::Deserialize(StringMap map) {
  mStore->mUsrTso = std::stoull(map[kKeyGlobalUsrTso]);
  mStore->mSysTso = std::stoull(map[kKeyGlobalSysTso]);

  mStore->mCRManager->mGlobalWmkInfo.mWmkOfAllTx = mStore->mUsrTso.load();
}

} // namespace leanstore::cr
