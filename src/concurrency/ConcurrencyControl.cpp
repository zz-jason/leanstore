#include "concurrency/ConcurrencyControl.hpp"

#include "buffer-manager/TreeRegistry.hpp"
#include "concurrency/CRManager.hpp"
#include "concurrency/Worker.hpp"
#include "leanstore/Exceptions.hpp"
#include "leanstore/Units.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "utils/Defer.hpp"
#include "utils/Log.hpp"
#include "utils/Misc.hpp"
#include "utils/RandomGenerator.hpp"

#include <atomic>
#include <mutex>
#include <set>
#include <shared_mutex>

namespace leanstore::cr {

//------------------------------------------------------------------------------
// CommitTree
//------------------------------------------------------------------------------

void CommitTree::AppendCommitLog(TXID startTs, TXID commitTs) {
  LS_DCHECK(mCommitLog.size() < mCapacity);
  utils::Timer timer(CRCounters::MyCounters().cc_ms_committing);
  std::unique_lock xGuard(mMutex);
  mCommitLog.push_back({commitTs, startTs});
  LS_DLOG("Commit log appended, workerId={}, startTs={}, commitTs={}",
          Worker::My().mWorkerId, startTs, commitTs);
}

void CommitTree::CompactCommitLog() {
  utils::Timer timer(CRCounters::MyCounters().cc_ms_gc_cm);
  if (mCommitLog.size() < mCapacity) {
    return;
  }

  // Calculate the compacted commit log.
  std::set<std::pair<TXID, TXID>> set;

  // Keep the latest (commitTs, startTs) in the commit log, so that other
  // workers can see the latest commitTs of this worker.
  set.insert(mCommitLog[mCommitLog.size() - 1]);

  const WORKERID myWorkerId = Worker::My().mWorkerId;
  auto allWorkers = Worker::My().mAllWorkers;
  for (WORKERID i = 0; i < Worker::My().mAllWorkers.size(); i++) {
    if (i == myWorkerId) {
      continue;
    }

    auto activeTxId = allWorkers[i]->mActiveTxId.load();
    if (activeTxId == 0) {
      // Don't need to keep the old commit log entry if the worker is not
      // running any transaction.
      continue;
    }

    activeTxId &= Worker::kCleanBitsMask;
    if (auto result = lcbNoLatch(activeTxId); result) {
      set.insert(*result);
    }
  }

  // Refill the compacted commit log
  std::unique_lock xGuard(mMutex);
  mCommitLog.clear();
  for (auto& p : set) {
    mCommitLog.push_back(p);
  }

  DEBUG_BLOCK() {
    LS_DLOG("Commit log cleaned up, workerId={}, mCommitLog.size()={}",
            Worker::My().mWorkerId, mCommitLog.size());
  }
}

TXID CommitTree::Lcb(TXID startTs) {
  std::shared_lock guard(mMutex);

  if (auto result = lcbNoLatch(startTs); result) {
    return result->second;
  }
  return 0;
}

std::optional<std::pair<TXID, TXID>> CommitTree::lcbNoLatch(TXID startTs) {
  auto comp = [&](const auto& pair, TXID startTs) {
    return startTs > pair.first;
  };
  auto it =
      std::lower_bound(mCommitLog.begin(), mCommitLog.end(), startTs, comp);
  if (it == mCommitLog.begin()) {
    return {};
  }
  it--;
  LS_DCHECK(it->second < startTs);
  return *it;
}

//------------------------------------------------------------------------------
// ConcurrencyControl
//------------------------------------------------------------------------------

COMMANDID ConcurrencyControl::PutVersion(
    TREEID treeId, bool isRemoveCommand, uint64_t versionSize,
    std::function<void(uint8_t*)> putCallBack) {
  utils::Timer timer(CRCounters::MyCounters().cc_ms_history_tree_insert);
  auto& curWorker = Worker::My();
  auto commandId = curWorker.mCommandId++;
  if (isRemoveCommand) {
    commandId |= kRemoveCommandMark;
  }
  mHistoryStorage.PutVersion(curWorker.mActiveTx.mStartTs, commandId, treeId,
                             isRemoveCommand, versionSize, putCallBack);
  return commandId;
}

bool ConcurrencyControl::VisibleForMe(WORKERID workerId, TXID txId) {
  // visible if writtern by me
  if (Worker::My().mWorkerId == workerId) {
    return true;
  }

  switch (ActiveTx().mTxIsolationLevel) {
  case IsolationLevel::kSnapshotIsolation:
  case IsolationLevel::kSerializable: {
    // mGlobalWmkOfAllTx is copied from global watermark info at the beginning
    // of each transaction. Global watermarks are occassionally updated by
    // Worker::updateGlobalTxWatermarks, it's possible that mGlobalWmkOfAllTx is
    // not the latest value, but it is always safe to use it as the lower bound
    // of the visibility check.
    if (txId < mGlobalWmkOfAllTx) {
      return true;
    }

    // If we have queried the LCB on the target worker and cached the value in
    // mLcbCacheVal, we can use it to check the visibility directly.
    if (mLcbCacheKey[workerId] == ActiveTx().mStartTs) {
      return mLcbCacheVal[workerId] >= txId;
    }

    // If the tuple is visible for the last transaction, it is visible for the
    // current transaction as well. No need to query LCB on the target worker.
    if (mLcbCacheVal[workerId] >= txId) {
      return true;
    }

    // Now we need to query LCB on the target worker and update the local cache.
    utils::Timer timer(CRCounters::MyCounters().cc_ms_snapshotting);
    TXID largestVisibleTxId =
        Other(workerId).mCommitTree.Lcb(ActiveTx().mStartTs);
    if (largestVisibleTxId) {
      mLcbCacheKey[workerId] = ActiveTx().mStartTs;
      mLcbCacheVal[workerId] = largestVisibleTxId;
      return largestVisibleTxId >= txId;
    }

    return false;
  }
  default: {
    Log::Fatal("Unsupported isolation level: {}",
               static_cast<uint64_t>(ActiveTx().mTxIsolationLevel));
  }
  }
  return false;
}

bool ConcurrencyControl::VisibleForAll(TXID txId) {
  return txId < mStore->mCRManager->mGlobalWmkInfo.mWmkOfAllTx.load();
}

// TODO: smooth purge, we should not let the system hang on this, as a quick
// fix, it should be enough if we purge in small batches
void ConcurrencyControl::GarbageCollection() {
  utils::Timer timer(CRCounters::MyCounters().cc_ms_gc);
  if (!mStore->mStoreOption.mEnableGc) {
    return;
  }

  updateGlobalTxWatermarks();
  updateLocalWatermarks();

  // remove versions that are nolonger needed by any transaction
  if (mCleanedWmkOfShortTx <= mLocalWmkOfAllTx) {
    utils::Timer timer(CRCounters::MyCounters().cc_ms_gc_history_tree);
    LS_DLOG("Garbage collect history tree"
            ", workerId={}, fromTxId={}, toTxId(mLocalWmkOfAllTx)={}",
            Worker::My().mWorkerId, 0, mLocalWmkOfAllTx);
    mHistoryStorage.PurgeVersions(
        0, mLocalWmkOfAllTx,
        [&](const TXID versionTxId, const TREEID treeId,
            const uint8_t* versionData, uint64_t versionSize [[maybe_unused]],
            const bool calledBefore) {
          mStore->mTreeRegistry->GarbageCollect(treeId, versionData,
                                                Worker::My().mWorkerId,
                                                versionTxId, calledBefore);
          COUNTERS_BLOCK() {
            WorkerCounters::MyCounters().cc_gc_long_tx_executed[treeId]++;
          }
        },
        0);
    mCleanedWmkOfShortTx = mLocalWmkOfAllTx + 1;
  } else {
    LS_DLOG("Skip garbage collect history tree, workerId={}, "
            "mCleanedWmkOfShortTx={}, mLocalWmkOfAllTx={}",
            Worker::My().mWorkerId, mCleanedWmkOfShortTx, mLocalWmkOfAllTx);
  }

  // move tombstones to graveyard
  if (mStore->mStoreOption.mEnableLongRunningTx &&
      mLocalWmkOfAllTx < mLocalWmkOfShortTx &&
      mCleanedWmkOfShortTx <= mLocalWmkOfShortTx) {
    utils::Timer timer(CRCounters::MyCounters().cc_ms_gc_graveyard);
    LS_DLOG("Garbage collect graveyard, workerId={}, fromTxId={}, "
            "toTxId(mLocalWmkOfShortTx)={}",
            Worker::My().mWorkerId, mCleanedWmkOfShortTx, mLocalWmkOfShortTx);
    mHistoryStorage.VisitRemovedVersions(
        mCleanedWmkOfShortTx, mLocalWmkOfShortTx,
        [&](const TXID versionTxId, const TREEID treeId,
            const uint8_t* versionData, uint64_t, const bool calledBefore) {
          mStore->mTreeRegistry->GarbageCollect(treeId, versionData,
                                                Worker::My().mWorkerId,
                                                versionTxId, calledBefore);
          COUNTERS_BLOCK() {
            WorkerCounters::MyCounters().cc_todo_oltp_executed[treeId]++;
          }
        });
    mCleanedWmkOfShortTx = mLocalWmkOfShortTx + 1;
  } else {
    LS_DLOG("Skip garbage collect graveyard, workerId={}, "
            "mCleanedWmkOfShortTx={}, mLocalWmkOfShortTx={}",
            Worker::My().mWorkerId, mCleanedWmkOfShortTx, mLocalWmkOfShortTx);
  }
}

ConcurrencyControl& ConcurrencyControl::Other(WORKERID otherWorkerId) {
  return Worker::My().mAllWorkers[otherWorkerId]->mCc;
}

// It calculates and updates the global oldest running transaction id and the
// oldest running short-running transaction id. Based on these two oldest
// running transaction ids, it calculates and updates the global watermarks of
// all transactions and short-running transactions, under which all transactions
// and short-running transactions are visible, and versions older than the
// watermarks can be garbage collected.
//
// Called by the worker thread that is committing a transaction before garbage
// collection.
void ConcurrencyControl::updateGlobalTxWatermarks() {
  if (!mStore->mStoreOption.mEnableGc) {
    LS_DLOG("Skip updating global watermarks, GC is disabled");
    return;
  }

  utils::Timer timer(CRCounters::MyCounters().cc_ms_refresh_global_state);
  auto meetGcProbability =
      mStore->mStoreOption.mEnableEagerGc ||
      utils::RandomGenerator::RandU64(0, Worker::My().mAllWorkers.size()) == 0;
  auto performGc = meetGcProbability &&
                   mStore->mCRManager->mGlobalWmkInfo.mGlobalMutex.try_lock();
  if (!performGc) {
    LS_DLOG(
        "Skip updating global watermarks, meetGcProbability={}, performGc={}",
        meetGcProbability, performGc);
    return;
  }

  // release the lock on exit
  SCOPED_DEFER(mStore->mCRManager->mGlobalWmkInfo.mGlobalMutex.unlock());

  // There is a chance that oldestTxId or oldestShortTxId is
  // std::numeric_limits<TXID>::max(). It is ok because LCB(+oo) returns the id
  // of latest committed transaction. Under this condition, all the tombstones
  // or update versions generated by the previous transactions can be garbage
  // collected, i.e. removed or moved to graveyard.
  TXID oldestTxId = std::numeric_limits<TXID>::max();
  TXID newestLongTxId = std::numeric_limits<TXID>::min();
  TXID oldestShortTxId = std::numeric_limits<TXID>::max();
  auto allWorkers = Worker::My().mAllWorkers;
  for (WORKERID i = 0; i < Worker::My().mAllWorkers.size(); i++) {
    auto activeTxId = allWorkers[i]->mActiveTxId.load();
    // Skip transactions not running.
    if (activeTxId == 0) {
      continue;
    }
    // Skip transactions running in read-committed mode.
    if (activeTxId & Worker::kRcBit) {
      continue;
    }

    bool isLongRunningTx = activeTxId & Worker::kLongRunningBit;
    activeTxId &= Worker::kCleanBitsMask;
    oldestTxId = std::min(activeTxId, oldestTxId);
    if (isLongRunningTx) {
      newestLongTxId = std::max(activeTxId, newestLongTxId);
    } else {
      oldestShortTxId = std::min(activeTxId, oldestShortTxId);
    }
  }

  // Update the three transaction ids
  mStore->mCRManager->mGlobalWmkInfo.UpdateActiveTxInfo(
      oldestTxId, oldestShortTxId, newestLongTxId);

  if (!mStore->mStoreOption.mEnableLongRunningTx &&
      mStore->mCRManager->mGlobalWmkInfo.mOldestActiveTx !=
          mStore->mCRManager->mGlobalWmkInfo.mOldestActiveShortTx) {
    Log::Fatal("Oldest transaction id should be equal to the oldest "
               "short-running transaction id when long-running transaction is "
               "disabled");
  }

  // Update global lower watermarks based on the three transaction ids
  TXID globalWmkOfAllTx = std::numeric_limits<TXID>::max();
  TXID globalWmkOfShortTx = std::numeric_limits<TXID>::max();
  for (WORKERID i = 0; i < Worker::My().mAllWorkers.size(); i++) {
    ConcurrencyControl& mCc = Other(i);
    if (mCc.mUpdatedLatestCommitTs == mCc.mLatestCommitTs) {
      LS_DLOG("Skip updating watermarks for worker {}, no transaction "
              "committed since last round, mLatestCommitTs={}",
              i, mCc.mLatestCommitTs.load());
      TXID wmkOfAllTx = mCc.mWmkOfAllTx;
      TXID wmkOfShortTx = mCc.mWmkOfShortTx;
      if (wmkOfAllTx > 0 || wmkOfShortTx > 0) {
        globalWmkOfAllTx = std::min(wmkOfAllTx, globalWmkOfAllTx);
        globalWmkOfShortTx = std::min(wmkOfShortTx, globalWmkOfShortTx);
      }
      continue;
    }

    TXID wmkOfAllTx =
        mCc.mCommitTree.Lcb(mStore->mCRManager->mGlobalWmkInfo.mOldestActiveTx);
    TXID wmkOfShortTx = mCc.mCommitTree.Lcb(
        mStore->mCRManager->mGlobalWmkInfo.mOldestActiveShortTx);

    mCc.mWmkVersion.store(mCc.mWmkVersion.load() + 1,
                          std::memory_order_release);
    mCc.mWmkOfAllTx.store(wmkOfAllTx, std::memory_order_release);
    mCc.mWmkOfShortTx.store(wmkOfShortTx, std::memory_order_release);
    mCc.mWmkVersion.store(mCc.mWmkVersion.load() + 1,
                          std::memory_order_release);
    mCc.mUpdatedLatestCommitTs.store(mCc.mLatestCommitTs,
                                     std::memory_order_release);
    LS_DLOG("Watermarks updated for worker {}, mWmkOfAllTx=LCB({})={}, "
            "mWmkOfShortTx=LCB({})={}",
            i, wmkOfAllTx, mCc.mWmkOfAllTx.load(), wmkOfShortTx,
            mCc.mWmkOfShortTx.load());

    // The lower watermarks of current worker only matters when there are
    // transactions started before global oldestActiveTx
    if (wmkOfAllTx > 0 || wmkOfShortTx > 0) {
      globalWmkOfAllTx = std::min(wmkOfAllTx, globalWmkOfAllTx);
      globalWmkOfShortTx = std::min(wmkOfShortTx, globalWmkOfShortTx);
    }
  }

  // If a worker hasn't committed any new transaction since last round, the
  // commit log keeps the same, which causes the lower watermarks the same
  // as last round, which further causes the global lower watermarks the
  // same as last round. This is not a problem, but updating the global
  // lower watermarks is not necessary in this case.
  if (mStore->mCRManager->mGlobalWmkInfo.mWmkOfAllTx == globalWmkOfAllTx &&
      mStore->mCRManager->mGlobalWmkInfo.mWmkOfShortTx == globalWmkOfShortTx) {
    LS_DLOG("Skip updating global watermarks, global watermarks are the "
            "same as last round, globalWmkOfAllTx={}, globalWmkOfShortTx={}",
            globalWmkOfAllTx, globalWmkOfShortTx);
    return;
  }

  // TXID globalWmkOfAllTx = std::numeric_limits<TXID>::max();
  // TXID globalWmkOfShortTx = std::numeric_limits<TXID>::max();
  if (globalWmkOfAllTx == std::numeric_limits<TXID>::max() ||
      globalWmkOfShortTx == std::numeric_limits<TXID>::max()) {
    LS_DLOG("Skip updating global watermarks, can not find any valid lower "
            "watermarks, globalWmkOfAllTx={}, globalWmkOfShortTx={}",
            globalWmkOfAllTx, globalWmkOfShortTx);
    return;
  }

  mStore->mCRManager->mGlobalWmkInfo.UpdateWmks(globalWmkOfAllTx,
                                                globalWmkOfShortTx);
}

void ConcurrencyControl::updateLocalWatermarks() {
  SCOPED_DEFER(LS_DLOG("Local watermarks updated, workerId={}, "
                       "mLocalWmkOfAllTx={}, mLocalWmkOfShortTx={}",
                       Worker::My().mWorkerId, mLocalWmkOfAllTx,
                       mLocalWmkOfShortTx));
  while (true) {
    uint64_t version = mWmkVersion.load();

    // spin until the latch is free
    while ((version = mWmkVersion.load()) & 1) {
    };

    // update the two local watermarks
    mLocalWmkOfAllTx = mWmkOfAllTx.load();
    mLocalWmkOfShortTx = mWmkOfShortTx.load();

    // restart if the latch was taken
    if (version == mWmkVersion.load()) {
      return;
    }
  }

  LS_DCHECK(
      !mStore->mStoreOption.mEnableLongRunningTx ||
          mLocalWmkOfAllTx <= mLocalWmkOfShortTx,
      "Lower watermark of all transactions should be no higher than the lower "
      "watermark of short-running transactions, workerId={}, "
      "mLocalWmkOfAllTx={}, mLocalWmkOfShortTx={}",
      Worker::My().mWorkerId, mLocalWmkOfAllTx, mLocalWmkOfShortTx);
}

} // namespace leanstore::cr
