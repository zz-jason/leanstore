#include "ConcurrencyControl.hpp"

#include "CRMG.hpp"
#include "Config.hpp"
#include "Worker.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "shared-headers/Exceptions.hpp"
#include "shared-headers/Units.hpp"
#include "storage/buffer-manager/TreeRegistry.hpp"
#include "utils/Defer.hpp"
#include "utils/Misc.hpp"
#include "utils/RandomGenerator.hpp"

#include "glog/logging.h"

#include <atomic>
#include <mutex>
#include <set>
#include <shared_mutex>

namespace leanstore::cr {

//------------------------------------------------------------------------------
// CommitTree
//------------------------------------------------------------------------------

void CommitTree::AppendCommitLog(TXID startTs, TXID commitTs) {
  DCHECK(mCommitLog.size() < mCapacity);
  utils::Timer timer(CRCounters::MyCounters().cc_ms_committing);
  std::unique_lock xGuard(mMutex);
  mCommitLog.push_back({commitTs, startTs});
  DLOG(INFO) << "Commit log appended"
             << ", workerId=" << Worker::My().mWorkerId
             << ", startTs=" << startTs << ", commitTs=" << commitTs;
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
    DLOG(INFO) << "Commit log cleaned up"
               << ", workerId=" << Worker::My().mWorkerId
               << ", mCommitLog.size()=" << mCommitLog.size();
    for (auto [commitTs, startTs] : mCommitLog) {
      DLOG(INFO) << "Commit log entry:"
                 << " startTs=" << startTs << ", commitTs=" << commitTs;
    }
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
  DCHECK(it->second < startTs);
  return *it;
}

//------------------------------------------------------------------------------
// ConcurrencyControl
//------------------------------------------------------------------------------

COMMANDID ConcurrencyControl::PutVersion(TREEID treeId, bool isRemoveCommand,
                                         u64 versionSize,
                                         std::function<void(u8*)> putCallBack) {
  utils::Timer timer(CRCounters::MyCounters().cc_ms_history_tree_insert);
  auto& curWorker = Worker::My();
  auto commandId = curWorker.mCommandId++;
  if (isRemoveCommand) {
    commandId |= TYPE_MSB(COMMANDID);
  }
  mHistoryTree->PutVersion(curWorker.mWorkerId, curWorker.mActiveTx.mStartTs,
                           commandId, treeId, isRemoveCommand, versionSize,
                           putCallBack);
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
    DLOG(FATAL) << "Unsupported isolation level: "
                << static_cast<u64>(ActiveTx().mTxIsolationLevel);
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
  if (!FLAGS_enable_garbage_collection) {
    return;
  }

  updateGlobalTxWatermarks();
  updateLocalWatermarks();

  // remove versions that are nolonger needed by any transaction
  if (mCleanedWmkOfShortTx <= mLocalWmkOfAllTx) {
    utils::Timer timer(CRCounters::MyCounters().cc_ms_gc_history_tree);
    DLOG(INFO) << "Garbage collect history tree"
               << ", workerId=" << Worker::My().mWorkerId << ", fromTxId=" << 0
               << ", toTxId(mLocalWmkOfAllTx)=" << mLocalWmkOfAllTx
               << ", mCleanedWmkOfShortTx=" << mCleanedWmkOfShortTx;
    mStore->mCRManager->mHistoryTreePtr->PurgeVersions(
        Worker::My().mWorkerId, 0, mLocalWmkOfAllTx,
        [&](const TXID versionTxId, const TREEID treeId, const u8* versionData,
            u64 versionSize [[maybe_unused]], const bool calledBefore) {
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
    DLOG(INFO) << "Skip garbage collect history tree"
               << ", workerId=" << Worker::My().mWorkerId
               << ", mCleanedWmkOfShortTx=" << mCleanedWmkOfShortTx
               << ", mLocalWmkOfAllTx=" << mLocalWmkOfAllTx;
  }

  // move tombstones to graveyard
  if (FLAGS_enable_long_running_transaction &&
      mLocalWmkOfAllTx < mLocalWmkOfShortTx &&
      mCleanedWmkOfShortTx <= mLocalWmkOfShortTx) {
    utils::Timer timer(CRCounters::MyCounters().cc_ms_gc_graveyard);
    DLOG(INFO) << "Garbage collect removed versions"
               << ", workerId=" << Worker::My().mWorkerId
               << ", fromTxId=" << mCleanedWmkOfShortTx
               << ", toTxId(mLocalWmkOfShortTx)=" << mLocalWmkOfShortTx;
    mStore->mCRManager->mHistoryTreePtr->VisitRemovedVersions(
        Worker::My().mWorkerId, mCleanedWmkOfShortTx, mLocalWmkOfShortTx,
        [&](const TXID versionTxId, const TREEID treeId, const u8* versionData,
            u64, const bool calledBefore) {
          mStore->mTreeRegistry->GarbageCollect(treeId, versionData,
                                                Worker::My().mWorkerId,
                                                versionTxId, calledBefore);
          COUNTERS_BLOCK() {
            WorkerCounters::MyCounters().cc_todo_oltp_executed[treeId]++;
          }
        });
    mCleanedWmkOfShortTx = mLocalWmkOfShortTx + 1;
  } else {
    DLOG(INFO) << "Skip garbage collect removed versions"
               << ", workerId=" << Worker::My().mWorkerId
               << ", mCleanedWmkOfShortTx=" << mCleanedWmkOfShortTx
               << ", mLocalWmkOfAllTx=" << mLocalWmkOfAllTx
               << ", mLocalWmkOfShortTx=" << mLocalWmkOfShortTx;
  }
}

ConcurrencyControl& ConcurrencyControl::Other(WORKERID otherWorkerId) {
  return Worker::My().mAllWorkers[otherWorkerId]->cc;
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
  if (!FLAGS_enable_garbage_collection) {
    DLOG(INFO) << "Skip updating global watermarks, GC is disabled";
    return;
  }

  utils::Timer timer(CRCounters::MyCounters().cc_ms_refresh_global_state);
  auto meetGcProbability =
      FLAGS_enable_eager_garbage_collection ||
      utils::RandomGenerator::RandU64(0, Worker::My().mAllWorkers.size()) == 0;
  auto performGc = meetGcProbability &&
                   mStore->mCRManager->mGlobalWmkInfo.mGlobalMutex.try_lock();
  if (!performGc) {
    DLOG(INFO) << "Skip updating global watermarks"
               << ", meetGcProbability=" << meetGcProbability
               << ", performGc=" << performGc;
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

  LOG_IF(FATAL, !FLAGS_enable_long_running_transaction &&
                    mStore->mCRManager->mGlobalWmkInfo.mOldestActiveTx !=
                        mStore->mCRManager->mGlobalWmkInfo.mOldestActiveShortTx)
      << "Oldest transaction id should be equal to the oldest short-running "
         "transaction id when long-running transaction is disabled";

  // Update global lower watermarks based on the three transaction ids
  TXID globalWmkOfAllTx = std::numeric_limits<TXID>::max();
  TXID globalWmkOfShortTx = std::numeric_limits<TXID>::max();
  for (WORKERID i = 0; i < Worker::My().mAllWorkers.size(); i++) {
    ConcurrencyControl& cc = Other(i);
    if (cc.mUpdatedLatestCommitTs == cc.mLatestCommitTs) {
      DLOG(INFO) << "Skip updating watermarks for worker " << i
                 << ", no transaction committed since last round"
                 << ", mLatestCommitTs=" << cc.mLatestCommitTs;
      TXID wmkOfAllTx = cc.mWmkOfAllTx;
      TXID wmkOfShortTx = cc.mWmkOfShortTx;
      if (wmkOfAllTx > 0 || wmkOfShortTx > 0) {
        globalWmkOfAllTx = std::min(wmkOfAllTx, globalWmkOfAllTx);
        globalWmkOfShortTx = std::min(wmkOfShortTx, globalWmkOfShortTx);
      }
      continue;
    }

    TXID wmkOfAllTx =
        cc.mCommitTree.Lcb(mStore->mCRManager->mGlobalWmkInfo.mOldestActiveTx);
    TXID wmkOfShortTx = cc.mCommitTree.Lcb(
        mStore->mCRManager->mGlobalWmkInfo.mOldestActiveShortTx);

    cc.mWmkVersion.store(cc.mWmkVersion.load() + 1, std::memory_order_release);
    cc.mWmkOfAllTx.store(wmkOfAllTx, std::memory_order_release);
    cc.mWmkOfShortTx.store(wmkOfShortTx, std::memory_order_release);
    cc.mWmkVersion.store(cc.mWmkVersion.load() + 1, std::memory_order_release);
    cc.mUpdatedLatestCommitTs.store(cc.mLatestCommitTs,
                                    std::memory_order_release);
    DLOG(INFO) << "Watermarks updated for worker " << i << ", mWmkOfAllTx=LCB("
               << wmkOfAllTx << ")=" << cc.mWmkOfAllTx << ", mWmkOfShortTx=LCB("
               << wmkOfShortTx << ")=" << cc.mWmkOfShortTx;

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
    DLOG(INFO) << "Skip updating global watermarks"
               << ", global watermarks are the same as last round"
               << ", globalWmkOfAllTx=" << globalWmkOfAllTx
               << ", globalWmkOfShortTx=" << globalWmkOfShortTx;
    return;
  }

  // TXID globalWmkOfAllTx = std::numeric_limits<TXID>::max();
  // TXID globalWmkOfShortTx = std::numeric_limits<TXID>::max();
  if (globalWmkOfAllTx == std::numeric_limits<TXID>::max() ||
      globalWmkOfShortTx == std::numeric_limits<TXID>::max()) {
    DLOG(INFO) << "Skip updating global watermarks"
               << ", can not find any valid lower watermarks"
               << ", globalWmkOfAllTx=" << globalWmkOfAllTx
               << ", globalWmkOfShortTx=" << globalWmkOfShortTx;
    return;
  }

  mStore->mCRManager->mGlobalWmkInfo.UpdateWmks(globalWmkOfAllTx,
                                                globalWmkOfShortTx);
}

void ConcurrencyControl::updateLocalWatermarks() {
  SCOPED_DEFER(DLOG(INFO) << "Local watermarks updated"
                          << ", workerId=" << Worker::My().mWorkerId
                          << ", mLocalWmkOfAllTx=" << mLocalWmkOfAllTx
                          << ", mLocalWmkOfShortTx=" << mLocalWmkOfShortTx);
  while (true) {
    u64 version = mWmkVersion.load();

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

  DCHECK(!FLAGS_enable_long_running_transaction ||
         mLocalWmkOfAllTx <= mLocalWmkOfShortTx)
      << "Lower watermark of all transactions should be no higher than the "
         "lower watermark of short-running transactions"
      << ", workerId=" << Worker::My().mWorkerId
      << ", mLocalWmkOfAllTx=" << mLocalWmkOfAllTx
      << ", mLocalWmkOfShortTx=" << mLocalWmkOfShortTx;
}

} // namespace leanstore::cr
