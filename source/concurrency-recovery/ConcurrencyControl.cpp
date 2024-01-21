#include "CRMG.hpp"
#include "Config.hpp"
#include "Worker.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "storage/buffer-manager/TreeRegistry.hpp"
#include "utils/Defer.hpp"
#include "utils/Misc.hpp"
#include "utils/RandomGenerator.hpp"

#include "glog/logging.h"

#include <atomic>
#include <set>

namespace leanstore::cr {

// Starts from a positive number, 0 is used for invalid timestamp
std::atomic<u64> ConcurrencyControl::sTimeStampOracle = 1;

// Also for interval garbage collection
void ConcurrencyControl::UpdateGlobalTxWatermarks() {
  if (!FLAGS_enable_garbage_collection) {
    DLOG(INFO) << "Skip updating global watermarks, GC is disabled";
    return;
  }

  utils::Timer timer(CRCounters::MyCounters().cc_ms_refresh_global_state);
  auto meetGcProbability =
      FLAGS_enable_eager_garbage_collection ||
      utils::RandomGenerator::RandU64(0, Worker::my().mNumAllWorkers) == 0;
  auto performGc = meetGcProbability && Worker::sGlobalMutex.try_lock();
  if (!performGc) {
    DLOG(INFO) << "Skip updating global watermarks"
               << ", meetGcProbability=" << meetGcProbability
               << ", performGc=" << performGc;
    return;
  }

  // release the lock on exit
  SCOPED_DEFER(Worker::sGlobalMutex.unlock());

  TXID oldestTxId = std::numeric_limits<TXID>::max();
  TXID newestLongTxId = std::numeric_limits<TXID>::min();
  TXID oldestShortTxId = std::numeric_limits<TXID>::max();
  for (WORKERID i = 0; i < Worker::my().mNumAllWorkers; i++) {
    u64 runningTxId = Worker::sLatestStartTs[i].load();
    // Skip transactions running in read-committed mode.
    if (runningTxId & Worker::kRcBit) {
      continue;
    }

    bool isLongRunningTx = runningTxId & Worker::kLongRunningBit;
    runningTxId &= Worker::kCleanBitsMask;
    oldestTxId = std::min(runningTxId, oldestTxId);
    if (isLongRunningTx) {
      newestLongTxId = std::max(runningTxId, newestLongTxId);
    } else {
      oldestShortTxId = std::min(runningTxId, oldestShortTxId);
    }
  }

  // Update the three transaction ids
  Worker::sGlobalOldestTxId.store(oldestTxId, std::memory_order_release);
  Worker::sGlobalNewestLongTxId.store(newestLongTxId,
                                      std::memory_order_release);
  Worker::sGlobalOldestShortTxId.store(oldestShortTxId,
                                       std::memory_order_release);

  DLOG(INFO) << "Global watermark updated"
             << ", sGlobalOldestTxId=" << Worker::sGlobalOldestTxId
             << ", sGlobalNewestLongTxId=" << Worker::sGlobalNewestLongTxId
             << ", sGlobalOldestShortTxId=" << Worker::sGlobalOldestShortTxId;

  LOG_IF(FATAL, !FLAGS_enable_long_running_transaction &&
                    Worker::sGlobalOldestTxId != Worker::sGlobalOldestShortTxId)
      << "Oldest transaction id should be equal to the oldest short-running "
         "transaction id when long-running transaction is disabled";

  // Update global lower watermarks based on the three transaction ids
  TXID globalWmkOfAllTx = std::numeric_limits<TXID>::max();
  TXID globalWmkOfShortTx = std::numeric_limits<TXID>::max();
  for (WORKERID i = 0; i < Worker::my().mNumAllWorkers; i++) {
    ConcurrencyControl& cc = other(i);
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

    TXID wmkOfAllTx = cc.mCommitTree.Lcb(Worker::sGlobalOldestTxId);
    TXID wmkOfShortTx = cc.mCommitTree.Lcb(Worker::sGlobalOldestShortTxId);

    cc.mWmkVersion.store(cc.mWmkVersion.load() + 1, std::memory_order_release);
    cc.mWmkOfAllTx.store(wmkOfAllTx, std::memory_order_release);
    cc.mWmkOfShortTx.store(wmkOfShortTx, std::memory_order_release);
    cc.mWmkVersion.store(cc.mWmkVersion.load() + 1, std::memory_order_release);
    cc.mUpdatedLatestCommitTs.store(cc.mLatestCommitTs,
                                    std::memory_order_release);
    DLOG(INFO) << "Watermarks updated for worker " << i << ", mWmkOfAllTx=LCB("
               << Worker::sGlobalOldestTxId << ")=" << cc.mWmkOfAllTx
               << ", mWmkOfShortTx=LCB(" << Worker::sGlobalOldestShortTxId
               << ")=" << cc.mWmkOfShortTx;

    // The lower watermarks of current worker only matters when there are
    // transactions started before Worker::sGlobalOldestTxId
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
  if (Worker::sGlobalWmkOfAllTx == globalWmkOfAllTx &&
      Worker::sGlobalWmkOfShortTx == globalWmkOfShortTx) {
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
  Worker::sGlobalWmkOfAllTx.store(globalWmkOfAllTx, std::memory_order_release);
  Worker::sGlobalWmkOfShortTx.store(globalWmkOfShortTx,
                                    std::memory_order_release);
  DLOG(INFO) << "Global watermarks updated"
             << ", sGlobalWmkOfAllTx=" << Worker::sGlobalWmkOfAllTx
             << ", sGlobalWmkOfShortTx=" << Worker::sGlobalWmkOfShortTx;
}

void ConcurrencyControl::updateLocalWatermarks() {
  SCOPED_DEFER(DLOG(INFO) << "Local watermarks updated"
                          << ", workerId=" << Worker::my().mWorkerId
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
      << ", workerId=" << Worker::my().mWorkerId
      << ", mLocalWmkOfAllTx=" << mLocalWmkOfAllTx
      << ", mLocalWmkOfShortTx=" << mLocalWmkOfShortTx;
}

// TODO: smooth purge, we should not let the system hang on this, as a quick
// fix, it should be enough if we purge in small batches
void ConcurrencyControl::GarbageCollection() {
  if (!FLAGS_enable_garbage_collection) {
    return;
  }

  utils::Timer timer(CRCounters::MyCounters().cc_ms_gc);
  updateLocalWatermarks();

  // remove versions that are nolonger needed by any transaction
  if (mCleanedWmkOfShortTx < mLocalWmkOfAllTx) {
    utils::Timer timer(CRCounters::MyCounters().cc_ms_gc_history_tree);
    auto onRemoveVersion =
        [&](const TXID versionTxId, const TREEID treeId, const u8* versionData,
            u64 versionSize [[maybe_unused]], const bool calledBefore) {
          leanstore::storage::TreeRegistry::sInstance->GarbageCollect(
              treeId, versionData, Worker::my().mWorkerId, versionTxId,
              calledBefore);
          COUNTERS_BLOCK() {
            WorkerCounters::MyCounters().cc_gc_long_tx_executed[treeId]++;
          }
        };
    DLOG(INFO) << "Garbage collect history tree"
               << ", workerId=" << Worker::my().mWorkerId << ", fromTxId=" << 0
               << ", toTxId(mLocalWmkOfAllTx-1)=" << mLocalWmkOfAllTx - 1
               << ", mCleanedWmkOfShortTx=" << mCleanedWmkOfShortTx;
    CRManager::sInstance->mHistoryTreePtr->PurgeVersions(
        Worker::my().mWorkerId, 0, mLocalWmkOfAllTx - 1, onRemoveVersion, 0);
    mCleanedWmkOfShortTx = mLocalWmkOfAllTx;
  } else {
    DLOG(INFO) << "Skip garbage collect history tree"
               << ", workerId=" << Worker::my().mWorkerId
               << ", mCleanedWmkOfShortTx=" << mCleanedWmkOfShortTx
               << ", mLocalWmkOfAllTx=" << mLocalWmkOfAllTx;
  }

  // move tombstones to graveyard
  if (FLAGS_enable_long_running_transaction &&
      mLocalWmkOfAllTx < mLocalWmkOfShortTx &&
      mCleanedWmkOfShortTx < mLocalWmkOfShortTx) {
    DLOG(INFO) << "Garbage collect removed versions"
               << ", workerId=" << Worker::my().mWorkerId
               << ", fromTxId=" << mCleanedWmkOfShortTx
               << ", toTxId(mLocalWmkOfShortTx-1)=" << mLocalWmkOfShortTx - 1;
    utils::Timer timer(CRCounters::MyCounters().cc_ms_gc_graveyard);
    auto onRemoveVersion = [&](const TXID versionTxId, const TREEID treeId,
                               const u8* versionData, u64,
                               const bool calledBefore) {
      leanstore::storage::TreeRegistry::sInstance->GarbageCollect(
          treeId, versionData, Worker::my().mWorkerId, versionTxId,
          calledBefore);
      COUNTERS_BLOCK() {
        WorkerCounters::MyCounters().cc_todo_oltp_executed[treeId]++;
      }
    };
    CRManager::sInstance->mHistoryTreePtr->VisitRemovedVersions(
        Worker::my().mWorkerId, mCleanedWmkOfShortTx, mLocalWmkOfShortTx - 1,
        onRemoveVersion);
    mCleanedWmkOfShortTx = mLocalWmkOfShortTx;
  } else {
    DLOG(INFO) << "Skip garbage collect removed versions"
               << ", workerId=" << Worker::my().mWorkerId
               << ", mCleanedWmkOfShortTx=" << mCleanedWmkOfShortTx
               << ", mLocalWmkOfAllTx=" << mLocalWmkOfAllTx
               << ", mLocalWmkOfShortTx=" << mLocalWmkOfShortTx;
  }
}

ConcurrencyControl::Visibility ConcurrencyControl::isVisibleForIt(
    WORKERID whomWorkerId, TXID commitTs) {
  return local_workers_start_ts[whomWorkerId] > commitTs
             ? Visibility::kVisible
             : Visibility::kVisibleNextRound;
}

// kUndetermined is not possible atm because we spin on startTs
ConcurrencyControl::Visibility ConcurrencyControl::isVisibleForIt(
    WORKERID whomWorkerId, WORKERID whatWorkerId, TXID txTs) {
  const bool isCommitTs = txTs & kMsb;
  const TXID commitTs =
      isCommitTs ? (txTs & kMsbMask) : getCommitTimestamp(whatWorkerId, txTs);
  return isVisibleForIt(whomWorkerId, commitTs);
}

TXID ConcurrencyControl::getCommitTimestamp(WORKERID workerId, TXID txTs) {
  if (txTs & kMsb) {
    return txTs & kMsbMask;
  }
  DCHECK((txTs & kMsb) || VisibleForMe(workerId, txTs));
  const TXID& startTs = txTs;
  TXID lcb = other(workerId).mCommitTree.Lcb(startTs);
  // TODO: align with GC
  TXID commitTs = lcb ? lcb : std::numeric_limits<TXID>::max();
  ENSURE(commitTs > startTs);
  return commitTs;
}

bool ConcurrencyControl::VisibleForMe(WORKERID workerId, u64 txId) {
  const bool isCommitTs = txId & kMsb;
  const TXID commitTs = isCommitTs ? (txId & kMsbMask) : 0;
  const TXID startTs = txId & kMsbMask;

  // visible if writtern by me
  if (Worker::my().mWorkerId == workerId) {
    return true;
  }

  switch (ActiveTx().mTxIsolationLevel) {
  case IsolationLevel::kSnapshotIsolation:
  case IsolationLevel::kSerializable: {
    if (isCommitTs) {
      return Worker::my().mActiveTx.mStartTs > commitTs;
    }

    if (startTs < mGlobalWmkOfAllTxSnapshot) {
      return true;
    }

    // Use the cache
    if (local_snapshot_cache_ts[workerId] == ActiveTx().mStartTs) {
      return mLocalSnapshotCache[workerId] >= startTs;
    }
    if (mLocalSnapshotCache[workerId] >= startTs) {
      return true;
    }
    utils::Timer timer(CRCounters::MyCounters().cc_ms_snapshotting);
    TXID largestVisibleTxId =
        other(workerId).mCommitTree.Lcb(Worker::my().mActiveTx.mStartTs);
    if (largestVisibleTxId) {
      mLocalSnapshotCache[workerId] = largestVisibleTxId;
      local_snapshot_cache_ts[workerId] = Worker::my().mActiveTx.mStartTs;
      return largestVisibleTxId >= startTs;
    }
    return false;
  }
  default: {
    UNREACHABLE();
  }
  }
}

bool ConcurrencyControl::VisibleForAll(TXID ts) {
  if (ts & kMsb) {
    // Commit Timestamp
    return (ts & kMsbMask) < Worker::sGlobalOldestTxId.load();
  }
  // Start Timestamp
  return ts < Worker::sGlobalWmkOfAllTx.load();
}

TXID ConcurrencyControl::CommitTree::AppendCommitLog(TXID startTs) {
  utils::Timer timer(CRCounters::MyCounters().cc_ms_committing);
  mutex.lock();
  DCHECK(cursor < capacity);
  const TXID commitTs = sTimeStampOracle.fetch_add(1);
  // Transactions are sequential in one worker, so the commitTs and startTs is
  // always increasing in the commit log of one worker
  mCommitLog[cursor++] = {commitTs, startTs};
  mutex.unlock();
  return commitTs;
}

// find the largest commitTs that is not smaller than startTs
std::optional<std::pair<TXID, TXID>> ConcurrencyControl::CommitTree::lcbNoLatch(
    TXID startTs) {
  auto comp = [&](const auto& pair, TXID startTs) {
    return startTs > pair.first;
  };
  auto* it = std::lower_bound(mCommitLog, mCommitLog + cursor, startTs, comp);
  if (it == mCommitLog) {
    return {};
  }
  it--;
  DCHECK(it->second < startTs);
  return *it;
}

TXID ConcurrencyControl::CommitTree::Lcb(TXID startTs) {
  mutex.lock_shared();
  SCOPED_DEFER(mutex.unlock_shared());

  if (auto result = lcbNoLatch(startTs); result) {
    return result->second;
  }
  return 0;
}

void ConcurrencyControl::CommitTree::CleanUpCommitLog() {
  if (cursor < capacity) {
    return;
  }

  utils::Timer timer(CRCounters::MyCounters().cc_ms_gc_cm);
  std::set<std::pair<TXID, TXID>> set; // TODO: unordered_set

  const WORKERID myWorkerId = Worker::my().mWorkerId;
  for (WORKERID i = 0; i < Worker::my().mNumAllWorkers; i++) {
    if (i == myWorkerId) {
      continue;
    }

    u64 latestStartTs = Worker::sLatestStartTs[i].load();
    latestStartTs &= Worker::kCleanBitsMask;

    set.insert(mCommitLog[cursor - 1]);
    if (latestStartTs == 0) {
      // to avoid race conditions when switching from RC to SI
      set.insert(mCommitLog[0]);
    } else if (auto result = lcbNoLatch(latestStartTs); result) {
      set.insert(*result);
    }
  }

  mutex.lock();
  cursor = 0;
  for (auto& p : set) {
    mCommitLog[cursor++] = p;
  }
  mutex.unlock();
}

} // namespace leanstore::cr
