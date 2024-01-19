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

std::atomic<u64> ConcurrencyControl::sGlobalClock = Worker::kWorkersIncrement;

// Also for interval garbage collection
void ConcurrencyControl::UpdateGlobalTxWatermarks() {
  if (!FLAGS_enable_garbage_collection) {
    return;
  }

  utils::Timer timer(CRCounters::MyCounters().cc_ms_refresh_global_state);

  auto performRefresh =
      utils::RandomGenerator::getRandU64(0, Worker::my().mNumAllWorkers) == 0 &&
      Worker::sGlobalMutex.try_lock();
  if (!performRefresh) {
    return;
  }
  SCOPED_DEFER(Worker::sGlobalMutex.unlock()); // release the lock on exit

  TXID localOldestTxId = std::numeric_limits<TXID>::max();
  TXID localNewestLongTxId = std::numeric_limits<TXID>::min();
  TXID localOldestShortTxId = std::numeric_limits<TXID>::max();
  for (WORKERID i = 0; i < Worker::my().mNumAllWorkers; i++) {
    std::atomic<u64>& latestStartTs = Worker::sLatestStartTs[i];

    u64 runningTxId = latestStartTs.load();
    while ((runningTxId & Worker::kLatchBit) &&
           ((runningTxId & Worker::kCleanBitsMask) < ActiveTx().mStartTs)) {
      // can only happen when the worker just finished a transaction whose
      // startTs is smaller than the current transaction's startTs and the
      // worker is about to start a new transaction.
      runningTxId = latestStartTs.load();
    }

    // Skip transactions running in read-committed mode.
    if (runningTxId & Worker::kRcBit) {
      continue;
    }

    bool isLongRunningTx = runningTxId & Worker::kLongRunningBit;
    runningTxId &= Worker::kCleanBitsMask;
    localOldestTxId = std::min<TXID>(runningTxId, localOldestTxId);
    if (isLongRunningTx) {
      localNewestLongTxId = std::max<TXID>(runningTxId, localNewestLongTxId);
    } else {
      localOldestShortTxId = std::min<TXID>(runningTxId, localOldestShortTxId);
    }
  }

  // Update the three transaction ids
  Worker::sGlobalOldestTxId.store(localOldestTxId, std::memory_order_release);
  Worker::sGlobalNewestLongTxId.store(localNewestLongTxId,
                                      std::memory_order_release);
  Worker::sGlobalOldestShortTxId.store(localOldestShortTxId,
                                       std::memory_order_release);

  // Update global lower watermarks based on the three transaction ids
  TXID globalWmkOfAllTx = std::numeric_limits<TXID>::max();
  TXID globalWmkOfShortTx = std::numeric_limits<TXID>::max();
  bool skippedAWorker = false;
  for (WORKERID i = 0; i < Worker::my().mNumAllWorkers; i++) {
    ConcurrencyControl& cc = other(i);
    if (cc.mLatestLwm4Tx == cc.mLatestCommitTs) {
      skippedAWorker = true;
      continue;
    }
    cc.mLatestLwm4Tx.store(cc.mLatestCommitTs, std::memory_order_release);
    TXID wmkOfAllTx = cc.mCommitTree.LCB(Worker::sGlobalOldestTxId);
    TXID wmkOfShortTx = cc.mCommitTree.LCB(Worker::sGlobalOldestShortTxId);

    if (FLAGS_enable_long_running_transaction &&
        Worker::sGlobalOldestTxId != Worker::sGlobalOldestShortTxId) {
      globalWmkOfShortTx = std::min<TXID>(wmkOfShortTx, globalWmkOfShortTx);
    } else {
      wmkOfShortTx = wmkOfAllTx;
    }

    globalWmkOfAllTx = std::min<TXID>(wmkOfAllTx, globalWmkOfAllTx);

    cc.mWmkVersion.store(cc.mWmkVersion.load() + 1, std::memory_order_release);
    cc.mWmkOfAllTx.store(wmkOfAllTx, std::memory_order_release);
    cc.mWmkOfShortTx.store(wmkOfShortTx, std::memory_order_release);
    cc.mWmkVersion.store(cc.mWmkVersion.load() + 1, std::memory_order_release);
  }

  if (!skippedAWorker) {
    Worker::sGlobalWmkOfAllTx.store(globalWmkOfAllTx,
                                    std::memory_order_release);
    Worker::sGlobalWmkOfShortTx.store(globalWmkOfShortTx,
                                      std::memory_order_release);
  }
}

void ConcurrencyControl::switchToSnapshotIsolationMode() {
  u64 workerId = Worker::my().mWorkerId;
  {
    std::unique_lock guard(Worker::sGlobalMutex);
    std::atomic<u64>& workerSnapshot = Worker::sLatestStartTs[workerId];
    workerSnapshot.store(sGlobalClock.load(), std::memory_order_release);
  }
  UpdateGlobalTxWatermarks();
}

void ConcurrencyControl::switchToReadCommittedMode() {
  u64 workerId = Worker::my().mWorkerId;
  {
    // Latch-free work only when all counters increase monotone, we can not
    // simply go back
    std::unique_lock guard(Worker::sGlobalMutex);
    std::atomic<u64>& workerSnapshot = Worker::sLatestStartTs[workerId];
    u64 newSnapshot = workerSnapshot.load() | Worker::kRcBit;
    workerSnapshot.store(newSnapshot, std::memory_order_release);
  }
  UpdateGlobalTxWatermarks();
}

void ConcurrencyControl::updateLocalWatermarks() {
  while (true) {
    u64 version = mWmkVersion.load();

    // spin until the latch is free
    while ((version = mWmkVersion.load()) & 1) {
    };

    // update the two local watermarks
    mLocalWmk4AllTx = mWmkOfAllTx.load();
    mLocalWmk4ShortTx = mWmkOfShortTx.load();

    // restart if the latch was taken
    if (version == mWmkVersion.load()) {
      return;
    }
  }

  DCHECK(!FLAGS_enable_long_running_transaction ||
         mLocalWmk4AllTx <= mLocalWmk4ShortTx)
      << "Lower watermark of all transactions should be no higher than the "
         "lower watermark of short-running transactions"
      << ", workerId=" << Worker::my().mWorkerId
      << ", mLocalWmk4AllTx=" << mLocalWmk4AllTx
      << ", mLocalWmk4ShortTx=" << mLocalWmk4ShortTx;
}

// TODO: smooth purge, we should not let the system hang on this, as a quick
// fix, it should be enough if we purge in small batches
void ConcurrencyControl::GarbageCollection() {
  if (!FLAGS_enable_garbage_collection) {
    return;
  }

  utils::Timer timer(CRCounters::MyCounters().cc_ms_gc);
  updateLocalWatermarks();

  // remove versions that are nolonger needed by any long-running transaction
  if (cleaned_untill_oltp_lwm < mLocalWmk4AllTx) {
    utils::Timer timer(CRCounters::MyCounters().cc_ms_gc_history_tree);
    CRManager::sInstance->mHistoryTreePtr->PurgeVersions(
        Worker::my().mWorkerId, 0, mLocalWmk4AllTx - 1,
        [&](const TXID versionTxId, const TREEID treeId, const u8* versionData,
            u64, const bool calledBefore) {
          leanstore::storage::TreeRegistry::sInstance->GarbageCollect(
              treeId, versionData, Worker::my().mWorkerId, versionTxId,
              calledBefore);
          COUNTERS_BLOCK() {
            WorkerCounters::MyCounters().cc_todo_olap_executed[treeId]++;
          }
        },
        0);

    // advance the cleaned_untill_oltp_lwm
    cleaned_untill_oltp_lwm = mLocalWmk4AllTx;
  }

  // MOVE deletes to the graveyard
  if (FLAGS_enable_long_running_transaction &&
      mLocalWmk4AllTx < mLocalWmk4ShortTx &&
      cleaned_untill_oltp_lwm < mLocalWmk4ShortTx) {
    utils::Timer timer(CRCounters::MyCounters().cc_ms_gc_graveyard);
    CRManager::sInstance->mHistoryTreePtr->VisitRemovedVersions(
        Worker::my().mWorkerId, cleaned_untill_oltp_lwm, mLocalWmk4ShortTx - 1,
        [&](const TXID txId, const TREEID treeId, const u8* versionData, u64,
            const bool calledBefore) {
          cleaned_untill_oltp_lwm = std::max(cleaned_untill_oltp_lwm, txId + 1);
          leanstore::storage::TreeRegistry::sInstance->GarbageCollect(
              treeId, versionData, Worker::my().mWorkerId, txId, calledBefore);
          COUNTERS_BLOCK() {
            WorkerCounters::MyCounters().cc_todo_oltp_executed[treeId]++;
          }
        });
  }
}

ConcurrencyControl::VISIBILITY ConcurrencyControl::isVisibleForIt(
    WORKERID whomWorkerId, TXID commitTs) {
  return local_workers_start_ts[whomWorkerId] > commitTs
             ? VISIBILITY::VISIBLE_ALREADY
             : VISIBILITY::VISIBLE_NEXT_ROUND;
}

// UNDETERMINED is not possible atm because we spin on startTs
ConcurrencyControl::VISIBILITY ConcurrencyControl::isVisibleForIt(
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
  TXID lcb = other(workerId).mCommitTree.LCB(startTs);
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

    if (startTs < local_global_all_lwm_cache) {
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
        other(workerId).mCommitTree.LCB(Worker::my().mActiveTx.mStartTs);
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

TXID ConcurrencyControl::CommitTree::commit(TXID startTs) {
  utils::Timer timer(CRCounters::MyCounters().cc_ms_committing);
  mutex.lock();
  assert(cursor < capacity);
  const TXID commitTs = sGlobalClock.fetch_add(1);
  array[cursor++] = {commitTs, startTs};
  mutex.unlock();
  return commitTs;
}

std::optional<std::pair<TXID, TXID>> ConcurrencyControl::CommitTree::LCBUnsafe(
    TXID startTs) {
  auto* const begin = array;
  auto* const end = array + cursor;
  auto* it =
      std::lower_bound(begin, end, startTs, [&](const auto& pair, TXID ts) {
        return pair.first < ts;
      });

  if (it == begin) {
    return {};
  }

  it--;
  DCHECK(it->second < startTs);
  return *it;
}

TXID ConcurrencyControl::CommitTree::LCB(TXID startTs) {
  TXID lcb = 0;
  mutex.lock_shared();
  auto v = LCBUnsafe(startTs);
  if (v) {
    lcb = v->second;
  }
  mutex.unlock_shared();
  return lcb;
}

void ConcurrencyControl::CommitTree::cleanIfNecessary() {
  if (cursor < capacity) {
    return;
  }

  utils::Timer timer(CRCounters::MyCounters().cc_ms_gc_cm);
  std::set<std::pair<TXID, TXID>> set; // TODO: unordered_set

  const WORKERID myWorkerId = cr::Worker::Worker::my().mWorkerId;
  for (WORKERID i = 0; i < cr::Worker::Worker::my().mNumAllWorkers; i++) {
    if (i == myWorkerId) {
      continue;
    }
    u64 itsStartTs = Worker::sLatestStartTs[i].load();
    while (itsStartTs & Worker::kLatchBit) {
      itsStartTs = Worker::sLatestStartTs[i].load();
    }
    itsStartTs &= Worker::kCleanBitsMask;
    set.insert(array[cursor - 1]); // for  the new TX
    if (itsStartTs == 0) {
      // to avoid race conditions when switching from RC to SI
      set.insert(array[0]);
    } else {
      auto v = LCBUnsafe(itsStartTs);
      if (v) {
        set.insert(*v);
      }
    }
  }

  mutex.lock();
  cursor = 0;
  for (auto& p : set) {
    array[cursor++] = p;
  }
  mutex.unlock();
}

} // namespace leanstore::cr
