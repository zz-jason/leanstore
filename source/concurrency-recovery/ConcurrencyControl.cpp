#include "Worker.hpp"
#include "storage/buffer-manager/TreeRegistry.hpp"

#include "utils/Misc.hpp"

#include <set>

namespace leanstore {
namespace cr {

atomic<u64> ConcurrencyControl::sGlobalClock = Worker::WORKERS_INCREMENT;

// Also for interval garbage collection
void ConcurrencyControl::refreshGlobalState() {
  if (!FLAGS_todo) {
    // Why bother
    return;
  }

  utils::Timer timer(CRCounters::myCounters().cc_ms_refresh_global_state);

  if (utils::RandomGenerator::getRandU64(0, Worker::my().mNumAllWorkers) == 0 &&
      Worker::sGlobalMutex.try_lock()) {
    TXID localNewestOlap = std::numeric_limits<TXID>::min();
    TXID localOldestOltp = std::numeric_limits<TXID>::max();
    TXID localOldestTx = std::numeric_limits<TXID>::max();
    for (WORKERID i = 0; i < Worker::my().mNumAllWorkers; i++) {
      atomic<u64>& workerSnapshot = Worker::sWorkersCurrentSnapshot[i];

      u64 workerInFlightTxId = workerSnapshot.load();
      while ((workerInFlightTxId & Worker::LATCH_BIT) &&
             ((workerInFlightTxId & Worker::CLEAN_BITS_MASK) <
              activeTX().startTS())) {
        workerInFlightTxId = workerSnapshot.load();
      }

      bool isRc = workerInFlightTxId & Worker::RC_BIT;
      bool isOlap = workerInFlightTxId & Worker::OLAP_BIT;
      workerInFlightTxId &= Worker::CLEAN_BITS_MASK;

      if (!isRc) {
        localOldestTx = std::min<TXID>(workerInFlightTxId, localOldestTx);
        if (isOlap) {
          localNewestOlap = std::max<TXID>(workerInFlightTxId, localNewestOlap);
        } else {
          localOldestOltp = std::min<TXID>(workerInFlightTxId, localOldestOltp);
        }
      }
    }

    Worker::sOldestAllStartTs.store(localOldestTx, std::memory_order_release);
    Worker::sOldestOltpStartTx.store(localOldestOltp,
                                     std::memory_order_release);
    Worker::sNewestOlapStartTx.store(localNewestOlap,
                                     std::memory_order_release);

    TXID global_all_lwm_buffer = std::numeric_limits<TXID>::max();
    TXID global_oltp_lwm_buffer = std::numeric_limits<TXID>::max();
    bool skipped_a_worker = false;
    for (WORKERID i = 0; i < Worker::my().mNumAllWorkers; i++) {
      ConcurrencyControl& workerState = other(i);
      if (workerState.mLatestLwm4Tx == workerState.mLatestWriteTx) {
        skipped_a_worker = true;
        continue;
      } else {
        workerState.mLatestLwm4Tx.store(workerState.mLatestWriteTx,
                                        std::memory_order_release);
      }

      TXID its_all_lwm_buffer =
          workerState.commit_tree.LCB(Worker::sOldestAllStartTs);
      TXID its_oltp_lwm_buffer =
          workerState.commit_tree.LCB(Worker::sOldestOltpStartTx);

      if (FLAGS_olap_mode &&
          Worker::sOldestAllStartTs != Worker::sOldestOltpStartTx) {
        global_oltp_lwm_buffer =
            std::min<TXID>(its_oltp_lwm_buffer, global_oltp_lwm_buffer);
      } else {
        its_oltp_lwm_buffer = its_all_lwm_buffer;
      }

      global_all_lwm_buffer =
          std::min<TXID>(its_all_lwm_buffer, global_all_lwm_buffer);

      workerState.local_lwm_latch.store(workerState.local_lwm_latch.load() + 1,
                                        std::memory_order_release); // Latch
      workerState.all_lwm_receiver.store(its_all_lwm_buffer,
                                         std::memory_order_release);
      workerState.oltp_lwm_receiver.store(its_oltp_lwm_buffer,
                                          std::memory_order_release);
      workerState.local_lwm_latch.store(workerState.local_lwm_latch.load() + 1,
                                        std::memory_order_release); // Release
    }
    if (!skipped_a_worker) {
      Worker::sAllLwm.store(global_all_lwm_buffer, std::memory_order_release);
      Worker::sOltpLwm.store(global_oltp_lwm_buffer, std::memory_order_release);
    }
    // -------------------------------------------------------------------------------------
    Worker::sGlobalMutex.unlock();
  }
}

void ConcurrencyControl::switchToSnapshotIsolationMode() {
  u64 workerId = Worker::my().mWorkerId;
  {
    std::unique_lock guard(Worker::sGlobalMutex);
    atomic<u64>& workerSnapshot = Worker::sWorkersCurrentSnapshot[workerId];
    workerSnapshot.store(sGlobalClock.load(), std::memory_order_release);
  }
  refreshGlobalState();
}
// -------------------------------------------------------------------------------------
void ConcurrencyControl::switchToReadCommittedMode() {
  u64 workerId = Worker::my().mWorkerId;
  {
    // Latch-free work only when all counters increase monotone, we can not
    // simply go back
    std::unique_lock guard(Worker::sGlobalMutex);
    atomic<u64>& workerSnapshot = Worker::sWorkersCurrentSnapshot[workerId];
    u64 newSnapshot = workerSnapshot.load() | Worker::RC_BIT;
    workerSnapshot.store(newSnapshot, std::memory_order_release);
  }
  refreshGlobalState();
}

void ConcurrencyControl::garbageCollection() {
  if (!FLAGS_todo) {
    return;
  }

  // TODO: smooth purge, we should not let the system hang on this, as a quick
  // fix, it should be enough if we purge in small batches
  utils::Timer timer(CRCounters::myCounters().cc_ms_gc);
synclwm : {
  u64 lwm_version = local_lwm_latch.load();
  while ((lwm_version = local_lwm_latch.load()) & 1) {
  };
  local_all_lwm = all_lwm_receiver.load();
  local_oltp_lwm = oltp_lwm_receiver.load();
  if (lwm_version != local_lwm_latch.load()) {
    goto synclwm;
  }
  ENSURE(!FLAGS_olap_mode || local_all_lwm <= local_oltp_lwm);
}

  // ATTENTION: atm, with out extra sync, the two lwm can not
  if (local_all_lwm > cleaned_untill_oltp_lwm) {
    utils::Timer timer(CRCounters::myCounters().cc_ms_gc_history_tree);
    // PURGE!
    mHistoryTree.purgeVersions(
        Worker::my().mWorkerId, 0, local_all_lwm - 1,
        [&](const TXID tx_id, const TREEID treeId, const u8* version_payload,
            [[maybe_unused]] u64 version_payload_length,
            const bool called_before) {
          leanstore::storage::TreeRegistry::sInstance->todo(
              treeId, version_payload, Worker::my().mWorkerId, tx_id,
              called_before);
          COUNTERS_BLOCK() {
            WorkerCounters::myCounters().cc_todo_olap_executed[treeId]++;
          }
        },
        0);
    cleaned_untill_oltp_lwm = std::max(local_all_lwm, cleaned_untill_oltp_lwm);
  }
  if (FLAGS_olap_mode && local_all_lwm != local_oltp_lwm) {
    if (FLAGS_graveyard && local_oltp_lwm > 0 &&
        local_oltp_lwm > cleaned_untill_oltp_lwm) {
      utils::Timer timer(CRCounters::myCounters().cc_ms_gc_graveyard);
      // MOVE deletes to the graveyard
      const u64 from_tx_id =
          cleaned_untill_oltp_lwm > 0 ? cleaned_untill_oltp_lwm : 0;
      mHistoryTree.visitRemoveVersions(
          Worker::my().mWorkerId, from_tx_id, local_oltp_lwm - 1,
          [&](const TXID tx_id, const TREEID treeId, const u8* version_payload,
              [[maybe_unused]] u64 version_payload_length,
              const bool called_before) {
            cleaned_untill_oltp_lwm =
                std::max(cleaned_untill_oltp_lwm, tx_id + 1);
            leanstore::storage::TreeRegistry::sInstance->todo(
                treeId, version_payload, Worker::my().mWorkerId, tx_id,
                called_before);
            COUNTERS_BLOCK() {
              WorkerCounters::myCounters().cc_todo_oltp_executed[treeId]++;
            }
          });
    }
  }
}

ConcurrencyControl::VISIBILITY ConcurrencyControl::isVisibleForIt(
    WORKERID whom_worker_id, TXID commitTs) {
  return local_workers_start_ts[whom_worker_id] > commitTs
             ? VISIBILITY::VISIBLE_ALREADY
             : VISIBILITY::VISIBLE_NEXT_ROUND;
}
// -------------------------------------------------------------------------------------
// UNDETERMINED is not possible atm because we spin on startTs
ConcurrencyControl::VISIBILITY ConcurrencyControl::isVisibleForIt(
    WORKERID whom_worker_id, WORKERID what_worker_id, TXID tx_ts) {
  const bool is_commit_ts = tx_ts & MSB;
  const TXID commitTs = is_commit_ts
                            ? (tx_ts & MSB_MASK)
                            : getCommitTimestamp(what_worker_id, tx_ts);
  return isVisibleForIt(whom_worker_id, commitTs);
}

TXID ConcurrencyControl::getCommitTimestamp(WORKERID workerId, TXID tx_ts) {
  if (tx_ts & MSB) {
    return tx_ts & MSB_MASK;
  }
  assert((tx_ts & MSB) || isVisibleForMe(workerId, tx_ts));
  // -------------------------------------------------------------------------------------
  const TXID& startTs = tx_ts;
  TXID lcb = other(workerId).commit_tree.LCB(startTs);
  TXID commitTs =
      lcb ? lcb : std::numeric_limits<TXID>::max(); // TODO: align with GC
  ENSURE(commitTs > startTs);
  return commitTs;
}

// It is also used to check whether the tuple is write-locked, hence we need the
// toWrite intention flag
bool ConcurrencyControl::isVisibleForMe(WORKERID workerId, u64 txId,
                                        bool toWrite) {
  const bool isCommitTs = txId & MSB;
  const TXID commitTs = isCommitTs ? (txId & MSB_MASK) : 0;
  const TXID startTs = txId & MSB_MASK;

  // visible for all the READ-UNCOMMITTED transactions
  if (!toWrite && activeTX().isReadUncommitted()) {
    return true;
  }

  // visible if writtern by me
  if (Worker::my().mWorkerId == workerId) {
    return true;
  }

  switch (activeTX().mTxIsolationLevel) {
  case TX_ISOLATION_LEVEL::READ_UNCOMMITTED:
  case TX_ISOLATION_LEVEL::READ_COMMITTED: {
    if (isCommitTs) {
      return true;
    }

    // use cache
    if (mLocalSnapshotCache[workerId] >= startTs) {
      return true;
    }

    utils::Timer timer(CRCounters::myCounters().cc_ms_snapshotting);

    TXID curTs = cr::Worker::Worker::my().cc.sGlobalClock.load() + 1;
    TXID lastCommitTs = other(workerId).commit_tree.LCB(curTs);
    mLocalSnapshotCache[workerId] = lastCommitTs;
    local_snapshot_cache_ts[workerId] = curTs;

    bool isVisible = lastCommitTs >= startTs;

    // If the worker starts a transaction after the last commit, the data might
    // be invible, i.e. startTs > lastCommitTs.
    RAISE_WHEN(!isVisible);

    return isVisible;
  }
  case TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION:
  case TX_ISOLATION_LEVEL::SERIALIZABLE: {
    if (isCommitTs) {
      return Worker::my().mActiveTx.startTS() > commitTs;
    }

    if (startTs < local_global_all_lwm_cache) {
      return true;
    }

    // Use the cache
    if (local_snapshot_cache_ts[workerId] == activeTX().startTS()) {
      return mLocalSnapshotCache[workerId] >= startTs;
    } else if (mLocalSnapshotCache[workerId] >= startTs) {
      return true;
    }
    utils::Timer timer(CRCounters::myCounters().cc_ms_snapshotting);
    TXID largest_visible_tx_id =
        other(workerId).commit_tree.LCB(Worker::my().mActiveTx.startTS());
    if (largest_visible_tx_id) {
      mLocalSnapshotCache[workerId] = largest_visible_tx_id;
      local_snapshot_cache_ts[workerId] = Worker::my().mActiveTx.startTS();
      return largest_visible_tx_id >= startTs;
    }
    return false;
  }
  default: {
    UNREACHABLE();
  }
  }
}

bool ConcurrencyControl::isVisibleForAll(WORKERID, TXID ts) {
  if (ts & MSB) {
    // Commit Timestamp
    return (ts & MSB_MASK) < Worker::sOldestAllStartTs.load();
  } else {
    // Start Timestamp
    return ts < Worker::sAllLwm.load();
  }
}

TXID ConcurrencyControl::CommitTree::commit(TXID startTs) {
  utils::Timer timer(CRCounters::myCounters().cc_ms_committing);
  mutex.lock();
  assert(cursor < capacity);
  const TXID commitTs = sGlobalClock.fetch_add(1);
  array[cursor++] = {commitTs, startTs};
  mutex.unlock();
  return commitTs;
}

std::optional<std::pair<TXID, TXID>> ConcurrencyControl::CommitTree::LCBUnsafe(
    TXID startTs) {
  const auto begin = array;
  const auto end = array + cursor;
  auto it =
      std::lower_bound(begin, end, startTs, [&](const auto& pair, TXID ts) {
        return pair.first < ts;
      });
  if (it == begin) {
    // raise(SIGTRAP);
    return {};
  } else {
    it--;
    assert(it->second < startTs);
    return *it;
  }
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

  utils::Timer timer(CRCounters::myCounters().cc_ms_gc_cm);
  std::set<std::pair<TXID, TXID>> set; // TODO: unordered_set

  const WORKERID my_worker_id = cr::Worker::Worker::my().mWorkerId;
  for (WORKERID i = 0; i < cr::Worker::Worker::my().mNumAllWorkers; i++) {
    if (i == my_worker_id) {
      continue;
    }
    u64 its_start_ts = Worker::sWorkersCurrentSnapshot[i].load();
    while (its_start_ts & Worker::LATCH_BIT) {
      its_start_ts = Worker::sWorkersCurrentSnapshot[i].load();
    }
    its_start_ts &= Worker::CLEAN_BITS_MASK;
    set.insert(array[cursor - 1]); // for  the new TX
    if (its_start_ts == 0) {
      // to avoid race conditions when switching from RC to SI
      set.insert(array[0]);
    } else {
      auto v = LCBUnsafe(its_start_ts);
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

} // namespace cr
} // namespace leanstore
