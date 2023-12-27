#include "Worker.hpp"

#include "Config.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "storage/buffer-manager/TreeRegistry.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <mutex>
#include <sstream>

#include <stdio.h>

namespace leanstore {
namespace cr {

thread_local std::unique_ptr<Worker> Worker::sTlsWorker = nullptr;
std::shared_mutex Worker::sGlobalMutex;

// All transactions < are committed
std::unique_ptr<atomic<u64>[]> Worker::sWorkersCurrentSnapshot =
    std::make_unique<atomic<u64>[]>(FLAGS_worker_threads);
atomic<u64> Worker::sOldestAllStartTs = 0;
atomic<u64> Worker::sOldestOltpStartTx = 0;
atomic<u64> Worker::sAllLwm = 0;
atomic<u64> Worker::sOltpLwm = 0;
atomic<u64> Worker::sNewestOlapStartTx = 0;

Worker::Worker(u64 workerId, std::vector<Worker*>& allWorkers, u64 numWorkers,
               HistoryTreeInterface& historyTree)
    : cc(historyTree, numWorkers), mWorkerId(workerId), mAllWorkers(allWorkers),
      mNumAllWorkers(numWorkers) {
  CRCounters::myCounters().mWorkerId = workerId;

  // init wal buffer
  mLogging.mWalBuffer = (u8*)(std::aligned_alloc(512, FLAGS_wal_buffer_size));
  std::memset(mLogging.mWalBuffer, 0, FLAGS_wal_buffer_size);

  cc.mLocalSnapshotCache = make_unique<u64[]>(numWorkers);
  cc.local_snapshot_cache_ts = make_unique<u64[]>(numWorkers);
  cc.local_workers_start_ts = make_unique<u64[]>(numWorkers + 1);
  sWorkersCurrentSnapshot[mWorkerId] = 0;
}

Worker::~Worker() {
  delete[] cc.commit_tree.array;
  free(mLogging.mWalBuffer);
}

void Worker::startTX(TX_MODE mode, TX_ISOLATION_LEVEL level, bool isReadOnly) {
  utils::Timer timer(CRCounters::myCounters().cc_ms_start_tx);
  Transaction prevTx = mActiveTx;
  DCHECK(prevTx.state != TX_STATE::STARTED);
  mActiveTx.Start(mode, level, isReadOnly);

  if (!FLAGS_wal) {
    return;
  }

  // Init wal and group commit related transaction information
  mLogging.mTxWalBegin = mLogging.mWalBuffered;
  SCOPED_DEFER(if (!isReadOnly) {
    mLogging.ReserveWALEntrySimple(WALEntry::TYPE::TX_START);
    mLogging.SubmitWALEntrySimple();
  });

  // Advance local GSN on demand to maintain transaction dependency
  const LID maxFlushedGsn = Logging::sMaxFlushedGsn.load();
  if (maxFlushedGsn > mLogging.GetCurrentGsn()) {
    mLogging.SetCurrentGsn(maxFlushedGsn);
    mLogging.UpdateWalFlushReq();
  }

  if (FLAGS_wal_rfa) {
    mLogging.mMinFlushedGsn = Logging::sMinFlushedGsn.load();
    mLogging.mHasRemoteDependency = false;
  } else {
    mLogging.mHasRemoteDependency = true;
  }

  // Draw TXID from global counter and publish it with the TX type (i.e., OLAP
  // or OLTP) We have to acquire a transaction id and use it for locking in
  // ANY isolation level
  if (level >= TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION) {
    // implies multi-statement
    if (prevTx.isReadCommitted() || prevTx.isReadUncommitted()) {
      cc.switchToSnapshotIsolationMode();
    }
    {
      utils::Timer timer(CRCounters::myCounters().cc_ms_snapshotting);
      auto& curWorkerSnapshot = sWorkersCurrentSnapshot[mWorkerId];
      curWorkerSnapshot.store(mActiveTx.mStartTs | LATCH_BIT,
                              std::memory_order_release);

      mActiveTx.mStartTs = ConcurrencyControl::sGlobalClock.fetch_add(1);
      if (FLAGS_olap_mode) {
        curWorkerSnapshot.store(mActiveTx.mStartTs |
                                    ((mActiveTx.isOLAP()) ? OLAP_BIT : 0),
                                std::memory_order_release);
      } else {
        curWorkerSnapshot.store(mActiveTx.mStartTs, std::memory_order_release);
      }
    }
    cc.commit_tree.cleanIfNecessary();
    cc.local_global_all_lwm_cache = sAllLwm.load();
  } else {
    if (prevTx.atLeastSI()) {
      cc.switchToReadCommittedMode();
    }
    cc.commit_tree.cleanIfNecessary();
  }
}

void Worker::commitTX() {
  if (activeTX().isDurable()) {
    {
      utils::Timer timer(CRCounters::myCounters().cc_ms_commit_tx);
      mCommandId = 0; // Reset mCommandId only on commit and never on abort

      DCHECK(mActiveTx.state == TX_STATE::STARTED);
      if (FLAGS_wal_tuple_rfa) {
        for (auto& dependency : mLogging.mRfaChecksAtPreCommit) {
          auto& otherLogging = mLogging.other(std::get<0>(dependency));
          auto& otherWorkerTx = std::get<1>(dependency);
          if (otherLogging.TxUnCommitted(otherWorkerTx)) {
            mLogging.mHasRemoteDependency = true;
            break;
          }
        }
        mLogging.mRfaChecksAtPreCommit.clear();
      }

      if (activeTX().hasWrote()) {
        TXID commitTs = cc.commit_tree.commit(mActiveTx.startTS());
        cc.mLatestWriteTx.store(commitTs, std::memory_order_release);
        mActiveTx.mCommitTs = commitTs;
      }

      mActiveTx.mMaxObservedGSN = mLogging.GetCurrentGsn();
      mActiveTx.state = TX_STATE::READY_TO_COMMIT;

      // TODO: commitTs in log
      mLogging.ReserveWALEntrySimple(WALEntry::TYPE::TX_COMMIT);
      mLogging.SubmitWALEntrySimple();

      mLogging.ReserveWALEntrySimple(WALEntry::TYPE::TX_FINISH);
      mLogging.SubmitWALEntrySimple();

      if (FLAGS_wal_variant == 2) {
        mLogging.mWalFlushReq.mOptimisticLatch.notify_all();
      }

      mActiveTx.stats.precommit = std::chrono::high_resolution_clock::now();

      std::unique_lock<std::mutex> g(mLogging.mPreCommittedQueueMutex);
      if (mLogging.mHasRemoteDependency) {
        mLogging.mPreCommittedQueue.push_back(mActiveTx);
      } else { // RFA
        CRCounters::myCounters().rfa_committed_tx++;
        mLogging.mPreCommittedQueueRfa.push_back(mActiveTx);
      }
    }

    // Only committing snapshot/ changing between SI and lower modes
    if (activeTX().atLeastSI()) {
      cc.refreshGlobalState();
    }

    // All isolation level generate garbage
    cc.garbageCollection();

    // wait transaction to be committed
    while (mLogging.TxUnCommitted(mActiveTx.mCommitTs)) {
    }
    mActiveTx.state = TX_STATE::COMMITTED;
  }
}

void Worker::abortTX() {
  utils::Timer timer(CRCounters::myCounters().cc_ms_abort_tx);

  ENSURE(FLAGS_wal);
  ENSURE(!mActiveTx.mWalExceedBuffer);
  ENSURE(mActiveTx.state == TX_STATE::STARTED);

  const u64 txId = mActiveTx.startTS();
  std::vector<const WALEntry*> entries;
  mLogging.iterateOverCurrentTXEntries([&](const WALEntry& entry) {
    if (entry.type == WALEntry::TYPE::COMPLEX) {
      entries.push_back(&entry);
    }
  });
  std::for_each(entries.rbegin(), entries.rend(), [&](const WALEntry* entry) {
    const auto& complexEntry = *reinterpret_cast<const WALEntryComplex*>(entry);
    leanstore::storage::TreeRegistry::sInstance->undo(
        complexEntry.mTreeId, complexEntry.payload, txId);
  });

  cc.mHistoryTree.purgeVersions(
      mWorkerId, mActiveTx.startTS(), mActiveTx.startTS(),
      [&](const TXID, const TREEID, const u8*, u64, const bool) {});

  mLogging.ReserveWALEntrySimple(WALEntry::TYPE::TX_ABORT);
  mLogging.SubmitWALEntrySimple();

  // TODO(jian.z): add compensation WALEntry
  mLogging.ReserveWALEntrySimple(WALEntry::TYPE::TX_FINISH);
  mLogging.SubmitWALEntrySimple();

  mActiveTx.state = TX_STATE::ABORTED;
  jumpmu::jump();
}

void Worker::shutdown() {
  cc.garbageCollection();
  cc.switchToReadCommittedMode();
}

} // namespace cr
} // namespace leanstore
