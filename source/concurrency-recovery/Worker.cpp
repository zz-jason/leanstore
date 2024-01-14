#include "Worker.hpp"

#include "Config.hpp"
#include "concurrency-recovery/Transaction.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "shared-headers/Exceptions.hpp"
#include "storage/buffer-manager/TreeRegistry.hpp"
#include "utils/Defer.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <mutex>

namespace leanstore {
namespace cr {

thread_local std::unique_ptr<Worker> Worker::sTlsWorker = nullptr;
std::shared_mutex Worker::sGlobalMutex;

// All transactions < are committed
std::unique_ptr<std::atomic<u64>[]> Worker::sWorkersCurrentSnapshot =
    std::make_unique<std::atomic<u64>[]>(FLAGS_worker_threads);
std::atomic<u64> Worker::sOldestAllStartTs = 0;
std::atomic<u64> Worker::sOldestOltpStartTx = 0;
std::atomic<u64> Worker::sAllLwm = 0;
std::atomic<u64> Worker::sOltpLwm = 0;
std::atomic<u64> Worker::sNewestOlapStartTx = 0;

Worker::Worker(u64 workerId, std::vector<Worker*>& allWorkers, u64 numWorkers)
    : cc(numWorkers),
      mWorkerId(workerId),
      mAllWorkers(allWorkers),
      mNumAllWorkers(numWorkers) {
  CRCounters::MyCounters().mWorkerId = workerId;

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
  cc.commit_tree.array = nullptr;

  free(mLogging.mWalBuffer);
  mLogging.mWalBuffer = nullptr;
}

void Worker::StartTx(TxMode mode, IsolationLevel level, bool isReadOnly) {
  utils::Timer timer(CRCounters::MyCounters().cc_ms_start_tx);
  Transaction prevTx = mActiveTx;
  DCHECK(prevTx.mState != TxState::kStarted)
      << "Previous transaction not ended"
      << ", workerId=" << mWorkerId << ", startTs=" << prevTx.mStartTs
      << ", txState=" << TxStatUtil::ToString(prevTx.mState);
  SCOPED_DEFER({
    DLOG(INFO) << "Start transaction"
               << ", workerId=" << mWorkerId
               << ", startTs=" << mActiveTx.mStartTs
               << ", txReadSnapshot(GSN)=" << mLogging.mTxReadSnapshot
               << ", workerGSN=" << mLogging.GetCurrentGsn()
               << ", globalMinFlushedGSN=" << Logging::sGlobalMinFlushedGSN
               << ", globalMaxFlushedGSN=" << Logging::sGlobalMaxFlushedGSN;
    if (!isReadOnly && FLAGS_wal) {
      mLogging.ReserveWALEntrySimple(WALEntry::TYPE::TX_START);
      mLogging.SubmitWALEntrySimple();
    }
  });

  mActiveTx.Start(mode, level, isReadOnly);

  if (!FLAGS_wal) {
    return;
  }

  // Sync GSN clock with the global max flushed (observed) GSN, so that the
  // global min flushed GSN can be advanced, transactions with remote dependency
  // can be committed in time.
  const auto maxFlushedGsn = Logging::sGlobalMaxFlushedGSN.load();
  if (maxFlushedGsn > mLogging.GetCurrentGsn()) {
    mLogging.SetCurrentGsn(maxFlushedGsn);
  }

  // Init wal and group commit related transaction information
  mLogging.mTxWalBegin = mLogging.mWalBuffered;

  // For remote dependency validation
  mLogging.mTxReadSnapshot = Logging::sGlobalMinFlushedGSN.load();
  mLogging.mHasRemoteDependency = false;

  // Draw TXID from global counter and publish it with the TX type (i.e., OLAP
  // or OLTP) We have to acquire a transaction id and use it for locking in ANY
  // isolation level
  if (level >= IsolationLevel::kSnapshotIsolation) {
    {
      utils::Timer timer(CRCounters::MyCounters().cc_ms_snapshotting);
      auto& curWorkerSnapshot = sWorkersCurrentSnapshot[mWorkerId];
      curWorkerSnapshot.store(mActiveTx.mStartTs | kLatchBit,
                              std::memory_order_release);

      mActiveTx.mStartTs = ConcurrencyControl::sGlobalClock.fetch_add(1);
      if (FLAGS_enable_olap_mode) {
        curWorkerSnapshot.store(mActiveTx.mStartTs |
                                    ((mActiveTx.IsOLAP()) ? kOlapBit : 0),
                                std::memory_order_release);
      } else {
        curWorkerSnapshot.store(mActiveTx.mStartTs, std::memory_order_release);
      }
    }
    cc.commit_tree.cleanIfNecessary();
    cc.local_global_all_lwm_cache = sAllLwm.load();
  } else {
    if (prevTx.AtLeastSI()) {
      cc.switchToReadCommittedMode();
    }
    cc.commit_tree.cleanIfNecessary();
  }
}

void Worker::CommitTx() {
  SCOPED_DEFER(COUNTERS_BLOCK() {
    mActiveTx.mState = TxState::kCommitted;
    DLOG(INFO) << "Transaction committed"
               << ", workerId=" << mWorkerId
               << ", startTs=" << mActiveTx.mStartTs
               << ", commitTs=" << mActiveTx.mCommitTs
               << ", maxObservedGSN=" << mActiveTx.mMaxObservedGSN;
  });

  utils::Timer timer(CRCounters::MyCounters().cc_ms_commit_tx);
  if (!mActiveTx.mIsDurable) {
    return;
  }

  mCommandId = 0; // Reset mCommandId only on commit and never on abort
  if (mActiveTx.mHasWrote) {
    auto commitTs = cc.commit_tree.commit(mActiveTx.mStartTs);
    cc.mLatestWriteTx.store(commitTs, std::memory_order_release);
    mActiveTx.mCommitTs = commitTs;
    DCHECK(mActiveTx.mStartTs < mActiveTx.mCommitTs)
        << "startTs should be smaller than commitTs"
        << ", workerId=" << my().mWorkerId
        << ", actual startTs=" << mActiveTx.mStartTs
        << ", actual commitTs=" << mActiveTx.mCommitTs;
  } else {
    DLOG(INFO) << "Transaction has no writes, skip assigning commitTs"
               << ", workerId=" << my().mWorkerId
               << ", actual startTs=" << mActiveTx.mStartTs;
  }

  mActiveTx.mMaxObservedGSN = mLogging.GetCurrentGsn();

  mLogging.ReserveWALEntrySimple(WALEntry::TYPE::TX_COMMIT);
  mLogging.SubmitWALEntrySimple();
  mLogging.ReserveWALEntrySimple(WALEntry::TYPE::TX_FINISH);
  mLogging.SubmitWALEntrySimple();

  if (mLogging.mHasRemoteDependency) {
    std::unique_lock<std::mutex> g(mLogging.mTxToCommitMutex);
    mLogging.mTxToCommit.push_back(mActiveTx);
    DLOG(INFO) << "Puting transaction with remote dependency to mTxToCommit"
               << ", workerId=" << mWorkerId
               << ", startTs=" << mActiveTx.mStartTs
               << ", commitTs=" << mActiveTx.mCommitTs
               << ", maxObservedGSN=" << mActiveTx.mMaxObservedGSN;
  } else {
    std::unique_lock<std::mutex> g(mLogging.mRfaTxToCommitMutex);
    CRCounters::MyCounters().rfa_committed_tx++;
    mLogging.mRfaTxToCommit.push_back(mActiveTx);
    DLOG(INFO) << "Puting transaction (RFA) to mRfaTxToCommit"
               << ", workerId=" << mWorkerId
               << ", startTs=" << mActiveTx.mStartTs
               << ", commitTs=" << mActiveTx.mCommitTs
               << ", maxObservedGSN=" << mActiveTx.mMaxObservedGSN;
  }

  // Only committing snapshot/ changing between SI and lower modes
  if (mActiveTx.AtLeastSI()) {
    cc.refreshGlobalState();
  }

  // All isolation level generate garbage
  cc.GarbageCollection();

  // wait transaction to be committed
  while (mLogging.TxUnCommitted(mActiveTx.mCommitTs)) {
  }
}

/// TODO(jian.z): revert changes made in-place on the btree
/// process of a transaction abort:
///
/// 1. Read previous wal entries
///
/// 2. Undo the changes via btree operations
///
/// 3. Write compensation wal entries during the undo process
///
/// 4. Purge versions in history tree, clean garbages made by the aborted
///    transaction
///
/// It may share the same code with the recovery process?
void Worker::AbortTx() {
  SCOPED_DEFER({
    mActiveTx.mState = TxState::kAborted;
    LOG(INFO) << "Transaction aborted"
              << ", workerId=" << mWorkerId
              << ", startTs=" << mActiveTx.mStartTs
              << ", commitTs=" << mActiveTx.mCommitTs
              << ", maxObservedGSN=" << mActiveTx.mMaxObservedGSN;
  });

  utils::Timer timer(CRCounters::MyCounters().cc_ms_abort_tx);
  if (!(mActiveTx.mState == TxState::kStarted && mActiveTx.mIsDurable)) {
    return;
  }

  // TODO(jian.z): support reading from WAL file once
  DCHECK(!mActiveTx.mWalExceedBuffer)
      << "Aborting from WAL file is not supported yet";

  std::vector<const WALEntry*> entries;
  mLogging.IterateCurrentTxWALs([&](const WALEntry& entry) {
    if (entry.type == WALEntry::TYPE::COMPLEX) {
      entries.push_back(&entry);
    }
  });

  const u64 txId = mActiveTx.mStartTs;
  std::for_each(entries.rbegin(), entries.rend(), [&](const WALEntry* entry) {
    const auto& complexEntry = *reinterpret_cast<const WALEntryComplex*>(entry);
    leanstore::storage::TreeRegistry::sInstance->undo(
        complexEntry.mTreeId, complexEntry.payload, txId);
  });

  cc.mHistoryTree->purgeVersions(
      mWorkerId, mActiveTx.mStartTs, mActiveTx.mStartTs,
      [&](const TXID, const TREEID, const u8*, u64, const bool) {});

  mLogging.ReserveWALEntrySimple(WALEntry::TYPE::TX_ABORT);
  mLogging.SubmitWALEntrySimple();
  mLogging.ReserveWALEntrySimple(WALEntry::TYPE::TX_FINISH);
  mLogging.SubmitWALEntrySimple();
}

void Worker::shutdown() {
  cc.GarbageCollection();
  cc.switchToReadCommittedMode();
}

} // namespace cr
} // namespace leanstore
