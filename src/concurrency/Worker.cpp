#include "concurrency/Worker.hpp"

#include "buffer-manager/TreeRegistry.hpp"
#include "concurrency/CRManager.hpp"
#include "concurrency/GroupCommitter.hpp"
#include "concurrency/Logging.hpp"
#include "concurrency/Transaction.hpp"
#include "concurrency/WalEntry.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "telemetry/MetricOnlyTimer.hpp"
#include "telemetry/MetricsManager.hpp"
#include "utils/Defer.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <mutex>

namespace leanstore::cr {

thread_local std::unique_ptr<Worker> Worker::sTlsWorker = nullptr;
thread_local Worker* Worker::sTlsWorkerRaw = nullptr;

Worker::Worker(uint64_t workerId, std::vector<Worker*>& allWorkers,
               leanstore::LeanStore* store)
    : mStore(store),
      mCc(store, allWorkers.size()),
      mActiveTxId(0),
      mWorkerId(workerId),
      mAllWorkers(allWorkers) {
  CRCounters::MyCounters().mWorkerId = workerId;

  // init wal buffer
  mLogging.mWalBufferSize = mStore->mStoreOption.mWalRingBufferSize;
  mLogging.mWalBuffer =
      (uint8_t*)(std::aligned_alloc(512, mLogging.mWalBufferSize));
  std::memset(mLogging.mWalBuffer, 0, mLogging.mWalBufferSize);

  mCc.mLcbCacheVal = std::make_unique<uint64_t[]>(mAllWorkers.size());
  mCc.mLcbCacheKey = std::make_unique<uint64_t[]>(mAllWorkers.size());
}

Worker::~Worker() {
  free(mLogging.mWalBuffer);
  mLogging.mWalBuffer = nullptr;
}

void Worker::StartTx(TxMode mode, IsolationLevel level, bool isReadOnly) {
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
               << ", globalMinFlushedGSN="
               << mStore->mCRManager->mGroupCommitter->mGlobalMinFlushedGSN
               << ", globalMaxFlushedGSN="
               << mStore->mCRManager->mGroupCommitter->mGlobalMaxFlushedGSN;
  });

  mActiveTx.Start(mode, level);

  if (!mActiveTx.mIsDurable) {
    return;
  }

  // Sync GSN clock with the global max flushed (observed) GSN, so that the
  // global min flushed GSN can be advanced, transactions with remote dependency
  // can be committed in time.
  const auto maxFlushedGsn =
      mStore->mCRManager->mGroupCommitter->mGlobalMaxFlushedGSN.load();
  if (maxFlushedGsn > mLogging.GetCurrentGsn()) {
    mLogging.SetCurrentGsn(maxFlushedGsn);
  }

  // Init wal and group commit related transaction information
  mLogging.mTxWalBegin = mLogging.mWalBuffered;

  // For remote dependency validation
  mLogging.mTxReadSnapshot =
      mStore->mCRManager->mGroupCommitter->mGlobalMinFlushedGSN.load();
  mLogging.mHasRemoteDependency = false;

  // For now, we only support SI and SSI
  if (level < IsolationLevel::kSnapshotIsolation) {
    LOG(FATAL) << "Unsupported isolation level: "
               << static_cast<uint64_t>(level);
  }

  // Draw TXID from global counter and publish it with the TX type (i.e.
  // long-running or short-running) We have to acquire a transaction id and use
  // it for locking in ANY isolation level
  //
  // TODO(jian.z): Allocating transaction start ts globally heavily hurts the
  // scalability, especially for read-only transactions
  if (isReadOnly) {
    mActiveTx.mStartTs = mStore->GetTs();
  } else {
    mActiveTx.mStartTs = mStore->AllocTs();
  }
  auto curTxId = mActiveTx.mStartTs;
  if (FLAGS_enable_long_running_transaction && mActiveTx.IsLongRunning()) {
    // Mark as long-running transaction
    curTxId |= kLongRunningBit;
  }

  // Publish the transaction id
  mActiveTxId.store(curTxId, std::memory_order_release);
  mCc.mGlobalWmkOfAllTx = mStore->mCRManager->mGlobalWmkInfo.mWmkOfAllTx.load();

  // Cleanup commit log if necessary
  mCc.mCommitTree.CompactCommitLog();
}

void Worker::CommitTx() {
  SCOPED_DEFER(mActiveTx.mState = TxState::kCommitted);

  if (!mActiveTx.mIsDurable) {
    return;
  }

  // Reset mCommandId on commit
  mCommandId = 0;
  if (mActiveTx.mHasWrote) {
    mActiveTx.mCommitTs = mStore->AllocTs();
    mCc.mCommitTree.AppendCommitLog(mActiveTx.mStartTs, mActiveTx.mCommitTs);
    mCc.mLatestCommitTs.store(mActiveTx.mCommitTs, std::memory_order_release);
  } else {
    DLOG(INFO) << "Transaction has no writes, skip assigning commitTs, append "
                  "log to commit tree, and group commit"
               << ", workerId=" << My().mWorkerId
               << ", actual startTs=" << mActiveTx.mStartTs;
  }

  // Reset startTs so that other transactions can safely update the global
  // transaction watermarks and garbage collect the unused versions.
  mActiveTxId.store(0, std::memory_order_release);

  if (!mActiveTx.mHasWrote) {
    return;
  }

  if (mActiveTx.mIsDurable) {
    mLogging.WriteWalTxFinish();
  }

  // update max observed GSN
  mActiveTx.mMaxObservedGSN = mLogging.GetCurrentGsn();

  if (mLogging.mHasRemoteDependency) {
    // for group commit
    std::unique_lock<std::mutex> g(mLogging.mTxToCommitMutex);
    mLogging.mTxToCommit.push_back(mActiveTx);
    DLOG(INFO) << "Puting transaction with remote dependency to mTxToCommit"
               << ", workerId=" << mWorkerId
               << ", startTs=" << mActiveTx.mStartTs
               << ", commitTs=" << mActiveTx.mCommitTs
               << ", maxObservedGSN=" << mActiveTx.mMaxObservedGSN;
  } else {
    // for group commit
    std::unique_lock<std::mutex> g(mLogging.mRfaTxToCommitMutex);
    CRCounters::MyCounters().rfa_committed_tx++;
    mLogging.mRfaTxToCommit.push_back(mActiveTx);
    DLOG(INFO) << "Puting transaction (RFA) to mRfaTxToCommit"
               << ", workerId=" << mWorkerId
               << ", startTs=" << mActiveTx.mStartTs
               << ", commitTs=" << mActiveTx.mCommitTs
               << ", maxObservedGSN=" << mActiveTx.mMaxObservedGSN;
  }

  // Cleanup versions in history tree
  mCc.GarbageCollection();

  // Wait transaction to be committed
  telemetry::MetricOnlyTimer timer;
  while (mLogging.TxUnCommitted(mActiveTx.mCommitTs)) {
  }
  METRIC_HIST_OBSERVE(mStore->mMetricsManager, tx_commit_wal_wait_us,
                      timer.ElaspedUs());
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
    METRIC_COUNTER_INC(mStore->mMetricsManager, tx_abort_total, 1);
    mActiveTxId.store(0, std::memory_order_release);
    LOG(INFO) << "Transaction aborted"
              << ", workerId=" << mWorkerId
              << ", startTs=" << mActiveTx.mStartTs
              << ", commitTs=" << mActiveTx.mCommitTs
              << ", maxObservedGSN=" << mActiveTx.mMaxObservedGSN;
  });

  if (!(mActiveTx.mState == TxState::kStarted && mActiveTx.mIsDurable)) {
    return;
  }

  // TODO(jian.z): support reading from WAL file once
  DCHECK(!mActiveTx.mWalExceedBuffer)
      << "Aborting from WAL file is not supported yet";

  std::vector<const WalEntry*> entries;
  mLogging.IterateCurrentTxWALs([&](const WalEntry& entry) {
    if (entry.mType == WalEntry::Type::kComplex) {
      entries.push_back(&entry);
    }
  });

  const uint64_t txId = mActiveTx.mStartTs;
  std::for_each(entries.rbegin(), entries.rend(), [&](const WalEntry* entry) {
    const auto& complexEntry = *reinterpret_cast<const WalEntryComplex*>(entry);
    mStore->mTreeRegistry->undo(complexEntry.mTreeId, complexEntry.mPayload,
                                txId);
  });

  mCc.mHistoryStorage.PurgeVersions(
      mActiveTx.mStartTs, mActiveTx.mStartTs,
      [&](const TXID, const TREEID, const uint8_t*, uint64_t, const bool) {},
      0);

  if (mActiveTx.mHasWrote && mActiveTx.mIsDurable) {
    // TODO: write compensation wal records between abort and finish
    mLogging.WriteWalTxAbort();
    mLogging.WriteWalTxFinish();
  }
}

} // namespace leanstore::cr
