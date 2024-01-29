#include "Worker.hpp"

#include "CRMG.hpp"
#include "Config.hpp"
#include "LeanStore.hpp"
#include "concurrency-recovery/GroupCommitter.hpp"
#include "concurrency-recovery/Transaction.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "shared-headers/Exceptions.hpp"
#include "storage/buffer-manager/TreeRegistry.hpp"
#include "utils/Defer.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <mutex>

namespace leanstore::cr {

thread_local std::unique_ptr<Worker> Worker::sTlsWorker = nullptr;

Worker::Worker(u64 workerId, std::vector<Worker*>& allWorkers,
               leanstore::LeanStore* store)
    : mStore(store),
      cc(store, allWorkers.size()),
      mActiveTxId(0),
      mWorkerId(workerId),
      mAllWorkers(allWorkers) {
  CRCounters::MyCounters().mWorkerId = workerId;

  // init wal buffer
  mLogging.mWalBuffer = (u8*)(std::aligned_alloc(512, FLAGS_wal_buffer_size));
  std::memset(mLogging.mWalBuffer, 0, FLAGS_wal_buffer_size);

  cc.mLcbCacheVal = make_unique<u64[]>(mAllWorkers.size());
  cc.mLcbCacheKey = make_unique<u64[]>(mAllWorkers.size());
}

Worker::~Worker() {
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
               << ", globalMinFlushedGSN="
               << mStore->mCRManager->mGroupCommitter->mGlobalMinFlushedGSN
               << ", globalMaxFlushedGSN="
               << mStore->mCRManager->mGroupCommitter->mGlobalMaxFlushedGSN;
    if (!mActiveTx.mIsReadOnly && mActiveTx.mIsDurable) {
      mLogging.WriteSimpleWal(WALEntry::TYPE::TX_START);
    }
  });

  mActiveTx.Start(mode, level, isReadOnly);

  if (!FLAGS_wal) {
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
    LOG(FATAL) << "Unsupported isolation level: " << static_cast<u64>(level);
  }

  // Draw TXID from global counter and publish it with the TX type (i.e.
  // long-running or short-running) We have to acquire a transaction id and use
  // it for locking in ANY isolation level
  mActiveTx.mStartTs = mStore->AllocTs();
  auto curTxId = mActiveTx.mStartTs;
  if (FLAGS_enable_long_running_transaction && mActiveTx.IsLongRunning()) {
    // Mark as long-running transaction
    curTxId |= kLongRunningBit;
  }

  // Publish the transaction id
  mActiveTxId.store(curTxId, std::memory_order_release);
  cc.mGlobalWmkOfAllTx = mStore->mCRManager->mGlobalWmkInfo.mWmkOfAllTx.load();

  // Cleanup commit log if necessary
  cc.mCommitTree.CompactCommitLog();
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

  // Reset mCommandId on commit
  mCommandId = 0;
  if (mActiveTx.mHasWrote) {
    mActiveTx.mCommitTs = mStore->AllocTs();
    cc.mCommitTree.AppendCommitLog(mActiveTx.mStartTs, mActiveTx.mCommitTs);
    cc.mLatestCommitTs.store(mActiveTx.mCommitTs, std::memory_order_release);
  } else {
    DLOG(INFO) << "Transaction has no writes, skip assigning commitTs, append "
                  "log to commit tree, and group commit"
               << ", workerId=" << My().mWorkerId
               << ", actual startTs=" << mActiveTx.mStartTs;
  }

  // Reset startTs so that other transactions can safely update the global
  // transaction watermarks and garbage collect the unused versions.
  mActiveTxId.store(0, std::memory_order_release);

  if (!mActiveTx.mIsReadOnly && mActiveTx.mIsDurable) {
    mLogging.WriteSimpleWal(WALEntry::TYPE::TX_COMMIT);
    mLogging.WriteSimpleWal(WALEntry::TYPE::TX_FINISH);
  } else if (mActiveTx.mIsReadOnly) {
    DCHECK(!mActiveTx.mHasWrote)
        << "Read-only transaction should not have writes"
        << ", workerId=" << mWorkerId << ", startTs=" << mActiveTx.mStartTs;
  }

  if (mActiveTx.mHasWrote && mLogging.mHasRemoteDependency) {
    // for group commit
    mActiveTx.mMaxObservedGSN = mLogging.GetCurrentGsn();
    std::unique_lock<std::mutex> g(mLogging.mTxToCommitMutex);
    mLogging.mTxToCommit.push_back(mActiveTx);
    DLOG(INFO) << "Puting transaction with remote dependency to mTxToCommit"
               << ", workerId=" << mWorkerId
               << ", startTs=" << mActiveTx.mStartTs
               << ", commitTs=" << mActiveTx.mCommitTs
               << ", maxObservedGSN=" << mActiveTx.mMaxObservedGSN;
  } else if (mActiveTx.mHasWrote) {
    // for group commit
    mActiveTx.mMaxObservedGSN = mLogging.GetCurrentGsn();
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
  cc.GarbageCollection();

  // Wait transaction to be committed
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
    mActiveTxId.store(0, std::memory_order_release);
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
    mStore->mTreeRegistry->undo(complexEntry.mTreeId, complexEntry.payload,
                                txId);
  });

  cc.mHistoryTree->PurgeVersions(
      mWorkerId, mActiveTx.mStartTs, mActiveTx.mStartTs,
      [&](const TXID, const TREEID, const u8*, u64, const bool) {});

  if (!mActiveTx.mIsReadOnly && mActiveTx.mIsDurable) {
    // TODO: write compensation wal records between abort and finish
    mLogging.WriteSimpleWal(WALEntry::TYPE::TX_ABORT);
    mLogging.WriteSimpleWal(WALEntry::TYPE::TX_FINISH);
  }
}

} // namespace leanstore::cr
