#include "leanstore/concurrency/WorkerContext.hpp"

#include "leanstore-c/perf_counters.h"
#include "leanstore/LeanStore.hpp"
#include "leanstore/buffer-manager/TreeRegistry.hpp"
#include "leanstore/concurrency/CRManager.hpp"
#include "leanstore/concurrency/GroupCommitter.hpp"
#include "leanstore/concurrency/Logging.hpp"
#include "leanstore/concurrency/Transaction.hpp"
#include "leanstore/concurrency/WalEntry.hpp"
#include "leanstore/utils/CounterUtil.hpp"
#include "leanstore/utils/Defer.hpp"
#include "leanstore/utils/Log.hpp"

#include <algorithm>
#include <cstdlib>
#include <mutex>

namespace leanstore::cr {

thread_local std::unique_ptr<WorkerContext> WorkerContext::sTlsWorkerCtx = nullptr;
thread_local WorkerContext* WorkerContext::sTlsWorkerCtxRaw = nullptr;
thread_local PerfCounters tlsPerfCounters;

WorkerContext::WorkerContext(uint64_t workerId, std::vector<WorkerContext*>& allWorkers,
                             leanstore::LeanStore* store)
    : mStore(store),
      mCc(store, allWorkers.size()),
      mActiveTxId(0),
      mWorkerId(workerId),
      mAllWorkers(allWorkers) {

  // init wal buffer
  mLogging.mWalBufferSize = mStore->mStoreOption->mWalBufferSize;
  mLogging.mWalBuffer = (uint8_t*)(std::aligned_alloc(512, mLogging.mWalBufferSize));
  std::memset(mLogging.mWalBuffer, 0, mLogging.mWalBufferSize);

  mCc.mLcbCacheVal = std::make_unique<uint64_t[]>(mAllWorkers.size());
  mCc.mLcbCacheKey = std::make_unique<uint64_t[]>(mAllWorkers.size());
}

WorkerContext::~WorkerContext() {
  free(mLogging.mWalBuffer);
  mLogging.mWalBuffer = nullptr;
}

void WorkerContext::StartTx(TxMode mode, IsolationLevel level, bool isReadOnly) {
  Transaction prevTx [[maybe_unused]] = mActiveTx;
  LS_DCHECK(prevTx.mState != TxState::kStarted,
            "Previous transaction not ended, workerId={}, startTs={}, txState={}", mWorkerId,
            prevTx.mStartTs, TxStatUtil::ToString(prevTx.mState));
  SCOPED_DEFER({
    LS_DLOG("Start transaction, workerId={}, startTs={}, globalMinFlushedSysTx={}", mWorkerId,
            mActiveTx.mStartTs, mStore->mCRManager->mGroupCommitter->mGlobalMinFlushedSysTx.load());
  });

  mActiveTx.Start(mode, level);

  if (!mActiveTx.mIsDurable) {
    return;
  }

  //! Reset the max observed system transaction id
  mActiveTx.mMaxObservedSysTxId = mStore->mCRManager->mGroupCommitter->mGlobalMinFlushedSysTx;

  // Init wal and group commit related transaction information
  mLogging.mTxWalBegin = mLogging.mWalBuffered;

  // For now, we only support SI and SSI
  if (level < IsolationLevel::kSnapshotIsolation) {
    Log::Fatal("Unsupported isolation level: {}", static_cast<uint64_t>(level));
  }

  // Draw TXID from global counter and publish it with the TX type (i.e.  long-running or
  // short-running) We have to acquire a transaction id and use it for locking in ANY isolation
  // level
  if (isReadOnly) {
    mActiveTx.mStartTs = mStore->GetUsrTxTs();
  } else {
    mActiveTx.mStartTs = mStore->AllocUsrTxTs();
  }
  auto curTxId = mActiveTx.mStartTs;
  if (mStore->mStoreOption->mEnableLongRunningTx && mActiveTx.IsLongRunning()) {
    // Mark as long-running transaction
    curTxId |= kLongRunningBit;
  }

  // Publish the transaction id
  mActiveTxId.store(curTxId, std::memory_order_release);
  mCc.mGlobalWmkOfAllTx = mStore->mCRManager->mGlobalWmkInfo.mWmkOfAllTx.load();

  // Cleanup commit log if necessary
  mCc.mCommitTree.CompactCommitLog();
}

void WorkerContext::CommitTx() {
  SCOPED_DEFER({
    COUNTER_INC(&tlsPerfCounters.mTxCommitted);
    if (mActiveTx.mHasRemoteDependency) {
      COUNTER_INC(&tlsPerfCounters.mTxWithRemoteDependencies);
    } else {
      COUNTER_INC(&tlsPerfCounters.mTxWithoutRemoteDependencies);
    }
    mActiveTx.mState = TxState::kCommitted;
  });

  if (!mActiveTx.mIsDurable) {
    return;
  }

  // Reset mCommandId on commit
  mCommandId = 0;
  if (mActiveTx.mHasWrote) {
    mActiveTx.mCommitTs = mStore->AllocUsrTxTs();
    mCc.mCommitTree.AppendCommitLog(mActiveTx.mStartTs, mActiveTx.mCommitTs);
    mCc.mLatestCommitTs.store(mActiveTx.mCommitTs, std::memory_order_release);
  } else {
    LS_DLOG("Transaction has no writes, skip assigning commitTs, append log to "
            "commit tree, and group commit, workerId={}, actual startTs={}",
            mWorkerId, mActiveTx.mStartTs);
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

  // for group commit
  if (mActiveTx.mHasRemoteDependency) {
    std::unique_lock<std::mutex> g(mLogging.mTxToCommitMutex);
    mLogging.mTxToCommit.push_back(mActiveTx);
  } else {
    std::unique_lock<std::mutex> g(mLogging.mRfaTxToCommitMutex);
    mLogging.mRfaTxToCommit.push_back(mActiveTx);
  }

  // Cleanup versions in history tree
  mCc.GarbageCollection();

  // Wait logs to be flushed
  LS_DLOG("Wait transaction to commit, workerId={}, startTs={}, commitTs={}, maxObseredSysTx={}, "
          "hasRemoteDep={}",
          mWorkerId, mActiveTx.mStartTs, mActiveTx.mCommitTs, mActiveTx.mMaxObservedSysTxId,
          mActiveTx.mHasRemoteDependency);

  mLogging.WaitToCommit(mActiveTx.mCommitTs);
}

//! TODO(jian.z): revert changes made in-place on the btree process of a transaction abort:
//!
//! 1. Read previous wal entries
//! 2. Undo the changes via btree operations
//! 3. Write compensation wal entries during the undo process
//! 4. Purge versions in history tree, clean garbages made by the aborted transaction
//!
//! It may share the same code with the recovery process?
void WorkerContext::AbortTx() {
  SCOPED_DEFER({
    mActiveTx.mState = TxState::kAborted;
    COUNTER_INC(&tlsPerfCounters.mTxAborted);
    if (mActiveTx.mHasRemoteDependency) {
      COUNTER_INC(&tlsPerfCounters.mTxWithRemoteDependencies);
    } else {
      COUNTER_INC(&tlsPerfCounters.mTxWithoutRemoteDependencies);
    }
    mActiveTxId.store(0, std::memory_order_release);
    Log::Info("Transaction aborted, workerId={}, startTs={}, commitTs={}, maxObservedSysTx={}",
              mWorkerId, mActiveTx.mStartTs, mActiveTx.mCommitTs, mActiveTx.mMaxObservedSysTxId);
  });

  if (!(mActiveTx.mState == TxState::kStarted && mActiveTx.mIsDurable)) {
    return;
  }

  // TODO(jian.z): support reading from WAL file once
  LS_DCHECK(!mActiveTx.mWalExceedBuffer, "Aborting from WAL file is not supported yet");
  std::vector<const WalEntry*> entries;
  mLogging.IterateCurrentTxWALs([&](const WalEntry& entry) {
    if (entry.mType == WalEntry::Type::kComplex) {
      entries.push_back(&entry);
    }
  });

  const uint64_t txId = mActiveTx.mStartTs;
  std::for_each(entries.rbegin(), entries.rend(), [&](const WalEntry* entry) {
    const auto& complexEntry = *reinterpret_cast<const WalEntryComplex*>(entry);
    mStore->mTreeRegistry->undo(complexEntry.mTreeId, complexEntry.mPayload, txId);
  });

  mCc.mHistoryStorage.PurgeVersions(
      mActiveTx.mStartTs, mActiveTx.mStartTs,
      [&](const TXID, const TREEID, const uint8_t*, uint64_t, const bool) {}, 0);

  if (mActiveTx.mHasWrote && mActiveTx.mIsDurable) {
    // TODO: write compensation wal records between abort and finish
    mLogging.WriteWalTxAbort();
    mLogging.WriteWalTxFinish();
  }
}

PerfCounters* WorkerContext::GetPerfCounters() {
  return &tlsPerfCounters;
}

} // namespace leanstore::cr
