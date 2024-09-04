#include "leanstore/concurrency/GroupCommitter.hpp"

#include "leanstore/concurrency/CRManager.hpp"
#include "leanstore/concurrency/Transaction.hpp"
#include "leanstore/concurrency/WorkerContext.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/telemetry/MetricOnlyTimer.hpp"
#include "telemetry/MetricsManager.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <format>

namespace leanstore::cr {

//! The alignment of the WAL record
constexpr size_t kAligment = 4096;

void GroupCommitter::runImpl() {
  CPUCounters::registerThread(mThreadName, false);

  TXID minFlushedSysTx = std::numeric_limits<TXID>::max();
  TXID minFlushedUsrTx = std::numeric_limits<TXID>::max();
  std::vector<uint64_t> numRfaTxs(mWorkerCtxs.size(), 0);
  std::vector<WalFlushReq> walFlushReqCopies(mWorkerCtxs.size());

  while (mKeepRunning) {
    // phase 1
    collectWalRecords(minFlushedSysTx, minFlushedUsrTx, numRfaTxs, walFlushReqCopies);

    // phase 2
    if (!mAIo.IsEmpty()) {
      flushWalRecords();
    }

    // phase 3
    determineCommitableTx(minFlushedSysTx, minFlushedUsrTx, numRfaTxs, walFlushReqCopies);
  }
}

void GroupCommitter::collectWalRecords(TXID& minFlushedSysTx, TXID& minFlushedUsrTx,
                                       std::vector<uint64_t>& numRfaTxs [[maybe_unused]],
                                       std::vector<WalFlushReq>& walFlushReqCopies) {
  leanstore::telemetry::MetricOnlyTimer timer;
  SCOPED_DEFER({
    METRIC_HIST_OBSERVE(mStore->mMetricsManager, group_committer_prep_iocbs_us, timer.ElaspedUs());
  });

  minFlushedSysTx = std::numeric_limits<TXID>::max();
  minFlushedUsrTx = std::numeric_limits<TXID>::max();

  for (auto workerId = 0u; workerId < mWorkerCtxs.size(); workerId++) {
    auto& logging = mWorkerCtxs[workerId]->mLogging;

    auto lastReqVersion = walFlushReqCopies[workerId].mVersion;
    auto version = logging.mWalFlushReq.Get(walFlushReqCopies[workerId]);
    walFlushReqCopies[workerId].mVersion = version;
    const auto& reqCopy = walFlushReqCopies[workerId];

    if (reqCopy.mVersion == lastReqVersion) {
      // no transaction log write since last round group commit, skip.
      continue;
    }

    if (reqCopy.mSysTxWrittern > 0) {
      minFlushedSysTx = std::min(minFlushedSysTx, reqCopy.mSysTxWrittern);
    }
    if (reqCopy.mCurrTxId > 0) {
      minFlushedUsrTx = std::min(minFlushedUsrTx, reqCopy.mCurrTxId);
    }

    // prepare IOCBs on demand
    const uint64_t buffered = reqCopy.mWalBuffered;
    const uint64_t flushed = logging.mWalFlushed;
    const uint64_t bufferEnd = mStore->mStoreOption->mWalBufferSize;
    if (buffered > flushed) {
      append(logging.mWalBuffer, flushed, buffered);
    } else if (buffered < flushed) {
      append(logging.mWalBuffer, flushed, bufferEnd);
      append(logging.mWalBuffer, 0, buffered);
    }
  }

  if (!mAIo.IsEmpty() && mStore->mStoreOption->mEnableWalFsync) {
    mAIo.PrepareFsync(mWalFd);
  }
}

void GroupCommitter::flushWalRecords() {
  leanstore::telemetry::MetricOnlyTimer timer;
  SCOPED_DEFER({
    METRIC_HIST_OBSERVE(mStore->mMetricsManager, group_committer_write_iocbs_us, timer.ElaspedUs());
  });

  // submit all log writes using a single system call.
  if (auto res = mAIo.SubmitAll(); !res) {
    Log::Error("Failed to submit all IO, error={}", res.error().ToString());
  }

  //! wait all to finish.
  timespec timeout = {1, 0}; // 1s
  if (auto res = mAIo.WaitAll(&timeout); !res) {
    Log::Error("Failed to wait all IO, error={}", res.error().ToString());
  }

  //! sync the metadata in the end.
  if (mStore->mStoreOption->mEnableWalFsync) {
    auto failed = fdatasync(mWalFd);
    if (failed) {
      Log::Error("fdatasync failed, errno={}, error={}", errno, strerror(errno));
    }
  }
}

void GroupCommitter::determineCommitableTx(TXID minFlushedSysTx, TXID minFlushedUsrTx,
                                           const std::vector<uint64_t>& numRfaTxs [[maybe_unused]],
                                           const std::vector<WalFlushReq>& walFlushReqCopies) {
  leanstore::telemetry::MetricOnlyTimer timer;
  SCOPED_DEFER({
    METRIC_HIST_OBSERVE(mStore->mMetricsManager, group_committer_commit_txs_us, timer.ElaspedUs());
  });

  for (WORKERID workerId = 0; workerId < mWorkerCtxs.size(); workerId++) {
    auto& logging = mWorkerCtxs[workerId]->mLogging;
    const auto& reqCopy = walFlushReqCopies[workerId];

    // update the flushed commit TS info
    logging.mWalFlushed.store(reqCopy.mWalBuffered, std::memory_order_release);

    // commit transactions with remote dependency
    TXID maxCommitTs = 0;
    {
      if (auto* tx = logging.mActiveTxToCommit.load(std::memory_order_relaxed);
          tx != nullptr && tx->CanCommit(minFlushedSysTx, minFlushedUsrTx)) {
        maxCommitTs = tx->mCommitTs;
        tx->mState = TxState::kCommitted;
        logging.mActiveTxToCommit.store(nullptr);
        LS_DLOG("Transaction with remote dependency committed, workerId={}, startTs={}, "
                "commitTs={}, minFlushedSysTx={}, minFlushedUsrTx={}",
                workerId, tx->mStartTs, tx->mCommitTs, minFlushedSysTx, minFlushedUsrTx);
      }
    }

    // commit transactions without remote dependency
    TXID maxCommitTsRfa = 0;
    if (auto* tx = logging.mActiveRfaTxToCommit.load(std::memory_order_relaxed); tx != nullptr) {
      maxCommitTsRfa = tx->mCommitTs;
      tx->mState = TxState::kCommitted;
      logging.mActiveRfaTxToCommit.store(nullptr);
      LS_DLOG("Transaction without remote dependency committed, workerId={}, "
              "startTs={}, commitTs={}",
              workerId, tx->mStartTs, tx->mCommitTs);
    }

    // Has committed transaction
    TXID signaledUpTo = 0;
    if (maxCommitTs == 0 && maxCommitTsRfa != 0) {
      signaledUpTo = maxCommitTsRfa;
    } else if (maxCommitTs != 0 && maxCommitTsRfa == 0) {
      signaledUpTo = maxCommitTs;
    } else if (maxCommitTs != 0 && maxCommitTsRfa != 0) {
      signaledUpTo = std::min<TXID>(maxCommitTs, maxCommitTsRfa);
    }
    if (signaledUpTo > 0) {
      logging.UpdateSignaledCommitTs(signaledUpTo);
    }
  }

  mGlobalMinFlushedSysTx.store(minFlushedSysTx, std::memory_order_release);
}

void GroupCommitter::append(uint8_t* buf, uint64_t lower, uint64_t upper) {
  auto lowerAligned = utils::AlignDown(lower, kAligment);
  auto upperAligned = utils::AlignUp(upper, kAligment);
  auto* bufAligned = buf + lowerAligned;
  auto countAligned = upperAligned - lowerAligned;
  auto offsetAligned = utils::AlignDown(mWalSize, kAligment);

  mAIo.PrepareWrite(mWalFd, bufAligned, countAligned, offsetAligned);
  mWalSize += upper - lower;

  METRIC_COUNTER_INC(mStore->mMetricsManager, group_committer_disk_write_total, countAligned);
};

} // namespace leanstore::cr