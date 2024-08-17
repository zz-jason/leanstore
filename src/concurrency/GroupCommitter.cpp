#include "leanstore/concurrency/GroupCommitter.hpp"

#include "leanstore/concurrency/CRManager.hpp"
#include "leanstore/concurrency/Worker.hpp"
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

  uint64_t minFlushedGSN = std::numeric_limits<uint64_t>::max();
  uint64_t maxFlushedGSN = 0;
  TXID minFlushedTxId = std::numeric_limits<TXID>::max();
  std::vector<uint64_t> numRfaTxs(mWorkers.size(), 0);
  std::vector<WalFlushReq> walFlushReqCopies(mWorkers.size());

  while (mKeepRunning) {
    // phase 1
    collectWalRecords(minFlushedGSN, maxFlushedGSN, minFlushedTxId, numRfaTxs, walFlushReqCopies);

    // phase 2
    if (!mAIo.IsEmpty()) {
      flushWalRecords();
    }

    // phase 3
    determineCommitableTx(minFlushedGSN, maxFlushedGSN, minFlushedTxId, numRfaTxs,
                          walFlushReqCopies);
  }
}

void GroupCommitter::collectWalRecords(uint64_t& minFlushedGSN, uint64_t& maxFlushedGSN,
                                       TXID& minFlushedTxId, std::vector<uint64_t>& numRfaTxs,
                                       std::vector<WalFlushReq>& walFlushReqCopies) {
  leanstore::telemetry::MetricOnlyTimer timer;
  SCOPED_DEFER({
    METRIC_HIST_OBSERVE(mStore->mMetricsManager, group_committer_prep_iocbs_us, timer.ElaspedUs());
  });

  minFlushedGSN = std::numeric_limits<uint64_t>::max();
  maxFlushedGSN = 0;
  minFlushedTxId = std::numeric_limits<TXID>::max();

  for (uint32_t workerId = 0; workerId < mWorkers.size(); workerId++) {
    auto& logging = mWorkers[workerId]->mLogging;
    // collect logging info
    std::unique_lock<std::mutex> guard(logging.mRfaTxToCommitMutex);
    numRfaTxs[workerId] = logging.mRfaTxToCommit.size();
    guard.unlock();

    auto lastReqVersion = walFlushReqCopies[workerId].mVersion;
    auto version = logging.mWalFlushReq.Get(walFlushReqCopies[workerId]);
    walFlushReqCopies[workerId].mVersion = version;
    const auto& reqCopy = walFlushReqCopies[workerId];
    if (reqCopy.mVersion == lastReqVersion) {
      // no transaction log write since last round group commit, skip.
      continue;
    }

    // update GSN and commitTS info
    maxFlushedGSN = std::max<uint64_t>(maxFlushedGSN, reqCopy.mCurrGSN);
    minFlushedGSN = std::min<uint64_t>(minFlushedGSN, reqCopy.mCurrGSN);
    minFlushedTxId = std::min<TXID>(minFlushedTxId, reqCopy.mCurrTxId);

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

void GroupCommitter::determineCommitableTx(uint64_t minFlushedGSN, uint64_t maxFlushedGSN,
                                           TXID minFlushedTxId,
                                           const std::vector<uint64_t>& numRfaTxs,
                                           const std::vector<WalFlushReq>& walFlushReqCopies) {
  leanstore::telemetry::MetricOnlyTimer timer;
  SCOPED_DEFER({
    METRIC_HIST_OBSERVE(mStore->mMetricsManager, group_committer_commit_txs_us, timer.ElaspedUs());
  });

  for (WORKERID workerId = 0; workerId < mWorkers.size(); workerId++) {
    auto& logging = mWorkers[workerId]->mLogging;
    const auto& reqCopy = walFlushReqCopies[workerId];

    // update the flushed commit TS info
    logging.mWalFlushed.store(reqCopy.mWalBuffered, std::memory_order_release);

    // commit transactions with remote dependency
    TXID maxCommitTs = 0;
    {
      std::unique_lock<std::mutex> g(logging.mTxToCommitMutex);
      uint64_t i = 0;
      for (; i < logging.mTxToCommit.size(); ++i) {
        auto& tx = logging.mTxToCommit[i];
        if (!tx.CanCommit(minFlushedGSN, minFlushedTxId)) {
          break;
        }
        maxCommitTs = std::max<TXID>(maxCommitTs, tx.mCommitTs);
        tx.mState = TxState::kCommitted;
        LS_DLOG("Transaction with remote dependency committed"
                ", workerId={}, startTs={}, commitTs={}, minFlushedGSN={}, "
                "maxFlushedGSN={}, minFlushedTxId={}",
                workerId, tx.mStartTs, tx.mCommitTs, minFlushedGSN, maxFlushedGSN, minFlushedTxId);
      }
      if (i > 0) {
        logging.mTxToCommit.erase(logging.mTxToCommit.begin(), logging.mTxToCommit.begin() + i);
      }
    }

    // commit transactions without remote dependency
    TXID maxCommitTsRfa = 0;
    {
      std::unique_lock<std::mutex> g(logging.mRfaTxToCommitMutex);
      uint64_t i = 0;
      for (; i < numRfaTxs[workerId]; ++i) {
        auto& tx = logging.mRfaTxToCommit[i];
        maxCommitTsRfa = std::max<TXID>(maxCommitTsRfa, tx.mCommitTs);
        tx.mState = TxState::kCommitted;
        LS_DLOG("Transaction without remote dependency committed"
                ", workerId={}, startTs={}, commitTs={}, minFlushedGSN={}, "
                "maxFlushedGSN={}, minFlushedTxId={}",
                workerId, tx.mStartTs, tx.mCommitTs, minFlushedGSN, maxFlushedGSN, minFlushedTxId);
      }

      if (i > 0) {
        logging.mRfaTxToCommit.erase(logging.mRfaTxToCommit.begin(),
                                     logging.mRfaTxToCommit.begin() + i);
      }
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

  if (minFlushedGSN < std::numeric_limits<uint64_t>::max()) {
    LS_DLOG("Group commit finished, minFlushedGSN={}, maxFlushedGSN={}", minFlushedGSN,
            maxFlushedGSN);
    mGlobalMinFlushedGSN.store(minFlushedGSN, std::memory_order_release);
    mGlobalMaxFlushedGSN.store(maxFlushedGSN, std::memory_order_release);
  }
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