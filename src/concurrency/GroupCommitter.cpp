#include "concurrency/GroupCommitter.hpp"

#include "concurrency/CRManager.hpp"
#include "concurrency/Worker.hpp"
#include "profiling/counters/CPUCounters.hpp"
#include "telemetry/MetricOnlyTimer.hpp"
#include "telemetry/MetricsManager.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <format>

namespace leanstore {
namespace cr {

void GroupCommitter::runImpl() {
  CPUCounters::registerThread(mThreadName, false);

  uint64_t minFlushedGSN = std::numeric_limits<uint64_t>::max();
  uint64_t maxFlushedGSN = 0;
  TXID minFlushedTxId = std::numeric_limits<TXID>::max();
  std::vector<uint64_t> numRfaTxs(mWorkers.size(), 0);
  std::vector<WalFlushReq> walFlushReqCopies(mWorkers.size());

  /// write WAL records from every worker thread to SSD.
  while (mKeepRunning) {

    // phase 1
    prepareIOCBs(minFlushedGSN, maxFlushedGSN, minFlushedTxId, numRfaTxs,
                 walFlushReqCopies);

    if (!mAIo.IsEmpty()) {
      // phase 2
      writeIOCBs();
    }

    // phase 3
    commitTXs(minFlushedGSN, maxFlushedGSN, minFlushedTxId, numRfaTxs,
              walFlushReqCopies);
  }
}

void GroupCommitter::prepareIOCBs(uint64_t& minFlushedGSN,
                                  uint64_t& maxFlushedGSN, TXID& minFlushedTxId,
                                  std::vector<uint64_t>& numRfaTxs,
                                  std::vector<WalFlushReq>& walFlushReqCopies) {
  leanstore::telemetry::MetricOnlyTimer timer;
  SCOPED_DEFER({
    METRIC_HIST_OBSERVE(mStore->mMetricsManager, group_committer_prep_iocbs_us,
                        timer.ElaspedUs());
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
    DLOG_IF(INFO, reqCopy.mCurrGSN == 27 || reqCopy.mCurrGSN == 28)
        << "minFlushedGSN=" << minFlushedGSN
        << ", workerGSN=" << reqCopy.mCurrGSN << ", workerId=" << workerId;

    // prepare IOCBs on demand
    const uint64_t buffered = reqCopy.mWalBuffered;
    const uint64_t flushed = logging.mWalFlushed;
    const uint64_t bufferEnd = FLAGS_wal_buffer_size;
    if (buffered > flushed) {
      setUpIOCB(logging.mWalBuffer, flushed, buffered);
    } else if (buffered < flushed) {
      setUpIOCB(logging.mWalBuffer, flushed, bufferEnd);
      setUpIOCB(logging.mWalBuffer, 0, buffered);
    }
  }

  if (!mAIo.IsEmpty() && FLAGS_wal_fsync) {
    mAIo.PrepareFsync(mWalFd);
  }
}

void GroupCommitter::writeIOCBs() {
  leanstore::telemetry::MetricOnlyTimer timer;
  SCOPED_DEFER({
    METRIC_HIST_OBSERVE(mStore->mMetricsManager, group_committer_write_iocbs_us,
                        timer.ElaspedUs());
  });

  // submit all log writes using a single system call.
  if (auto res = mAIo.SubmitAll(); !res) {
    LOG(ERROR) << std::format("Failed to submit all IO, error={}",
                              res.error().ToString());
  }

  /// wait all to finish.
  if (auto res = mAIo.WaitAll(); !res) {
    LOG(ERROR) << std::format("Failed to wait all IO, error={}",
                              res.error().ToString());
  }

  /// sync the metadata in the end.
  if (FLAGS_wal_fsync) {
    auto failed = fdatasync(mWalFd);
    LOG_IF(ERROR, failed) << std::format(
        "fdatasync failed, mWalFd={}, errno={}, error={}", mWalFd, errno,
        strerror(errno));
  }
}

void GroupCommitter::commitTXs(
    uint64_t minFlushedGSN, uint64_t maxFlushedGSN, TXID minFlushedTxId,
    const std::vector<uint64_t>& numRfaTxs,
    const std::vector<WalFlushReq>& walFlushReqCopies) {
  leanstore::telemetry::MetricOnlyTimer timer;
  SCOPED_DEFER({
    METRIC_HIST_OBSERVE(mStore->mMetricsManager, group_committer_commit_txs_us,
                        timer.ElaspedUs());
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
        DLOG(INFO) << "Transaction with remote dependency committed"
                   << ", workerId=" << workerId << ", startTs=" << tx.mStartTs
                   << ", commitTs=" << tx.mCommitTs
                   << ", minFlushedGSN=" << minFlushedGSN
                   << ", maxFlushedGSN=" << maxFlushedGSN
                   << ", minFlushedTxId=" << minFlushedTxId;
      }
      if (i > 0) {
        logging.mTxToCommit.erase(logging.mTxToCommit.begin(),
                                  logging.mTxToCommit.begin() + i);
      }
    }

    // commit transactions without remote dependency
    // TODO(jian.z): commit these transactions in the worker itself
    TXID maxCommitTsRfa = 0;
    {
      std::unique_lock<std::mutex> g(logging.mRfaTxToCommitMutex);
      uint64_t i = 0;
      for (; i < numRfaTxs[workerId]; ++i) {
        auto& tx = logging.mRfaTxToCommit[i];
        maxCommitTsRfa = std::max<TXID>(maxCommitTsRfa, tx.mCommitTs);
        tx.mState = TxState::kCommitted;
        DLOG(INFO) << "Transaction (RFA) committed"
                   << ", workerId=" << workerId << ", startTs=" << tx.mStartTs
                   << ", commitTs=" << tx.mCommitTs
                   << ", minFlushedGSN=" << minFlushedGSN
                   << ", maxFlushedGSN=" << maxFlushedGSN
                   << ", minFlushedTxId=" << minFlushedTxId;
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
    DLOG(INFO) << "Update globalMinFlushedGSN=" << minFlushedGSN
               << ", globalMaxFlushedGSN=" << maxFlushedGSN;
    mGlobalMinFlushedGSN.store(minFlushedGSN, std::memory_order_release);
    mGlobalMaxFlushedGSN.store(maxFlushedGSN, std::memory_order_release);
  }
}

void GroupCommitter::setUpIOCB(uint8_t* buf, uint64_t lower, uint64_t upper) {
  constexpr size_t kAligment = 4096;

  auto lowerAligned = utils::AlignDown(lower, kAligment);
  auto upperAligned = utils::AlignUp(upper, kAligment);
  auto* bufAligned = buf + lowerAligned;
  auto countAligned = upperAligned - lowerAligned;
  auto offsetAligned = utils::AlignDown(mWalSize, kAligment);

  mAIo.PrepareWrite(mWalFd, bufAligned, countAligned, offsetAligned);
  mWalSize += upper - lower;

  METRIC_COUNTER_INC(mStore->mMetricsManager, group_committer_disk_write_total,
                     countAligned);
};

} // namespace cr
} // namespace leanstore