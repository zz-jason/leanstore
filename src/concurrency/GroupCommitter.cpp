#include "concurrency/GroupCommitter.hpp"

#include "concurrency/CRMG.hpp"
#include "concurrency/Worker.hpp"
#include "leanstore/Exceptions.hpp"
#include "profiling/counters/CPUCounters.hpp"
#include "utils/Timer.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>

namespace leanstore {
namespace cr {

void GroupCommitter::runImpl() {
  CPUCounters::registerThread(mThreadName, false);

  int32_t numIOCBs = 0;
  uint64_t minFlushedGSN = std::numeric_limits<uint64_t>::max();
  uint64_t maxFlushedGSN = 0;
  TXID minFlushedTxId = std::numeric_limits<TXID>::max();
  std::vector<uint64_t> numRfaTxs(mWorkers.size(), 0);
  std::vector<WalFlushReq> walFlushReqCopies(mWorkers.size());

  /// write WAL records from every worker thread to SSD.
  while (mKeepRunning) {

    // phase 1
    prepareIOCBs(numIOCBs, minFlushedGSN, maxFlushedGSN, minFlushedTxId,
                 numRfaTxs, walFlushReqCopies);

    if (numIOCBs > 0) {
      // phase 2
      writeIOCBs(numIOCBs);
    }

    // phase 3
    commitTXs(minFlushedGSN, maxFlushedGSN, minFlushedTxId, numRfaTxs,
              walFlushReqCopies);
  }
}

void GroupCommitter::prepareIOCBs(int32_t& numIOCBs, uint64_t& minFlushedGSN,
                                  uint64_t& maxFlushedGSN, TXID& minFlushedTxId,
                                  std::vector<uint64_t>& numRfaTxs,
                                  std::vector<WalFlushReq>& walFlushReqCopies) {
  /// counters
  leanstore::utils::SteadyTimer phase1Timer [[maybe_unused]];
  COUNTERS_BLOCK() {
    CRCounters::MyCounters().gct_rounds++;
    phase1Timer.Start();
  }
  SCOPED_DEFER(COUNTERS_BLOCK() {
    phase1Timer.Stop();
    CRCounters::MyCounters().gct_phase_1_ms += phase1Timer.ElaspedUS();
  });

  numIOCBs = 0;
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
      setUpIOCB(numIOCBs, logging.mWalBuffer, flushed, buffered);
      numIOCBs++;
    } else if (buffered < flushed) {
      setUpIOCB(numIOCBs, logging.mWalBuffer, flushed, bufferEnd);
      numIOCBs++;
      setUpIOCB(numIOCBs, logging.mWalBuffer, 0, buffered);
      numIOCBs++;
    }
  }
}

void GroupCommitter::writeIOCBs(int32_t numIOCBs) {
  DCHECK(numIOCBs > 0) << "should have at least 1 IOCB to write";

  // counter
  leanstore::utils::SteadyTimer writeTimer [[maybe_unused]];
  COUNTERS_BLOCK() {
    writeTimer.Start();
  }
  SCOPED_DEFER(COUNTERS_BLOCK() {
    writeTimer.Stop();
    CRCounters::MyCounters().gct_write_ms += writeTimer.ElaspedUS();
  });

  // submit all log writes using a single system call.
  for (auto left = numIOCBs; left > 0;) {
    auto* iocbToSubmit = mIOCBPtrs.get() + numIOCBs - left;
    int32_t submitted = io_submit(mIOContext, left, iocbToSubmit);
    if (submitted < 0) {
      LOG(ERROR) << "io_submit failed"
                 << ", mWalFd=" << mWalFd << ", numIOCBs=" << numIOCBs
                 << ", left=" << left << ", errorCode=" << -submitted
                 << ", errorMsg=" << strerror(-submitted);
      return;
    }
    left -= submitted;
  }

  auto numCompleted =
      io_getevents(mIOContext, numIOCBs, numIOCBs, mIOEvents.get(), nullptr);
  if (numCompleted < 0) {
    LOG(ERROR) << "io_getevents failed"
               << ", mWalFd=" << mWalFd << ", numIOCBs=" << numIOCBs
               << ", errorCode=" << -numCompleted
               << ", errorMsg=" << strerror(-numCompleted);
    return;
  }

  if (FLAGS_wal_fsync) {
    auto failed = fdatasync(mWalFd);
    LOG_IF(ERROR, failed) << "fdatasync failed"
                          << ", mWalFd=" << mWalFd << ", errorCode=" << errno
                          << ", errorMsg=" << strerror(errno);
  }
}

void GroupCommitter::commitTXs(
    uint64_t minFlushedGSN, uint64_t maxFlushedGSN, TXID minFlushedTxId,
    const std::vector<uint64_t>& numRfaTxs,
    const std::vector<WalFlushReq>& walFlushReqCopies) {
  // commited transactions
  uint64_t numCommitted = 0;

  // counter
  leanstore::utils::SteadyTimer phase2Timer [[maybe_unused]];
  COUNTERS_BLOCK() {
    phase2Timer.Start();
  }
  SCOPED_DEFER(COUNTERS_BLOCK() {
    CRCounters::MyCounters().gct_committed_tx += numCommitted;
    phase2Timer.Stop();
    CRCounters::MyCounters().gct_phase_2_ms += phase2Timer.ElaspedUS();
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
        numCommitted += i;
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
        numCommitted += i;
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

void GroupCommitter::setUpIOCB(int32_t ioSlot, uint8_t* buf, uint64_t lower,
                               uint64_t upper) {
  auto lowerAligned = utils::AlignDown(lower);
  auto upperAligned = utils::AlignUp(upper);
  auto* bufAligned = buf + lowerAligned;
  auto countAligned = upperAligned - lowerAligned;
  auto offsetAligned = utils::AlignDown(mWalSize);

  DCHECK(uint64_t(bufAligned) % 512 == 0);
  DCHECK(countAligned % 512 == 0);
  DCHECK(offsetAligned % 512 == 0);

  io_prep_pwrite(/* iocb */ &mIOCBs[ioSlot], /* fd */ mWalFd,
                 /* buf */ bufAligned, /* count */ countAligned,
                 /* offset */ offsetAligned);
  mWalSize += upper - lower;
  mIOCBs[ioSlot].data = bufAligned;
  mIOCBPtrs[ioSlot] = &mIOCBs[ioSlot];
  COUNTERS_BLOCK() {
    CRCounters::MyCounters().gct_write_bytes += countAligned;
  }
};

} // namespace cr
} // namespace leanstore