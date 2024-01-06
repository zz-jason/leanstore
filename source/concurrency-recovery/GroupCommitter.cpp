#include "GroupCommitter.hpp"
#include "CRMG.hpp"
#include "Worker.hpp"

namespace leanstore {
namespace cr {

void GroupCommitter::runImpl() {
  CPUCounters::registerThread(mThreadName, false);

  s32 numIOCBs = 0;
  u64 minFlushedGSN = std::numeric_limits<u64>::max();
  u64 maxFlushedGSN = 0;
  TXID minFlushedCommitTs = std::numeric_limits<TXID>::max();
  std::vector<u64> numRfaTxs(FLAGS_worker_threads, 0);
  std::vector<WalFlushReq> walFlushReqCopies(FLAGS_worker_threads);

  /// write WAL records from every worker thread to SSD.
  while (mKeepRunning) {

    // phase 1
    prepareIOCBs(numIOCBs, minFlushedGSN, maxFlushedGSN, minFlushedCommitTs,
                 numRfaTxs, walFlushReqCopies);

    if (numIOCBs > 0) {
      // phase 2
      writeIOCBs(numIOCBs);

      // phase 3
      commitTXs(minFlushedGSN, maxFlushedGSN, minFlushedCommitTs, numRfaTxs,
                walFlushReqCopies);
    }
  }
}

void GroupCommitter::prepareIOCBs(s32& numIOCBs, u64& minFlushedGSN,
                                  u64& maxFlushedGSN, TXID& minFlushedCommitTs,
                                  std::vector<u64>& numRfaTxs,
                                  std::vector<WalFlushReq>& walFlushReqCopies) {
  /// counters
  leanstore::utils::SteadyTimer phase1Timer [[maybe_unused]];
  COUNTERS_BLOCK() {
    CRCounters::myCounters().gct_rounds++;
    phase1Timer.Start();
  }
  SCOPED_DEFER(COUNTERS_BLOCK() {
    phase1Timer.Stop();
    CRCounters::myCounters().gct_phase_1_ms += phase1Timer.ElaspedUS();
  });

  numIOCBs = 0;
  minFlushedGSN = std::numeric_limits<u64>::max();
  maxFlushedGSN = 0;
  minFlushedCommitTs = std::numeric_limits<TXID>::max();

  for (u32 workerId = 0; workerId < mWorkers.size(); workerId++) {
    auto& logging = mWorkers[workerId]->mLogging;

    // collect logging info
    numRfaTxs[workerId] = logging.PreCommittedQueueRfaSize();
    walFlushReqCopies[workerId] = logging.mWalFlushReq.getSync();

    const auto& reqCopy = walFlushReqCopies[workerId];
    if (reqCopy.mCurrGSN <= 0) {
      // no transaction executed in the worker, skip it.
      continue;
    }

    // update GSN and commitTS info
    maxFlushedGSN = std::max<u64>(maxFlushedGSN, reqCopy.mCurrGSN);
    minFlushedGSN = std::min<u64>(minFlushedGSN, reqCopy.mCurrGSN);
    minFlushedCommitTs =
        std::min<TXID>(minFlushedCommitTs, reqCopy.mPrevTxCommitTs);

    // prepare IOCBs on demand
    const u64 buffered = reqCopy.mWalBuffered;
    const u64 flushed = logging.mWalFlushed;
    const u64 bufferEnd = FLAGS_wal_buffer_size;
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

void GroupCommitter::writeIOCBs(s32 numIOCBs) {
  DCHECK(numIOCBs > 0) << "should have at least 1 IOCB to write";

  // counter
  leanstore::utils::SteadyTimer writeTimer [[maybe_unused]];
  COUNTERS_BLOCK() {
    writeTimer.Start();
  }
  SCOPED_DEFER(COUNTERS_BLOCK() {
    writeTimer.Stop();
    CRCounters::myCounters().gct_write_ms += writeTimer.ElaspedUS();
  });

  // submit all log writes using a single system call.
  for (auto left = numIOCBs; left > 0;) {
    auto iocbToSubmit = mIOCBPtrs.get() + numIOCBs - left;
    s32 submitted = io_submit(mIOContext, left, iocbToSubmit);
    LOG_IF(ERROR, submitted < 0)
        << "io_submit failed, error=" << submitted << ", mWalFd=" << mWalFd;

    left -= submitted;
    DLOG_IF(INFO, submitted >= 0)
        << "io_submit succeed"
        << ", submitted=" << submitted << ", left=" << left
        << ", numIOCBs=" << numIOCBs << ", mWalFd=" << mWalFd;
  }

  auto numCompleted =
      io_getevents(mIOContext, numIOCBs, numIOCBs, mIOEvents.get(), nullptr);
  LOG_IF(ERROR, numCompleted < 0)
      << "io_getevents failed, error=" << numCompleted << ", mWalFd=" << mWalFd;
  DLOG_IF(INFO, numCompleted >= 0)
      << "io_getevents succeed, numCompleted=" << numCompleted;

  if (FLAGS_wal_fsync) {
    fdatasync(mWalFd);
  }
}

void GroupCommitter::commitTXs(
    u64 minFlushedGSN, u64 maxFlushedGSN, TXID minFlushedCommitTs,
    const std::vector<u64>& numRfaTxs,
    const std::vector<WalFlushReq>& walFlushReqCopies) {
  // commited transactions
  u64 numCommitted = 0;

  // counter
  leanstore::utils::SteadyTimer phase2Timer [[maybe_unused]];
  COUNTERS_BLOCK() {
    phase2Timer.Start();
  }
  SCOPED_DEFER(COUNTERS_BLOCK() {
    CRCounters::myCounters().gct_committed_tx += numCommitted;
    phase2Timer.Stop();
    CRCounters::myCounters().gct_phase_2_ms += phase2Timer.ElaspedUS();
  });

  for (WORKERID workerId = 0; workerId < mWorkers.size(); workerId++) {
    auto& logging = mWorkers[workerId]->mLogging;
    const auto& reqCopy = walFlushReqCopies[workerId];

    // update the flushed commit TS info
    logging.UpdateFlushedCommitTs(reqCopy.mPrevTxCommitTs);
    logging.mWalFlushed.store(reqCopy.mWalBuffered, std::memory_order_release);
    TXID signaledUpTo = std::numeric_limits<TXID>::max();

    // commit transactions with remote dependency
    {
      std::unique_lock<std::mutex> g(logging.mPreCommittedQueueMutex);
      u64 i = 0;
      for (; i < logging.mPreCommittedQueue.size(); ++i) {
        auto& tx = logging.mPreCommittedQueue[i];
        if (!tx.CanCommit(minFlushedGSN, minFlushedCommitTs)) {
          break;
        }
        tx.state = TX_STATE::COMMITTED;
        DLOG(INFO) << "Transaction with remote dependency committed"
                   << ", workerId=" << workerId << ", startTs=" << tx.mStartTs
                   << ", commitTs=" << tx.mCommitTs
                   << ", minFlushedGSN=" << minFlushedGSN
                   << ", minFlushedCommitTs=" << minFlushedCommitTs;
      }
      if (i > 0) {
        auto maxCommitTs = logging.mPreCommittedQueue[i - 1].commitTS();
        signaledUpTo = std::min<TXID>(signaledUpTo, maxCommitTs);
        logging.mPreCommittedQueue.erase(logging.mPreCommittedQueue.begin(),
                                         logging.mPreCommittedQueue.begin() +
                                             i);
        numCommitted += i;
      }
    }

    // commit transactions without remote dependency
    {
      std::unique_lock<std::mutex> g(logging.mPreCommittedQueueMutex);
      u64 i = 0;
      for (; i < numRfaTxs[workerId]; ++i) {
        auto& tx = logging.mPreCommittedQueueRfa[i];
        tx.state = TX_STATE::COMMITTED;
        DLOG(INFO) << "Transaction (RFA) committed"
                   << ", workerId=" << workerId << ", startTs=" << tx.mStartTs
                   << ", commitTs=" << tx.mCommitTs;
      }

      if (i > 0) {
        auto maxCommitTs = logging.mPreCommittedQueueRfa[i - 1].commitTS();
        signaledUpTo = std::min<TXID>(signaledUpTo, maxCommitTs);
        logging.mPreCommittedQueueRfa.erase(
            logging.mPreCommittedQueueRfa.begin(),
            logging.mPreCommittedQueueRfa.begin() + i);
        numCommitted += i;
      }
    }

    if (signaledUpTo < std::numeric_limits<TXID>::max() && signaledUpTo > 0) {
      logging.UpdateSignaledCommitTs(signaledUpTo);
    }
  }

  if (minFlushedGSN < std::numeric_limits<u64>::max()) {
    Logging::UpdateMinFlushedGsn(minFlushedGSN);
    Logging::UpdateMaxFlushedGsn(maxFlushedGSN);
  }
}

void GroupCommitter::setUpIOCB(s32 ioSlot, u8* buf, u64 lower, u64 upper) {
  auto lowerAligned = utils::downAlign(lower);
  auto upperAligned = utils::upAlign(upper);
  auto bufAligned = buf + lowerAligned;
  auto countAligned = upperAligned - lowerAligned;
  auto offsetAligned = utils::downAlign(mWalSize);

  DCHECK(u64(bufAligned) % 512 == 0);
  DCHECK(countAligned % 512 == 0);
  DCHECK(offsetAligned % 512 == 0);

  io_prep_pwrite(/* iocb */ &mIOCBs[ioSlot], /* fd */ mWalFd,
                 /* buf */ bufAligned, /* count */ countAligned,
                 /* offset */ offsetAligned);
  mWalSize += upper - lower;
  mIOCBs[ioSlot].data = bufAligned;
  mIOCBPtrs[ioSlot] = &mIOCBs[ioSlot];
  DLOG(INFO) << "setUpIOCB"
             << ", ioSlot=" << ioSlot << ", lower=" << lower
             << ", upper=" << upper << ", countAligned=" << countAligned
             << ", mWalSize=" << mWalSize;
  COUNTERS_BLOCK() {
    CRCounters::myCounters().gct_write_bytes += countAligned;
  }
};

} // namespace cr
} // namespace leanstore