#include "GroupCommitter.hpp"
#include "CRMG.hpp"
#include "Worker.hpp"

namespace leanstore {
namespace cr {

void GroupCommitter::runImpl() {
  CPUCounters::registerThread(mThreadName, false);

  [[maybe_unused]] leanstore::utils::SteadyTimer phase1Timer;
  [[maybe_unused]] leanstore::utils::SteadyTimer phase2Timer;
  [[maybe_unused]] leanstore::utils::SteadyTimer writeTimer;

  // Async IO. 2x because of potential wrapping around
  const u64 maxBatchSize = (FLAGS_worker_threads * 2) + 2;
  s32 numIoSlots = 0;
  auto iocbs = make_unique<struct iocb[]>(maxBatchSize);
  auto iocbs_ptr = make_unique<struct iocb*[]>(maxBatchSize);
  auto ioEvents = make_unique<struct io_event[]>(maxBatchSize);
  io_context_t ioCtx;
  {
    memset(&ioCtx, 0, sizeof(ioCtx));
    const int ret = io_setup(maxBatchSize, &ioCtx);
    LOG_IF(FATAL, ret != 0) << "io_setup failed, ret code = " << ret;
  }

  auto setUpIOControlBlock = [&](u8* buf, u64 lower, u64 upper) {
    auto lowerAligned = utils::downAlign(lower);
    auto upperAligned = utils::upAlign(upper);

    auto bufAligned = buf + lowerAligned;
    auto countAligned = upperAligned - lowerAligned;
    auto offsetAligned = utils::downAlign(mWalSize);

    DCHECK(u64(bufAligned) % 512 == 0);
    DCHECK(countAligned % 512 == 0);
    DCHECK(offsetAligned % 512 == 0);

    io_prep_pwrite(/* iocb */ &iocbs[numIoSlots], /* fd */ mWalFd,
                   /* buf */ bufAligned, /* count */ countAligned,
                   /* offset */ offsetAligned);
    mWalSize += upper - lower;

    iocbs[numIoSlots].data = bufAligned;
    iocbs_ptr[numIoSlots] = &iocbs[numIoSlots];
    numIoSlots++;

    DLOG(INFO) << "setUpIOControlBlock"
               << ", numIoSlots=" << numIoSlots << ", lower=" << lower
               << ", upper=" << upper << ", countAligned=" << countAligned
               << ", mWalSize=" << mWalSize;
    COUNTERS_BLOCK() {
      CRCounters::myCounters().gct_write_bytes += countAligned;
    }
  };

  u64 minFlushedGSN; // For Remote Flush Avoidance
  u64 maxFlushedGSN; // Sync all workers to this point
  TXID minFlushedCommitTs;
  std::vector<u64> readyToCommitRfaCut; // Exclusive
  std::vector<WalFlushReq> walFlushReqs;
  readyToCommitRfaCut.resize(FLAGS_worker_threads, 0);
  walFlushReqs.resize(FLAGS_worker_threads);

  /// write WAL records from every worker thread to SSD.
  while (mKeepRunning) {
    /// Phase 1: Prepare pwrite requests
    ///
    /// We use the asynchronous IO interface from libaio to batch all log writes
    /// and submit them using a single system call. Once the writes are done, we
    /// flush the block device with fsync to make sure that the log records we
    /// have just written are durable.
    numIoSlots = 0;
    COUNTERS_BLOCK() {
      CRCounters::myCounters().gct_rounds++;
      phase1Timer.Start();
    }

    minFlushedGSN = std::numeric_limits<u64>::max();
    maxFlushedGSN = 0;
    minFlushedCommitTs = std::numeric_limits<TXID>::max();

    for (u32 workerId = 0; workerId < FLAGS_worker_threads; workerId++) {
      auto& logging = mWorkers[workerId]->mLogging;
      readyToCommitRfaCut[workerId] = logging.PreCommittedQueueRfaSize();
      walFlushReqs[workerId] = logging.mWalFlushReq.getSync();
      const auto& walFlushReq = walFlushReqs[workerId];
      if (walFlushReq.mCurrGSN <= 0) {
        continue;
      }

      maxFlushedGSN = std::max<u64>(maxFlushedGSN, walFlushReq.mCurrGSN);
      minFlushedGSN = std::min<u64>(minFlushedGSN, walFlushReq.mCurrGSN);
      minFlushedCommitTs =
          std::min<TXID>(minFlushedCommitTs, walFlushReq.mPrevTxCommitTs);
      const u64 buffered = walFlushReq.mWalBuffered;
      const u64 flushed = logging.mWalFlushed;
      const u64 bufferEnd = FLAGS_wal_buffer_size;
      if (buffered > flushed) {
        setUpIOControlBlock(logging.mWalBuffer, flushed, buffered);
      } else if (buffered < flushed) {
        setUpIOControlBlock(logging.mWalBuffer, flushed, bufferEnd);
        setUpIOControlBlock(logging.mWalBuffer, 0, buffered);
      }
    }

    COUNTERS_BLOCK() {
      phase1Timer.Stop();
      writeTimer.Start();
    }

    // submit all log writes using a single system call.
    for (auto left = numIoSlots; left > 0;) {
      auto iocbToSubmit = iocbs_ptr.get() + numIoSlots - left;
      s32 submitted = io_submit(ioCtx, left, iocbToSubmit);
      LOG_IF(FATAL, submitted < 0)
          << "io_submit failed, return value=" << submitted
          << ", mWalFd=" << mWalFd;
      DLOG_IF(INFO, submitted >= 0)
          << "io_submit succeed, submitted=" << submitted
          << ", mWalFd=" << mWalFd;
      left -= submitted;
    }
    if (numIoSlots > 0) {
      auto done_requests =
          io_getevents(ioCtx, numIoSlots, numIoSlots, ioEvents.get(), nullptr);
      LOG_IF(FATAL, done_requests < 0)
          << "io_getevents failed, return value=" << done_requests;
      DLOG_IF(INFO, done_requests >= 0)
          << "io_getevents succeed, done_requests=" << done_requests;

      if (FLAGS_wal_fsync) {
        fdatasync(mWalFd);
      }
    }

    /// Phase 2: calculate the new safe set of transactions that are hardened
    /// and ready, signal their commit to the client.
    ///
    /// With this information in hand, we can commit the pre-committed
    /// transactions in each worker that have their own log and their
    /// dependencies hardened.
    COUNTERS_BLOCK() {
      writeTimer.Stop();
      phase2Timer.Start();
    }

    u64 numCommitted = 0;
    for (WORKERID workerId = 0; workerId < FLAGS_worker_threads; workerId++) {
      auto& logging = mWorkers[workerId]->mLogging;
      const auto& walFlushReq = walFlushReqs[workerId];
      logging.UpdateFlushedCommitTs(walFlushReq.mPrevTxCommitTs);
      TXID signaledUpTo = std::numeric_limits<TXID>::max();
      // commit the pre-committed transactions in each worker, update
      // signaledUpTo
      {
        logging.mWalFlushed.store(walFlushReq.mWalBuffered,
                                  std::memory_order_release);
        std::unique_lock<std::mutex> g(logging.mPreCommittedQueueMutex);

        // commit thansactions in mPreCommittedQueue as many as possible, and
        // erase the committed transactions
        u64 tx_i = 0;
        while (tx_i < logging.mPreCommittedQueue.size() &&
               logging.mPreCommittedQueue[tx_i].CanCommit(minFlushedGSN,
                                                          minFlushedCommitTs)) {
          DLOG(INFO) << "Transaction (startTs="
                     << logging.mPreCommittedQueue[tx_i].mStartTs
                     << ", commitTs="
                     << logging.mPreCommittedQueue[tx_i].mCommitTs
                     << ") in pre-committed queue is committed and erased from "
                        "the queue";
          logging.mPreCommittedQueue[tx_i].state = TX_STATE::COMMITTED;
          tx_i++;
        }
        if (tx_i > 0) {
          auto maxCommitTs = logging.mPreCommittedQueue[tx_i - 1].commitTS();
          signaledUpTo = std::min<TXID>(signaledUpTo, maxCommitTs);
          logging.mPreCommittedQueue.erase(logging.mPreCommittedQueue.begin(),
                                           logging.mPreCommittedQueue.begin() +
                                               tx_i);
          numCommitted += tx_i;
        }

        // commit transactions in mPreCommittedQueueRfa as many as possible, and
        // erase the committed transactions
        for (tx_i = 0; tx_i < readyToCommitRfaCut[workerId]; tx_i++) {
          DLOG(INFO) << "workerId=" << workerId
                     << ", commit transaction in pre-committed RFA queue"
                     << ", startaTs="
                     << logging.mPreCommittedQueueRfa[tx_i].mStartTs
                     << ", commitTs="
                     << logging.mPreCommittedQueueRfa[tx_i].mCommitTs
                     << ", posInQueue=" << tx_i
                     << ", minFlushedGSN=" << minFlushedGSN;
          logging.mPreCommittedQueueRfa[tx_i].state = TX_STATE::COMMITTED;
        }

        if (tx_i > 0) {
          auto maxCommitTs = logging.mPreCommittedQueueRfa[tx_i - 1].commitTS();
          signaledUpTo = std::min<TXID>(signaledUpTo, maxCommitTs);
          logging.mPreCommittedQueueRfa.erase(
              logging.mPreCommittedQueueRfa.begin(),
              logging.mPreCommittedQueueRfa.begin() + tx_i);
          numCommitted += tx_i;
        }
      }

      if (signaledUpTo < std::numeric_limits<TXID>::max() && signaledUpTo > 0) {
        logging.UpdateSignaledCommitTs(signaledUpTo);
        DLOG_IF(INFO, numCommitted > 0 ||
                          signaledUpTo < std::numeric_limits<TXID>::max())
            << "workerId=" << workerId << ", update transaction commit info"
            << ", signaledUpTo=" << signaledUpTo
            << ", numCommitted=" << numCommitted;
      }
    }

    COUNTERS_BLOCK() {
      CRCounters::myCounters().gct_committed_tx += numCommitted;
      phase2Timer.Stop();
      CRCounters::myCounters().gct_phase_1_ms += phase1Timer.ElaspedUS();
      CRCounters::myCounters().gct_write_ms += writeTimer.ElaspedUS();
      CRCounters::myCounters().gct_phase_2_ms += phase2Timer.ElaspedUS();
    }

    if (minFlushedGSN < std::numeric_limits<u64>::max()) {
      Logging::UpdateMinFlushedGsn(minFlushedGSN);
      Logging::UpdateMaxFlushedGsn(maxFlushedGSN);
    }
  }
}

} // namespace cr
} // namespace leanstore