#include "CRMG.hpp"

#include "profiling/counters/CPUCounters.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "utils/Misc.hpp"
#include "utils/Timer.hpp"

#include <glog/logging.h>

#include <chrono>
#include <cstring>
#include <thread>

#include <libaio.h>
#include <unistd.h>

using namespace std::chrono_literals;
namespace leanstore {
namespace cr {

void CRManager::runGroupCommiter() {
  [[maybe_unused]] leanstore::utils::SteadyTimer phase1Timer;
  [[maybe_unused]] leanstore::utils::SteadyTimer phase2Timer;
  [[maybe_unused]] leanstore::utils::SteadyTimer writeTimer;

  mRunningThreads++;

  // set thread name
  std::string thread_name("leanstore_group_committer");
  pthread_setname_np(pthread_self(), thread_name.c_str());
  CPUCounters::registerThread(thread_name, false);

  // Async IO. 2x because of potential wrapping around
  const u64 maxBatchSize = (mNumWorkerThreads * 2) + 2;
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

  auto addWalBufToPWrite = [&](u8* buf, u64 lower, u64 upper) {
    auto lowerAligned = utils::downAlign(lower);
    auto upperAligned = utils::upAlign(upper);
    auto data = buf + lowerAligned;
    auto size = upperAligned - lowerAligned;

    // TODO: add the concept of chunks
    DCHECK(u64(data) % 512 == 0);
    DCHECK(size % 512 == 0);
    DCHECK(mWalSize % 512 == 0);
    io_prep_pwrite(&iocbs[numIoSlots], mWalFd, data, size, mWalSize);
    mWalSize += size;

    iocbs[numIoSlots].data = data;
    iocbs_ptr[numIoSlots] = &iocbs[numIoSlots];
    numIoSlots++;

    COUNTERS_BLOCK() {
      CRCounters::myCounters().gct_write_bytes += size;
    }
  };

  LID minFlushedGsn; // For Remote Flush Avoidance
  LID maxFlushedGsn; // Sync all workers to this point
  TXID minFlushedCommitTs;
  std::vector<u64> readyToCommitRfaCut; // Exclusive
  std::vector<WalFlushReq> walFlushReqs;
  readyToCommitRfaCut.resize(mNumWorkerThreads, 0);
  walFlushReqs.resize(mNumWorkerThreads);

  while (mKeepRunning) {
    /// Phase 1: write WAL records from every worker thread on SSD.
    ///
    /// We use the asynchronous IO interface from libaio to batch all log writes
    /// and submit them using a single system call. Once the writes are done, we
    /// flush the block device with fsync to make sure that the log records we
    /// have just written are durable.
    numIoSlots = 0;
    CRCounters::myCounters().gct_rounds++;
    COUNTERS_BLOCK() {
      phase1Timer.Start();
    }

    minFlushedGsn = std::numeric_limits<LID>::max();
    maxFlushedGsn = 0;
    minFlushedCommitTs = std::numeric_limits<TXID>::max();

    for (u32 workerId = 0; workerId < mNumWorkerThreads; workerId++) {
      auto& logging = mWorkers[workerId]->mLogging;
      readyToCommitRfaCut[workerId] = logging.PreCommittedQueueRfaSize();
      walFlushReqs[workerId] = logging.mWalFlushReq.getSync();
      const auto& walFlushReq = walFlushReqs[workerId];

      maxFlushedGsn = std::max<LID>(maxFlushedGsn, walFlushReq.mCurrGSN);
      minFlushedGsn = std::min<LID>(minFlushedGsn, walFlushReq.mCurrGSN);
      minFlushedCommitTs =
          std::min<TXID>(minFlushedCommitTs, walFlushReq.mPrevTxCommitTs);

      const u64 buffered = walFlushReq.mWalBuffered;
      const u64 flushed = logging.mWalFlushed;
      const u64 bufferEnd = FLAGS_wal_buffer_size;

      if (buffered > flushed) {
        addWalBufToPWrite(logging.mWalBuffer, flushed, buffered);
      } else if (buffered < flushed) {
        addWalBufToPWrite(logging.mWalBuffer, flushed, bufferEnd);
        addWalBufToPWrite(logging.mWalBuffer, 0, buffered);
      }
    }

    COUNTERS_BLOCK() {
      phase1Timer.Stop();
      writeTimer.Start();
    }

    // submit all log writes using a single system call.
    DCHECK(mWalSize % 512 == 0);
    for (auto left = numIoSlots; left > 0;) {
      auto iocbToSubmit = iocbs_ptr.get() + numIoSlots - left;
      s32 submitted = io_submit(ioCtx, left, iocbToSubmit);
      LOG_IF(FATAL, submitted < 0)
          << "io_submit failed, return value=" << submitted
          << ", mWalFd=" << mWalFd << "left=" << left;
      left -= submitted;
    }
    if (numIoSlots > 0) {
      auto done_requests =
          io_getevents(ioCtx, numIoSlots, numIoSlots, ioEvents.get(), nullptr);
      LOG_IF(FATAL, done_requests < 0)
          << "io_getevents failed, return value=" << done_requests;
    }
    if (FLAGS_wal_fsync) {
      fdatasync(mWalFd);
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

    u64 committed_tx = 0;
    for (WORKERID workerId = 0; workerId < mNumWorkerThreads; workerId++) {
      auto& logging = mWorkers[workerId]->mLogging;
      const auto& walFlushReq = walFlushReqs[workerId];
      logging.UpdateFlushedCommitTs(walFlushReq.mPrevTxCommitTs);
      TXID signaled_up_to = std::numeric_limits<TXID>::max();
      // commit the pre-committed transactions in each worker, update
      // signaled_up_to
      {
        logging.mWalFlushed.store(walFlushReq.mWalBuffered,
                                  std::memory_order_release);
        std::unique_lock<std::mutex> g(logging.mPreCommittedQueueMutex);

        // commit thansactions in mPreCommittedQueue as many as possible
        u64 tx_i = 0;
        for (tx_i = 0; tx_i < logging.mPreCommittedQueue.size() &&
                       logging.mPreCommittedQueue[tx_i].CanCommit(
                           minFlushedGsn, minFlushedCommitTs);
             tx_i++) {
          logging.mPreCommittedQueue[tx_i].state = TX_STATE::COMMITTED;
        }

        // erase the committed transactions
        if (tx_i > 0) {
          auto maxCommitTs = logging.mPreCommittedQueue[tx_i - 1].commitTS();
          signaled_up_to = std::min<TXID>(signaled_up_to, maxCommitTs);
          logging.mPreCommittedQueue.erase(logging.mPreCommittedQueue.begin(),
                                           logging.mPreCommittedQueue.begin() +
                                               tx_i);
          committed_tx += tx_i;
        }

        // commit transactions in mPreCommittedQueueRfa as many as possible
        for (tx_i = 0; tx_i < readyToCommitRfaCut[workerId]; tx_i++) {
          logging.mPreCommittedQueueRfa[tx_i].state = TX_STATE::COMMITTED;
        }

        // erase the committed transactions
        if (tx_i > 0) {
          auto maxCommitTs = logging.mPreCommittedQueueRfa[tx_i - 1].commitTS();
          signaled_up_to = std::min<TXID>(signaled_up_to, maxCommitTs);
          logging.mPreCommittedQueueRfa.erase(
              logging.mPreCommittedQueueRfa.begin(),
              logging.mPreCommittedQueueRfa.begin() + tx_i);
          committed_tx += tx_i;
        }
      }

      if (signaled_up_to < std::numeric_limits<TXID>::max() &&
          signaled_up_to > 0) {
        logging.UpdateSignaledCommitTs(signaled_up_to);
      }
    }

    CRCounters::myCounters().gct_committed_tx += committed_tx;
    COUNTERS_BLOCK() {
      phase2Timer.Stop();
      CRCounters::myCounters().gct_phase_1_ms += phase1Timer.ElaspedUS();
      CRCounters::myCounters().gct_write_ms += writeTimer.ElaspedUS();
      CRCounters::myCounters().gct_phase_2_ms += phase2Timer.ElaspedUS();
    }

    Logging::UpdateMinFlushedGsn(minFlushedGsn);
    Logging::UpdateMaxFlushedGsn(maxFlushedGsn);
  }
  mRunningThreads--;
}

} // namespace cr
} // namespace leanstore
