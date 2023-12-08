#include "CRMG.hpp"

#include "profiling/counters/CPUCounters.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "utils/Misc.hpp"
#include "utils/Parallelize.hpp"

#include <libaio.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <thread>

using namespace std::chrono_literals;
namespace leanstore {
namespace cr {

void CRManager::groupCommiter2() {
  std::thread log_committer([&]() {
    mRunningThreads++;
    std::string thread_name("leanstore_log_committer");
    pthread_setname_np(pthread_self(), thread_name.c_str());
    CPUCounters::registerThread(thread_name, false);

    while (mKeepRunning) {
      u64 committed_tx = 0;
      for (WORKERID workerId = 0; workerId < mNumWorkerThreads; workerId++) {
        auto& logging = mWorkers[workerId]->mLogging;
        const auto time_now = std::chrono::high_resolution_clock::now();
        std::unique_lock<std::mutex> g(logging.mPreCommittedQueueMutex);

        u64 tx_i = 0;
        TXID signaled_up_to = std::numeric_limits<TXID>::max();
        for (tx_i = 0; tx_i < logging.mPreCommittedQueue.size(); tx_i++) {
          auto& tx = logging.mPreCommittedQueue[tx_i];
          if (Logging::NeedIncrementFlushesCounter(tx)) {
            tx.stats.flushes_counter++;
            break;
          }
          tx.state = TX_STATE::COMMITTED;
          tx.stats.commit = time_now;
          if (1) {
            const u64 cursor = CRCounters::myCounters().cc_latency_cursor++ %
                               CRCounters::latency_tx_capacity;
            CRCounters::myCounters().cc_ms_precommit_latency[cursor] =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    tx.stats.precommit - tx.stats.start)
                    .count();
            CRCounters::myCounters().cc_ms_commit_latency[cursor] =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    tx.stats.commit - tx.stats.start)
                    .count();
            CRCounters::myCounters().cc_flushes_counter[cursor] +=
                tx.stats.flushes_counter;
          }
        }
        if (tx_i > 0) {
          signaled_up_to = std::min<TXID>(
              signaled_up_to, logging.mPreCommittedQueue[tx_i - 1].commitTS());
          logging.mPreCommittedQueue.erase(logging.mPreCommittedQueue.begin(),
                                           logging.mPreCommittedQueue.begin() +
                                               tx_i);
          committed_tx += tx_i;
        }
      }

      // CRCounters::myCounters().gct_rounds += 1;
    }
    mRunningThreads--;
  });
  log_committer.detach();

  std::vector<std::thread> writer_threads;
  for (u64 t_i = 0; t_i < mNumWorkerThreads; t_i++) {
    writer_threads.emplace_back([&, t_i]() {
      const WORKERID workers_range_size = 1;
      const WORKERID workerId = t_i;
      mRunningThreads++;
      std::string thread_name("leanstore_log_writer_" + std::to_string(t_i));
      pthread_setname_np(pthread_self(), thread_name.c_str());
      CPUCounters::registerThread(thread_name, false);
      // -------------------------------------------------------------------------------------
      [[maybe_unused]] u64 round_i = 0; // For debugging
      // -------------------------------------------------------------------------------------
      // Async IO
      std::vector<u64> ready_to_commit_rfa_cut; // Exclusive ) ==
      std::vector<WalFlushReq> walFlushReqs;
      ready_to_commit_rfa_cut.resize(workers_range_size, 0);
      walFlushReqs.resize(workers_range_size);
      // -------------------------------------------------------------------------------------
      while (mKeepRunning) {
        round_i++;
        // -------------------------------------------------------------------------------------
        // Phase 1
        {
          auto& logging = mWorkers[workerId]->mLogging;
          {
            {
              std::unique_lock<std::mutex> g(logging.mPreCommittedQueueMutex);
              ready_to_commit_rfa_cut[0] = logging.mPreCommittedQueueRfa.size();
            }
            walFlushReqs[0] = logging.mWalFlushReq.getSync();
          }
          if (walFlushReqs[0].mWalBuffered > logging.mWalFlushed) {
            const u64 lower_offset = utils::downAlign(logging.mWalFlushed);
            const u64 upper_offset =
                utils::upAlign(walFlushReqs[0].mWalBuffered);
            const u64 size_aligned = upper_offset - lower_offset;
            // -------------------------------------------------------------------------------------
              // TODO: add the concept of chunks
              const u64 walFileOffset =
                  sSsdOffset.fetch_add(-size_aligned) - size_aligned;
              pwrite(mWalFd, logging.mWalBuffer + lower_offset, size_aligned,
                     walFileOffset);
              // add_pwrite(logging.mWalBuffer + lower_offset,
              // size_aligned, walFileOffset);
              // -------------------------------------------------------------------------------------
              COUNTERS_BLOCK() {
                CRCounters::myCounters().gct_write_bytes += size_aligned;
              }
          } else if (walFlushReqs[0].mWalBuffered < logging.mWalFlushed) {
            {
              // ------------XXXXXXXXX
              const u64 lower_offset = utils::downAlign(logging.mWalFlushed);
              const u64 upper_offset = FLAGS_wal_buffer_size;
              const u64 size_aligned = upper_offset - lower_offset;
              // -------------------------------------------------------------------------------------
                const u64 walFileOffset =
                    sSsdOffset.fetch_add(-size_aligned) - size_aligned;
                pwrite(mWalFd, logging.mWalBuffer + lower_offset, size_aligned,
                       walFileOffset);
                // add_pwrite(logging.mWalBuffer + lower_offset,
                // size_aligned, walFileOffset);
                // -------------------------------------------------------------------------------------
                COUNTERS_BLOCK() {
                  CRCounters::myCounters().gct_write_bytes += size_aligned;
                }
            }
            {
              // XXXXXX---------------
              const u64 lower_offset = 0;
              const u64 upper_offset =
                  utils::upAlign(walFlushReqs[0].mWalBuffered);
              const u64 size_aligned = upper_offset - lower_offset;
              // -------------------------------------------------------------------------------------
                const u64 walFileOffset =
                    sSsdOffset.fetch_add(-size_aligned) - size_aligned;
                pwrite(mWalFd, logging.mWalBuffer, size_aligned, walFileOffset);
                // add_pwrite(logging.mWalBuffer, size_aligned,
                // walFileOffset);
                // -------------------------------------------------------------------------------------
                COUNTERS_BLOCK() {
                  CRCounters::myCounters().gct_write_bytes += size_aligned;
                }
            }
          } else if (walFlushReqs[0].mWalBuffered == logging.mWalFlushed) {
            if (FLAGS_tmp7) {
              logging.mWalFlushReq.wait(walFlushReqs[0]);
            }
          }
        }
        // -------------------------------------------------------------------------------------
        // Flush
        if (FLAGS_wal_fsync) {
          ENSURE(sSsdOffset % 512 == 0);
          const u64 fsync_current_value = sFsyncCounter.load();
          sFsyncCounter.wait(fsync_current_value);
          while ((fsync_current_value + 2) >= sFsyncCounter.load() &&
                 mKeepRunning) {
          }
        }
        // -------------------------------------------------------------------------------------
        // Phase 2, commit
        u64 committed_tx = 0;
        {
          auto& logging = mWorkers[workerId]->mLogging;
          logging.UpdateFlushedCommitTs(walFlushReqs[0].mPrevTxCommitTs);
          logging.UpdateFlushedGsn(walFlushReqs[0].mCurrGSN);
          TXID signaled_up_to = std::numeric_limits<TXID>::max();
          // TODO: prevent contention on mutex
          {
            logging.mWalFlushed.store(walFlushReqs[0].mWalBuffered,
                                      std::memory_order_release);
            const auto time_now = std::chrono::high_resolution_clock::now();
            std::unique_lock<std::mutex> g(logging.mPreCommittedQueueMutex);
            // -------------------------------------------------------------------------------------
            // RFA
            u64 tx_i = 0;
            for (tx_i = 0; tx_i < ready_to_commit_rfa_cut[0]; tx_i++) {
              auto& tx = logging.mPreCommittedQueueRfa[tx_i];
              tx.state = TX_STATE::COMMITTED;
              tx.stats.commit = time_now;
              if (1) {
                const u64 cursor =
                    CRCounters::myCounters().cc_rfa_latency_cursor++ %
                    CRCounters::latency_tx_capacity;
                CRCounters::myCounters().cc_rfa_ms_precommit_latency[cursor] =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        tx.stats.precommit - tx.stats.start)
                        .count();
                CRCounters::myCounters().cc_rfa_ms_commit_latency[cursor] =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        tx.stats.commit - tx.stats.start)
                        .count();
              }
            }
            if (tx_i > 0) {
              signaled_up_to = std::min<TXID>(
                  signaled_up_to,
                  logging.mPreCommittedQueueRfa[tx_i - 1].commitTS());
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
        // CRCounters::myCounters().gct_rounds += 1;
        // CRCounters::myCounters().gct_rounds += t_i == 0;
      }
      mRunningThreads--;
    });
  }

  for (auto& thread : writer_threads) {
    thread.detach();
  }
}

} // namespace cr
} // namespace leanstore
