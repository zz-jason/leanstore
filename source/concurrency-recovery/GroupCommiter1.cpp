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

void CRManager::groupCommiter1() {
  std::vector<std::thread> writer_threads;
  utils::Parallelize::range(
      FLAGS_wal_log_writers, mNumWorkerThreads,
      [&](u64 t_i, u64 w_begin_i, u64 w_end_i) {
        writer_threads.emplace_back([&, t_i, w_begin_i, w_end_i]() {
          const WORKERID workers_range_size = w_end_i - w_begin_i;
          mRunningThreads++;
          std::string thread_name("leanstore_log_writer_" +
                                  std::to_string(t_i));
          pthread_setname_np(pthread_self(), thread_name.c_str());
          CPUCounters::registerThread(thread_name, false);

          [[maybe_unused]] u64 round_i = 0; // For debugging

          // Async IO
          // 2x because of potential wrapping around
          const u64 batch_max_size = (workers_range_size * 2) + 2;
          s32 io_slot = 0;
          auto iocbs = make_unique<struct iocb[]>(batch_max_size);
          auto iocbs_ptr = make_unique<struct iocb*[]>(batch_max_size);
          auto events = make_unique<struct io_event[]>(batch_max_size);
          io_context_t aio_context;
          {
            memset(&aio_context, 0, sizeof(aio_context));
            const int ret = io_setup(batch_max_size, &aio_context);
            if (ret != 0) {
              throw ex::GenericException("io_setup failed, ret code = " +
                                         std::to_string(ret));
            }
          }
          auto add_pwrite = [&](u8* src, u64 size, u64 offset) {
            ENSURE(offset % 512 == 0);
            ENSURE(u64(src) % 512 == 0);
            ENSURE(size % 512 == 0);
            io_prep_pwrite(&iocbs[io_slot], mWalFd, src, size, offset);
            iocbs[io_slot].data = src;
            iocbs_ptr[io_slot] = &iocbs[io_slot];
            io_slot++;
          };
          // -------------------------------------------------------------------------------------
          std::vector<u64> ready_to_commit_rfa_cut; // Exclusive ) ==
          std::vector<WalFlushReq> walFlushReqs;
          ready_to_commit_rfa_cut.resize(workers_range_size, 0);
          walFlushReqs.resize(workers_range_size);
          // -------------------------------------------------------------------------------------
          while (mKeepRunning) {
            io_slot = 0;
            round_i++;
            // -------------------------------------------------------------------------------------
            // Phase 1
            for (u32 workerId = w_begin_i; workerId < w_end_i; workerId++) {
              auto& logging = mWorkers[workerId]->mLogging;
              {
                {
                  std::unique_lock<std::mutex> g(
                      logging.mPreCommittedQueueMutex);
                  ready_to_commit_rfa_cut[workerId - w_begin_i] =
                      logging.mPreCommittedQueueRfa.size();
                }
                walFlushReqs[workerId - w_begin_i] =
                    logging.mWalFlushReq.getSync();
              }
              if (walFlushReqs[workerId - w_begin_i].mWalBuffered >
                  logging.mWalFlushed) {
                const u64 lower_offset = utils::downAlign(logging.mWalFlushed);
                const u64 upper_offset = utils::upAlign(
                    walFlushReqs[workerId - w_begin_i].mWalBuffered);
                const u64 size_aligned = upper_offset - lower_offset;
                // -------------------------------------------------------------------------------------
                // TODO: add the concept of chunks
                const u64 walFileOffset =
                    sSsdOffset.fetch_add(-size_aligned) - size_aligned;
                add_pwrite(logging.mWalBuffer + lower_offset, size_aligned,
                           walFileOffset);
                // -------------------------------------------------------------------------------------
                COUNTERS_BLOCK() {
                  CRCounters::myCounters().gct_write_bytes += size_aligned;
                }
              } else if (walFlushReqs[workerId - w_begin_i].mWalBuffered <
                         logging.mWalFlushed) {
                {
                  const u64 lower_offset =
                      utils::downAlign(logging.mWalFlushed);
                  const u64 upper_offset = FLAGS_wal_buffer_size;
                  const u64 size_aligned = upper_offset - lower_offset;

                  const u64 walFileOffset =
                      sSsdOffset.fetch_add(-size_aligned) - size_aligned;
                  add_pwrite(logging.mWalBuffer + lower_offset, size_aligned,
                             walFileOffset);

                  COUNTERS_BLOCK() {
                    CRCounters::myCounters().gct_write_bytes += size_aligned;
                  }
                }
                {

                  const u64 lower_offset = 0;
                  const u64 upper_offset = utils::upAlign(
                      walFlushReqs[workerId - w_begin_i].mWalBuffered);
                  const u64 size_aligned = upper_offset - lower_offset;

                    const u64 walFileOffset =
                        sSsdOffset.fetch_add(-size_aligned) - size_aligned;
                    add_pwrite(logging.mWalBuffer, size_aligned, walFileOffset);

                    COUNTERS_BLOCK() {
                      CRCounters::myCounters().gct_write_bytes += size_aligned;
                    }
                }
              } else if (walFlushReqs[workerId - w_begin_i].mWalBuffered ==
                         logging.mWalFlushed) {
                // raise(SIGTRAP);
              }
            }

            // Flush
              ENSURE(sSsdOffset % 512 == 0);
                u32 submitted = 0;
                u32 left = io_slot;
                while (left) {
                  s32 ret_code =
                      io_submit(aio_context, left, iocbs_ptr.get() + submitted);
                  if (ret_code != s32(io_slot)) {
                    cout << ret_code << "," << io_slot << "," << sSsdOffset
                         << endl;
                    ENSURE(false);
                  }
                  POSIX_CHECK(ret_code >= 0);
                  submitted += ret_code;
                  left -= ret_code;
                }
                if (submitted > 0) {
                  const s32 done_requests = io_getevents(
                      aio_context, submitted, submitted, events.get(), NULL);
                  POSIX_CHECK(done_requests >= 0);
                }
                const u64 fsync_current_value = sFsyncCounter.load();
                while ((fsync_current_value + 2) >= sFsyncCounter.load() &&
                       mKeepRunning) {
                }

            // Phase 2, commit
            u64 committed_tx = 0;
            for (WORKERID workerId = w_begin_i; workerId < w_end_i;
                 workerId++) {
              auto& logging = mWorkers[workerId]->mLogging;
              logging.UpdateFlushedCommitTs(
                  walFlushReqs[workerId - w_begin_i].mPrevTxCommitTs);
              logging.UpdateFlushedGsn(
                  walFlushReqs[workerId - w_begin_i].mCurrGSN);
              TXID signaled_up_to = std::numeric_limits<TXID>::max();
              // TODO: prevent contention on mutex
              {
                logging.mWalFlushed.store(
                    walFlushReqs[workerId - w_begin_i].mWalBuffered,
                    std::memory_order_release);
                const auto time_now = std::chrono::high_resolution_clock::now();
                std::unique_lock<std::mutex> g(logging.mPreCommittedQueueMutex);
                // -------------------------------------------------------------------------------------
                u64 tx_i = 0;
                for (tx_i = 0; tx_i < logging.mPreCommittedQueue.size();
                     tx_i++) {
                  auto& tx = logging.mPreCommittedQueue[tx_i];
                  if (Logging::NeedIncrementFlushesCounter(tx)) {
                    tx.stats.flushes_counter++;
                    break;
                  }
                  tx.state = TX_STATE::COMMITTED;
                  tx.stats.commit = time_now;
                  if (1) {
                    const u64 cursor =
                        CRCounters::myCounters().cc_latency_cursor++ %
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
                      signaled_up_to,
                      logging.mPreCommittedQueue[tx_i - 1].commitTS());
                  logging.mPreCommittedQueue.erase(
                      logging.mPreCommittedQueue.begin(),
                      logging.mPreCommittedQueue.begin() + tx_i);
                  committed_tx += tx_i;
                }
                // -------------------------------------------------------------------------------------
                // RFA
                for (tx_i = 0;
                     tx_i < ready_to_commit_rfa_cut[workerId - w_begin_i];
                     tx_i++) {
                  auto& tx = logging.mPreCommittedQueueRfa[tx_i];
                  tx.state = TX_STATE::COMMITTED;
                  tx.stats.commit = time_now;
                  if (1) {
                    const u64 cursor =
                        CRCounters::myCounters().cc_rfa_latency_cursor++ %
                        CRCounters::latency_tx_capacity;
                    CRCounters::myCounters()
                        .cc_rfa_ms_precommit_latency[cursor] =
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
      });
  for (auto& thread : writer_threads) {
    thread.detach();
  }
}
// -------------------------------------------------------------------------------------
} // namespace cr
} // namespace leanstore
