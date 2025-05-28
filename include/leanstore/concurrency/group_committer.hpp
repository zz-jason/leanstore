#pragma once

#include "leanstore/lean_store.hpp"
#include "leanstore/units.hpp"
#include "leanstore/utils/async_io.hpp"
#include "leanstore/utils/user_thread.hpp"

#include <atomic>
#include <string>

#include <libaio.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

namespace leanstore::cr {

class WorkerContext;
class WalFlushReq;

/// The group committer thread is responsible for committing transactions in batches. It collects
/// wal records from all the worker threads, writes them to the wal file with libaio, and determines
/// the commitable transactions based on the min flushed GSN and min flushed transaction ID.
class GroupCommitter : public leanstore::utils::UserThread {
public:
  leanstore::LeanStore* store_;

  /// File descriptor of the underlying WAL file.
  const int32_t wal_fd_;

  /// Start file offset of the next WalEntry.
  uint64_t wal_size_;

  /// The minimum flushed system transaction ID among all worker threads. User transactions whose
  /// max observed system transaction ID not larger than it can be committed safely.
  std::atomic<TXID> global_min_flushed_sys_tx_;

  /// All the workers.
  std::vector<WorkerContext*>& worker_ctxs_;

  /// The libaio wrapper.
  utils::AsyncIo aio_;

public:
  GroupCommitter(leanstore::LeanStore* store, int32_t wal_fd, std::vector<WorkerContext*>& workers,
                 int cpu)
      : UserThread(store, "GroupCommitter", cpu),
        store_(store),
        wal_fd_(wal_fd),
        wal_size_(0),
        global_min_flushed_sys_tx_(0),
        worker_ctxs_(workers),
        aio_(workers.size() * 2 + 2) {
  }

  virtual ~GroupCommitter() override = default;

protected:
  virtual void run_impl() override;

private:
  /// Phase 1: collect wal records from all the worker threads. Collected wal records are written to
  /// libaio IOCBs.
  ///
  /// @param[out] minFlushedSysTx the min flushed system transaction ID
  /// @param[out] minFlushedUsrTx the min flushed user transaction ID
  /// @param[out] numRfaTxs number of transactions without dependency
  /// @param[out] walFlushReqCopies snapshot of the flush requests
  void collect_wal_records(TXID& min_flushed_sys_tx, TXID& min_flushed_usr_tx,
                           std::vector<uint64_t>& num_rfa_txs,
                           std::vector<WalFlushReq>& wal_flush_req_copies);

  /// Phase 2: write all the collected wal records to the wal file with libaio.
  void flush_wal_records();

  /// Phase 3: determine the commitable transactions based on minFlushedGSN and minFlushedTxId.
  ///
  /// @param[in] minFlushedSysTx the min flushed system transaction ID
  /// @param[in] minFlushedUsrTx the min flushed user transaction ID
  /// @param[in] numRfaTxs number of transactions without dependency
  /// @param[in] walFlushReqCopies snapshot of the flush requests
  void determine_commitable_tx(TXID min_flushed_sys_tx, TXID min_flushed_usr_tx,
                               const std::vector<uint64_t>& num_rfa_txs,
                               const std::vector<WalFlushReq>& wal_flush_req_copies);

  /// Append a wal entry to libaio IOCBs.
  ///
  /// @param[in] buf the wal entry buffer
  /// @param[in] lower the begin offset of the wal entry in the buffer
  /// @param[in] upper the end offset of the wal entry in the buffer
  void append(uint8_t* buf, uint64_t lower, uint64_t upper);
};

} // namespace leanstore::cr
