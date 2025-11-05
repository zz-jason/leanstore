#pragma once

#include "leanstore/lean_store.hpp"
#include "leanstore/utils/async_io.hpp"
#include "leanstore/utils/managed_thread.hpp"

#include <string>

#include <libaio.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

namespace leanstore {

/// Forward declarations
class TxManager;
class WalFlushReq;

/// The group committer thread is responsible for committing transactions in
/// batches. It collects wal records from all the worker threads, writes them to
/// the wal file with libaio, and determines the commitable transactions based
/// on the min flushed system and user transaction ID.
class GroupCommitter : public utils::ManagedThread {
public:
  LeanStore* store_;

  /// File descriptor of the underlying WAL file.
  const int32_t wal_fd_;

  /// Start file offset of the next wal record.
  uint64_t wal_size_;

  /// The libaio wrapper.
  utils::AsyncIo aio_;

public:
  GroupCommitter(LeanStore* store, int cpu)
      : ManagedThread(store, "GroupCommitter", cpu),
        store_(store),
        wal_fd_(store->wal_fd_),
        wal_size_(0),
        aio_(store->store_option_->worker_threads_ * 2 + 2) {
  }

  virtual ~GroupCommitter() override = default;

protected:
  virtual void RunImpl() override;

private:
  /// Phase 1: collect wal records from all the worker threads. Collected wal records are written to
  /// libaio IOCBs.
  ///
  /// @param[out] minFlushedSysTx the min flushed system transaction ID
  /// @param[out] minFlushedUsrTx the min flushed user transaction ID
  /// @param[out] numRfaTxs number of transactions without dependency
  /// @param[out] walFlushReqCopies snapshot of the flush requests
  void CollectWalRecords(lean_txid_t& min_flushed_sys_tx, lean_txid_t& min_flushed_usr_tx,
                         std::vector<uint64_t>& num_rfa_txs,
                         std::vector<WalFlushReq>& wal_flush_req_copies);

  /// Phase 2: write all the collected wal records to the wal file with libaio.
  void FlushWalRecords();

  /// Phase 3: determine the commitable transactions based on min_flushed_sys_tx and
  /// min_flushed_usr_tx.
  ///
  /// @param[in] minFlushedSysTx the min flushed system transaction ID
  /// @param[in] minFlushedUsrTx the min flushed user transaction ID
  /// @param[in] numRfaTxs number of transactions without dependency
  /// @param[in] walFlushReqCopies snapshot of the flush requests
  void DetermineCommitableTx(lean_txid_t min_flushed_sys_tx, lean_txid_t min_flushed_usr_tx,
                             const std::vector<uint64_t>& num_rfa_txs,
                             const std::vector<WalFlushReq>& wal_flush_req_copies);

  /// Append a wal entry to libaio IOCBs.
  ///
  /// @param[in] buf the wal entry buffer
  /// @param[in] lower the begin offset of the wal entry in the buffer
  /// @param[in] upper the end offset of the wal entry in the buffer
  void Append(uint8_t* buf, uint64_t lower, uint64_t upper);
};

} // namespace leanstore
