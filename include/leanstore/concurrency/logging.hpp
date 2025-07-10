#pragma once

#include "leanstore-c/perf_counters.h"
#include "leanstore/concurrency/transaction.hpp"
#include "leanstore/sync/optimistic_guarded.hpp"
#include "leanstore/units.hpp"
#include "leanstore/utils/counter_util.hpp"
#include "leanstore/utils/portable.hpp"
#include "utils/coroutine/lean_mutex.hpp"

#include <algorithm>
#include <atomic>
#include <functional>
#include <vector>

namespace leanstore::cr {

/// forward declarations
class WalEntry;
class WalEntryComplex;

/// Used to sync wal flush request between group committer and worker.
struct WalFlushReq {
  /// Used for optimistic locking.
  uint64_t version_ = 0;

  /// The offset in the wal ring buffer.
  uint64_t wal_buffered_ = 0;

  /// The maximum system transasction ID written by the worker.
  /// NOTE: can only be updated when all the WAL entries belonging to the system transaction are
  /// written to the wal ring buffer.
  TXID sys_tx_writtern_ = 0;

  /// ID of the current transaction.
  /// NOTE: can only be updated when all the WAL entries belonging to the user transaction are
  /// written to the wal ring buffer.
  TXID curr_tx_id_ = 0;

  WalFlushReq(uint64_t wal_buffered = 0, uint64_t sys_tx_writtern = 0, TXID curr_tx_id = 0)
      : version_(0),
        wal_buffered_(wal_buffered),
        sys_tx_writtern_(sys_tx_writtern),
        curr_tx_id_(curr_tx_id) {
  }
};

template <typename T>
class WalPayloadHandler;

/// Helps to transaction concurrenct control and write-ahead logging.
class Logging {
public:
  LID prev_lsn_;

  /// The active complex WalEntry for the current transaction, usually used for insert, update,
  /// delete, or btree related operations.
  ///
  /// NOTE: Either active_walentry_simple_ or active_walentry_complex_ is effective during
  /// transaction processing.
  WalEntryComplex* active_walentry_complex_;

  /// Protects tx_to_commit_
  LeanMutex tx_to_commit_mutex_;

  /// The queue for each worker thread to store pending-to-commit transactions which have remote
  /// dependencies.
  std::vector<Transaction> tx_to_commit_;

  /// Protects tx_to_commit_
  LeanMutex rfa_tx_to_commit_mutex_;

  /// The queue for each worker thread to store pending-to-commit transactions which doesn't have
  /// any remote dependencies.
  std::vector<Transaction> rfa_tx_to_commit_;

  /// Represents the maximum commit timestamp in the worker. Transactions in the worker are
  /// committed if their commit timestamps are smaller than it.
  ///
  /// Updated by group committer
  std::atomic<TXID> signaled_commit_ts_ = 0;

  storage::OptimisticGuarded<WalFlushReq> wal_flush_req_;

  /// The ring buffer of the current worker thread. All the wal entries of the current worker are
  /// writtern to this ring buffer firstly, then flushed to disk by the group commit thread.
  ALIGNAS(512) uint8_t* wal_buffer_;

  /// The size of the wal ring buffer.
  uint64_t wal_buffer_size_;

  /// Used to track the write order of wal entries.
  LID lsn_clock_ = 0;

  /// The maximum writtern system transaction ID in the worker.
  TXID sys_tx_writtern_ = 0;

  /// The written offset of the wal ring buffer.
  uint64_t wal_buffered_ = 0;

  /// Represents the flushed offset in the wal ring buffer.  The wal ring buffer is firstly written
  /// by the worker thread then flushed to disk file by the group commit thread.
  std::atomic<uint64_t> wal_flushed_ = 0;

  /// The first WAL record of the current active transaction.
  uint64_t tx_wal_begin_;

public:
  void UpdateSignaledCommitTs(const LID signaled_commit_ts) {
    signaled_commit_ts_.store(signaled_commit_ts, std::memory_order_release);
  }

  void WaitToCommit(const TXID commit_ts) {
    COUNTER_INC(&tls_perf_counters.tx_commit_wait_);
    while (!(commit_ts <= signaled_commit_ts_.load())) {
    }
  }

  void ReserveContiguousBuffer(uint32_t requested_size);

  /// Iterate over current TX entries
  void IterateCurrentTxWALs(std::function<void(const WalEntry& entry)> callback);

  void WriteWalTxAbort();
  void WriteWalTxFinish();
  void WriteWalCarriageReturn();

  template <typename T, typename... Args>
  WalPayloadHandler<T> ReserveWALEntryComplex(uint64_t payload_size, PID page_id, LID psn,
                                              TREEID tree_id, Args&&... args);

  /// Submits wal record to group committer when it is ready to flush to disk.
  /// @param totalSize size of the wal record to be flush.
  void SubmitWALEntryComplex(uint64_t total_size);

  void UpdateSysTxWrittern(TXID sys_tx_id) {
    sys_tx_writtern_ = std::max(sys_tx_writtern_, sys_tx_id);
  }

private:
  void PublishWalBufferedOffset();

  void PublishWalFlushReq();

  /// Calculate the continuous free space left in the wal ring buffer. Return
  /// size of the contiguous free space.
  uint32_t WalContiguousFreeSpace();
};

} // namespace leanstore::cr