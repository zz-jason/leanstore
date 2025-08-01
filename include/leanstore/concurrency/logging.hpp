#pragma once

#include "leanstore-c/perf_counters.h"
#include "leanstore/concurrency/transaction.hpp"
#include "leanstore/sync/optimistic_guarded.hpp"
#include "leanstore/units.hpp"
#include "leanstore/utils/counter_util.hpp"
#include "leanstore/utils/misc.hpp"
#include "leanstore/utils/portable.hpp"
#include "utils/coroutine/coro_io.hpp"
#include "utils/coroutine/lean_mutex.hpp"

#include <algorithm>
#include <atomic>
#include <functional>
#include <string_view>
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
  uint64_t wal_buffer_bytes_;

  /// Used to track the write order of wal entries.
  LID lsn_clock_ = 0;

  /// The maximum writtern system transaction ID in the worker.
  TXID sys_tx_writtern_ = 0;

  /// Written offset of the wal ring buffer.
  uint64_t wal_buffered_ = 0;

  /// Flushed offset in the wal ring buffer. The wal ring buffer is firstly
  /// written by the worker thread then flushed to disk file by the group commit
  /// thread.
  std::atomic<uint64_t> wal_flushed_ = 0;

  /// The first WAL record of the current active transaction.
  uint64_t tx_wal_begin_;

  /// File descriptor for the write-ahead log.
  int32_t wal_fd_ = -1;

  /// Start offset of the next WalEntry.
  uint64_t wal_size_ = 0;

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

  void InitWalFd(std::string_view file_path) {
    wal_fd_ = open(file_path.data(), kFlags, kFileMode);
    if (wal_fd_ < 0) {
      Log::Fatal("Failed to init wal, file_path={}, error={}", file_path, strerror(errno));
    }
    Log::Info("Init wal succeed, file_path={}, fd={}", file_path, wal_fd_);
  }

  bool CoroFlush() {
    if (wal_buffered_ == wal_flushed_) {
      return false; // nothing to flush
    }

    if (wal_buffered_ > wal_flushed_) {
      CoroFlush(wal_flushed_, wal_buffered_);
    } else if (wal_buffered_ < wal_flushed_) {
      CoroFlush(wal_flushed_, wal_buffer_bytes_);
      CoroFlush(0, wal_buffered_);
    }
    wal_flushed_.store(wal_buffered_, std::memory_order_release);
    return true;
  }

private:
  static constexpr auto kFlags = O_DIRECT | O_RDWR | O_CREAT | O_TRUNC;
  static constexpr auto kFileMode = 0666;
  static constexpr auto kAligment = 4096u;

  void CoroFlush(uint64_t lower, uint64_t upper) {

    auto lower_aligned = utils::AlignDown(lower, kAligment);
    auto upper_aligned = utils::AlignUp(upper, kAligment);
    auto* buf_aligned = wal_buffer_ + lower_aligned;
    auto count_aligned = upper_aligned - lower_aligned;
    auto offset_aligned = utils::AlignDown(wal_size_, kAligment);

    CoroWrite(wal_fd_, buf_aligned, count_aligned, offset_aligned);
    wal_size_ += upper - lower;
  }

  // uint64_t PendingFlushBytes() const {
  //   if (wal_buffered_ >= wal_flushed_) {
  //     return wal_flushed_ - wal_flushed_;
  //   }
  //   return wal_buffered_ + (wal_buffer_bytes_ - wal_flushed_);
  // }

  void PublishWalBufferedOffset();

  void PublishWalFlushReq();

  /// Calculate the continuous free space left in the wal ring buffer. Return
  /// size of the contiguous free space.
  uint32_t WalContiguousFreeSpace();
};

} // namespace leanstore::cr