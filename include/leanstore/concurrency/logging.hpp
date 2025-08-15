#pragma once

#include "leanstore/concurrency/wal_entry.hpp"
#include "leanstore/sync/optimistic_guarded.hpp"
#include "leanstore/units.hpp"
#include "leanstore/utils/misc.hpp"
#include "leanstore/utils/portable.hpp"
#include "utils/coroutine/coro_io.hpp"

#include <algorithm>
#include <atomic>
#include <functional>
#include <string_view>

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
  /// Logical sequence number, i.e., the unique ID of each WAL.
  LID lsn_clock_ = 0;

  /// The previous LSN of the current transaction, used to link WAL entries.
  LID prev_lsn_;

  storage::OptimisticGuarded<WalFlushReq> wal_flush_req_;

  /// The maximum writtern system transaction ID in the worker.
  TXID sys_tx_writtern_ = 0;

  /// File descriptor for the write-ahead log.
  int32_t wal_fd_ = -1;

  /// Start offset of the next WalEntry.
  uint64_t wal_size_ = 0;

  /// The size of the wal ring buffer.
  uint64_t wal_buffer_bytes_;

  /// Written offset of the wal ring buffer.
  uint64_t wal_buffered_ = 0;

  /// Flushed offset in the wal ring buffer. The wal ring buffer is firstly
  /// written by the worker thread then flushed to disk file by the group commit
  /// thread.
  std::atomic<uint64_t> wal_flushed_ = 0;

  /// The ring buffer of the current worker thread. All the wal entries of the current worker are
  /// writtern to this ring buffer firstly, then flushed to disk by the group commit thread.
  ALIGNAS(512) uint8_t* wal_buffer_;

public:
  Logging(uint64_t wal_buffer_bytes)
      : wal_buffer_bytes_(wal_buffer_bytes),
        wal_buffer_((uint8_t*)(std::aligned_alloc(512, wal_buffer_bytes))) {
    std::memset(wal_buffer_, 0, wal_buffer_bytes);
  }

  ~Logging() {
    if (wal_buffer_ != nullptr) {
      free(wal_buffer_);
      wal_buffer_ = nullptr;
    }
  }

  LID ReserveLsn() {
    return lsn_clock_++;
  }

  uint8_t* ReserveWalBuffer(uint32_t requested_size);

  void AdvanceWalBuffer(uint32_t size) {
    LEAN_DCHECK(wal_buffered_ + size <= wal_buffer_bytes_);
    wal_buffered_ += size;
    wal_flush_req_.UpdateAttribute(&WalFlushReq::wal_buffered_, wal_buffered_);
  }

  /// Iterate over current TX entries
  void IterateCurrentTxWALs(uint64_t first_wal,
                            std::function<void(const WalEntry& entry)> callback);

  TXID GetSysTxWrittern() const {
    return sys_tx_writtern_;
  }

  void UpdateSysTxToHarden(TXID sys_tx_id) {
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

  void PublishWalFlushReq(TXID start_ts) {
    WalFlushReq current(wal_buffered_, sys_tx_writtern_, start_ts);
    wal_flush_req_.Set(current);
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

  void PublishWalBufferedOffset() {
    wal_flush_req_.UpdateAttribute(&WalFlushReq::wal_buffered_, wal_buffered_);
  }

  /// Calculate the continuous free space left in the wal ring buffer. Return
  /// size of the contiguous free space.
  uint32_t WalContiguousFreeSpace();

  void WriteWalCarriageReturn();
};

} // namespace leanstore::cr