#pragma once

#include "leanstore/common/portable.h"
#include "leanstore/common/wal_record.h"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/sync/optimistic_guarded.hpp"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <functional>
#include <string_view>

#include <fcntl.h>

namespace leanstore {

/// Used to sync wal flush request between group committer and worker.
struct WalFlushReq {
  /// Used for optimistic locking.
  uint64_t version_ = 0;

  /// The offset in the wal ring buffer.
  uint64_t wal_buffered_ = 0;

  /// The maximum system transasction ID written by the worker.
  /// NOTE: can only be updated when all the WAL entries belonging to the system transaction are
  /// written to the wal ring buffer.
  lean_txid_t buffered_sys_tx_ = 0;

  /// ID of the current transaction.
  /// NOTE: can only be updated when all the WAL entries belonging to the user transaction are
  /// written to the wal ring buffer.
  lean_txid_t curr_tx_id_ = 0;

  WalFlushReq(uint64_t wal_buffered = 0, uint64_t sys_tx_writtern = 0, lean_txid_t curr_tx_id = 0)
      : version_(0),
        wal_buffered_(wal_buffered),
        buffered_sys_tx_(sys_tx_writtern),
        curr_tx_id_(curr_tx_id) {
  }
};

/// Helps to transaction concurrenct control and write-ahead logging.
class Logging {
public:
  /// Logical sequence number, i.e., the unique ID of each WAL.
  lean_lid_t lsn_ = 0;

  OptimisticGuarded<WalFlushReq> wal_flush_req_;

  /// The maximum writtern system transaction ID in the worker.
  lean_txid_t buffered_sys_tx_ = 0;

  std::atomic<lean_txid_t> last_hardened_sys_tx_ = 0;

  std::atomic<lean_txid_t> last_hardened_usr_tx_ = 0;

  /// File descriptor for the write-ahead log.
  int32_t wal_fd_ = -1;

  /// Start offset of the next wal record.
  uint64_t wal_size_ = 0;

  /// The size of the wal ring buffer.
  const uint64_t wal_buffer_bytes_;

  /// Written offset of the wal ring buffer.
  uint64_t wal_buffered_ = 0;

  /// Flushed offset in the wal ring buffer. The wal ring buffer is firstly
  /// written by the worker thread then flushed to disk file by the group commit
  /// thread.
  std::atomic<uint64_t> wal_flushed_ = 0;

  /// The ring buffer of the current worker thread. All the wal entries of the
  /// current worker are writtern to this ring buffer firstly, then flushed to
  /// disk by the group commit thread.
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

    DeinitWalFd();
  }

  lean_lid_t GetLsn() {
    return lsn_;
  }

  uint8_t* ReserveWalBuffer(uint32_t requested_size, bool check_space = true);

  void AdvanceWalBuffer(uint32_t size) {
    LEAN_DCHECK(wal_buffered_ + size <= wal_buffer_bytes_);
    wal_buffered_ = (wal_buffered_ + size) % wal_buffer_bytes_;
    wal_flush_req_.UpdateAttribute(&WalFlushReq::wal_buffered_, wal_buffered_);
    lsn_ += size;
  }

  /// Iterate over all the wal records of the current transaction and apply the
  /// given callback function.
  ///
  /// TODO: For now, only wal records in the ring buffer are supported. To
  ///       support larger transactions, we need to read wal records from the
  ///       wal file.
  void ForeachWalOfCurrentTx(uint64_t first_wal,
                             std::function<void(const lean_wal_record* entry)> callback);

  lean_txid_t GetBufferedSysTx() const {
    return buffered_sys_tx_;
  }

  void UpdateBufferedSysTx(lean_txid_t sys_tx_id) {
    buffered_sys_tx_ = std::max(buffered_sys_tx_, sys_tx_id);
  }

  lean_txid_t GetLastHardenedSysTx() const {
    return last_hardened_sys_tx_.load(std::memory_order_acquire);
  }

  void SetLastHardenedSysTx(lean_txid_t sys_tx_id) {
    last_hardened_sys_tx_.store(sys_tx_id, std::memory_order_release);
  }

  lean_txid_t GetLastHardenedUsrTx() const {
    return last_hardened_usr_tx_.load(std::memory_order_acquire);
  }

  void SetLastHardenedUsrTx(lean_txid_t usr_tx_id) {
    last_hardened_usr_tx_.store(usr_tx_id, std::memory_order_release);
  }

  void InitWalFd(std::string_view file_path) {
    wal_fd_ = open(file_path.data(), kFlags, kFileMode);
    if (wal_fd_ < 0) {
      Log::Fatal("Failed to init wal, file_path={}, error={}", file_path, strerror(errno));
    }
    Log::Info("Init wal succeed, file_path={}, fd={}", file_path, wal_fd_);
  }

  /// Flush the wal ring buffer to the wal file, called by the logging
  /// coroutine. Synchronization note:
  ///
  /// 1. wal_flushed_, wal_buffered_ can be updated by user coros when the
  ///    logging coro is suspended during CoroFlushAndYield(). We have to copy
  ///    their values at the begining of CoroFlush() to avoid inconsistency.
  ///
  /// 2. wal_size_ is only updated by the logging coro, so no need to worry
  ///    about its inconsistency.
  bool CoroFlush();

  void PublishWalFlushReq(lean_txid_t start_ts) {
    WalFlushReq current(wal_buffered_, buffered_sys_tx_, start_ts);
    wal_flush_req_.Set(current);
  }

private:
  static constexpr auto kFlags = O_DIRECT | O_RDWR | O_CREAT | O_TRUNC;
  static constexpr auto kFileMode = 0666;
  static constexpr auto kAligment = 4096u;

  /// Prepare wal buffer for writing, submit the IO request to the async IO task queue.
  /// The current logging coro is yielded until the IO is complete.
  void CoroFlushAndYield(uint64_t lower, uint64_t upper, uint64_t offset);

  /// Publish the wal buffered offset to the wal flush request.
  void PublishWalBufferedOffset() {
    wal_flush_req_.UpdateAttribute(&WalFlushReq::wal_buffered_, wal_buffered_);
  }

  /// Calculate the continuous free space left in the wal ring buffer. Return
  /// size of the contiguous free space.
  uint32_t WalContiguousFreeSpace();

  /// Write a carriage return wal record to the end of the wal ring buffer.
  void WriteWalCarriageReturn();

  /// Deinitialize the wal file descriptor. It removes the extra allocated
  /// space, truncates the wal file to the actual size, and close the wal file
  /// descriptor if opened.
  void DeinitWalFd() {
    if (wal_fd_ >= 0) {
      auto ret = ftruncate(wal_fd_, wal_size_);
      if (ret < 0) {
        Log::Fatal("Failed to truncate wal file, wal_fd_={}, error={}", wal_fd_, strerror(errno));
      }
      close(wal_fd_);
      wal_fd_ = -1;
    }
  }
};

} // namespace leanstore