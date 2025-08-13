#include "leanstore/concurrency/logging.hpp"

#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/concurrency/wal_entry.hpp"
#include "leanstore/exceptions.hpp"
#include "leanstore/utils/log.hpp"
#include "utils/coroutine/coro_env.hpp"
#include "utils/coroutine/coroutine.hpp"
#include "utils/to_json.hpp"

#include <cstring>

namespace leanstore::cr {

uint32_t Logging::WalContiguousFreeSpace() {
  const auto flushed = wal_flushed_.load();
  if (flushed <= wal_buffered_) {
    return wal_buffer_bytes_ - wal_buffered_;
  }
  return flushed - wal_buffered_;
}

void Logging::ReserveContiguousBuffer(uint32_t bytes_required) {
  // Spin until there is enough space. The wal ring buffer space is reclaimed
  // when the group commit thread commits the written wal entries.
  while (true) {
    const auto flushed = wal_flushed_.load();
    if (flushed <= wal_buffered_) {
      // carraige return, consume the last bytes from wal_buffered_ to the end
      if (wal_buffer_bytes_ - wal_buffered_ < bytes_required) {
        WriteWalCarriageReturn();
#ifdef ENABLE_COROUTINE
        CoroEnv::CurCoro()->Yield(CoroState::kWaitingIo);
#endif
        continue;
      }
      // Have enough space from wal_buffered_ to the end
      return;
    }

    if (flushed - wal_buffered_ < bytes_required) {
      // wait for group commit thread to commit the written wal entries
#ifdef ENABLE_COROUTINE
      CoroEnv::CurCoro()->Yield(CoroState::kWaitingIo);
#endif
      continue;
    }
    return;
  }
}

void Logging::WriteWalTxAbort() {
  // Reserve space
  auto size = sizeof(WalTxAbort);
  ReserveContiguousBuffer(size);

  // Initialize a WalTxAbort
  auto* data = wal_buffer_ + wal_buffered_;
  std::memset(data, 0, size);
  auto* entry [[maybe_unused]] = new (data) WalTxAbort(size);

  // Submit the WalTxAbort to group committer
  wal_buffered_ += size;
  PublishWalFlushReq();

  LEAN_DLOG("WriteWalTxAbort, workerId={}, startTs={}, walJson={}", CoroEnv::CurTxMgr().worker_id_,
            CoroEnv::CurTxMgr().ActiveTx().start_ts_, utils::ToJsonString(entry));
}

void Logging::WriteWalTxFinish() {
  // Reserve space
  auto size = sizeof(WalTxFinish);
  ReserveContiguousBuffer(size);

  // Initialize a WalTxFinish
  auto* data = wal_buffer_ + wal_buffered_;
  std::memset(data, 0, size);
  auto* entry [[maybe_unused]] = new (data) WalTxFinish(CoroEnv::CurTxMgr().ActiveTx().start_ts_);

  // Submit the WalTxAbort to group committer
  wal_buffered_ += size;
  PublishWalFlushReq();

  LEAN_DLOG("WriteWalTxFinish, workerId={}, startTs={}, walJson={}", CoroEnv::CurTxMgr().worker_id_,
            CoroEnv::CurTxMgr().ActiveTx().start_ts_, utils::ToJsonString(entry));
}

void Logging::WriteWalCarriageReturn() {
  LEAN_DCHECK(wal_flushed_ <= wal_buffered_,
              "CarriageReturn should only used for the last bytes in the wal buffer");
  auto entry_size = wal_buffer_bytes_ - wal_buffered_;
  auto* entry_ptr = wal_buffer_ + wal_buffered_;
  new (entry_ptr) WalCarriageReturn(entry_size);
  wal_buffered_ = 0;
  PublishWalBufferedOffset();
}

void Logging::SubmitWALEntryComplex(uint64_t total_size) {
  active_walentry_complex_->crc32_ = active_walentry_complex_->ComputeCRC32();
  wal_buffered_ += total_size;
  PublishWalFlushReq();

  LEAN_DLOG("SubmitWal, workerId={}, startTs={}, walJson={}", CoroEnv::CurTxMgr().worker_id_,
            CoroEnv::CurTxMgr().ActiveTx().start_ts_,
            utils::ToJsonString(active_walentry_complex_));
}

void Logging::PublishWalBufferedOffset() {
  wal_flush_req_.UpdateAttribute(&WalFlushReq::wal_buffered_, wal_buffered_);
}

void Logging::PublishWalFlushReq() {
  auto start_ts = CoroEnv::CurTxMgr().ActiveTx().start_ts_;
  WalFlushReq current(wal_buffered_, sys_tx_writtern_, start_ts);
  wal_flush_req_.Set(current);
}

// Called by worker, so concurrent writes on the buffer
void Logging::IterateCurrentTxWALs(std::function<void(const WalEntry& entry)> callback) {
  uint64_t cursor = tx_wal_begin_;
  while (cursor != wal_buffered_) {
    const WalEntry& entry = *reinterpret_cast<WalEntry*>(wal_buffer_ + cursor);
    DEBUG_BLOCK() {
      if (entry.type_ == WalEntry::Type::kComplex) {
        reinterpret_cast<const WalEntryComplex*>(&entry)->CheckCRC();
      }
    }

    if (entry.type_ == WalEntry::Type::kCarriageReturn) {
      cursor = 0;
    } else {
      callback(entry);
      cursor += WalEntry::Size(&entry);
    }
  }
}

} // namespace leanstore::cr
