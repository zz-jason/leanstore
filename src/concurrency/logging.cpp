#include "leanstore/concurrency/logging.hpp"

#include "coroutine/coro_env.hpp"
#include "coroutine/coro_io.hpp"
#include "leanstore/common/wal_record.h"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/cpp/wal/wal_traits.hpp"
#include "leanstore/utils/misc.hpp"
#include "wal/wal_builder.hpp"

#include <cassert>
#include <cstring>

namespace leanstore {

bool Logging::CoroFlush() {
  auto flushed = wal_flushed_.load();
  auto buffered = wal_buffered_;

  if (buffered == flushed) {
    return false; // nothing to flush
  }

  if (flushed < buffered) {
    CoroFlushAndYield(flushed, buffered, wal_size_);
    wal_size_ += buffered - flushed;
  } else {
    CoroFlushAndYield(flushed, wal_buffer_bytes_, wal_size_);
    wal_size_ += (wal_buffer_bytes_ - flushed);

    CoroFlushAndYield(0, buffered, wal_size_);
    wal_size_ += buffered;
  }

  wal_flushed_.store(buffered, std::memory_order_release);
  return true;
}

void Logging::CoroFlushAndYield(uint64_t lower, uint64_t upper, uint64_t offset) {
  auto lower_aligned = utils::AlignDown(lower, kAligment);
  auto upper_aligned = utils::AlignUp(upper, kAligment);
  auto* buf_aligned = wal_buffer_ + lower_aligned;
  auto count_aligned = upper_aligned - lower_aligned;
  auto offset_aligned = utils::AlignDown(offset, kAligment);

  CoroWrite(wal_fd_, buf_aligned, count_aligned, offset_aligned);
}

uint32_t Logging::WalContiguousFreeSpace() {
  const auto flushed = wal_flushed_.load();
  if (flushed <= wal_buffered_) {
    return wal_buffer_bytes_ - wal_buffered_;
  }
  return flushed - wal_buffered_;
}

uint8_t* Logging::ReserveWalBuffer(uint32_t bytes_required, bool check_space) {
  if (!check_space) {
    return wal_buffer_ + wal_buffered_;
  }

  static constexpr auto kCarriageReturnSize = sizeof(lean_wal_carriage_return);
  // Spin until there is enough space. The wal ring buffer space is reclaimed
  // when the group commit thread commits the written wal entries.
  while (true) {
    const auto flushed = wal_flushed_.load();
    const auto buffered = wal_buffered_;

    if (flushed <= buffered) {
      // Not enough space till the end:
      // 1. write a carriage return
      // 2. yield current coroutine to wait for log flush to reclaim space
      if (buffered + bytes_required + kCarriageReturnSize > wal_buffer_bytes_) {
        WriteWalCarriageReturn();
#ifdef ENABLE_COROUTINE
        CoroEnv::CurCoro()->Yield(CoroState::kWaitingIo);
#endif
        continue;
      }

      return wal_buffer_ + buffered;
    }

    // Not enough space between wal_buffered_ and wal_flushed_:
    // yield current coroutine to wait for log flush to reclaim space
    if (buffered + bytes_required > flushed) {
#ifdef ENABLE_COROUTINE
      CoroEnv::CurCoro()->Yield(CoroState::kWaitingIo);
#endif
      continue;
    }

    return wal_buffer_ + buffered;
  }
}

void Logging::WriteWalCarriageReturn() {
  LEAN_DCHECK(wal_flushed_ <= wal_buffered_,
              "CarriageReturn should only used for the last bytes in the wal buffer");
  auto total_size = wal_buffer_bytes_ - wal_buffered_;
  assert(total_size >= sizeof(lean_wal_carriage_return) &&
         "CarriageReturn size exceeds the remaining buffer size");

  // Build and submit the carriage return wal record
  WalBuilder<lean_wal_carriage_return>(0, total_size - sizeof(lean_wal_carriage_return)).Submit();
}

// Called by worker, so concurrent writes on the buffer
void Logging::ForeachWalOfCurrentTx(uint64_t first_wal,
                                    std::function<void(const lean_wal_record* entry)> callback) {
  uint64_t cursor = first_wal;
  while (cursor != wal_buffered_) {
    const auto* entry = reinterpret_cast<const lean_wal_record*>(wal_buffer_ + cursor);
    if (entry->type_ == LEAN_WAL_TYPE_CARRIAGE_RETURN) {
      cursor = 0;
      continue;
    }

    if (IsMvccBTreeWalRecordType(entry->type_)) {
      callback(entry);
    }

    cursor += entry->size_;
  }
}

} // namespace leanstore
