#include "leanstore/concurrency/logging.hpp"

#include "leanstore/common/wal_format.h"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/concurrency/wal_entry.hpp"
#include "leanstore/exceptions.hpp"
#include "leanstore/utils/log.hpp"
#include "utils/coroutine/coro_env.hpp"
#include "utils/wal/wal_builder.hpp"

#include <cassert>
#include <cstring>

namespace leanstore::cr {

uint32_t Logging::WalContiguousFreeSpace() {
  const auto flushed = wal_flushed_.load();
  if (flushed <= wal_buffered_) {
    return wal_buffer_bytes_ - wal_buffered_;
  }
  return flushed - wal_buffered_;
}

uint8_t* Logging::ReserveWalBuffer(uint32_t bytes_required) {
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
      return wal_buffer_ + wal_buffered_;
    }

    if (flushed - wal_buffered_ < bytes_required) {
      // wait for group commit thread to commit the written wal entries
#ifdef ENABLE_COROUTINE
      CoroEnv::CurCoro()->Yield(CoroState::kWaitingIo);
#endif
      continue;
    }
    return wal_buffer_ + wal_buffered_;
  }
}

void Logging::WriteWalCarriageReturn() {
  LEAN_DCHECK(wal_flushed_ <= wal_buffered_,
              "CarriageReturn should only used for the last bytes in the wal buffer");
  auto total_size = wal_buffer_bytes_ - wal_buffered_;
  assert(total_size >= sizeof(lean_wal_carriage_return) &&
         "CarriageReturn size exceeds the remaining buffer size");

  // Build and submit the carriage return wal record
  WalBuilder<lean_wal_carriage_return>(total_size - sizeof(lean_wal_carriage_return)).Submit();
}

// Called by worker, so concurrent writes on the buffer
void Logging::IterateCurrentTxWALs(uint64_t first_wal,
                                   std::function<void(const WalEntry& entry)> callback) {
  uint64_t cursor = first_wal;
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
