#pragma once

#include "leanstore/concurrency/logging.hpp"
#include "leanstore/concurrency/wal_entry.hpp"
#include "leanstore/units.hpp"
#include "utils/coroutine/coro_env.hpp"
#include "utils/to_json.hpp"

namespace leanstore {

template <typename T>
class WalBuilder {
public:
  WalBuilder(uint64_t payload_size = 0) {
    wal_size_ = WalBuilder<T>::WalSize(payload_size);
    auto* wal_buf = CoroEnv::CurLogging().ReserveWalBuffer(wal_size_);
    wal_ = reinterpret_cast<cr::WalEntryComplex*>(wal_buf);
  }

  WalBuilder& InitHeader(LID prev_lsn, WORKERID worker_id, TXID txid, LID psn, PID page_id,
                         TREEID tree_id) {
    new (wal_) cr::WalEntryComplex(CoroEnv::CurLogging().ReserveLsn(), prev_lsn, wal_size_,
                                   worker_id, txid, psn, page_id, tree_id);
    return *this;
  }

  template <typename... Args>
  WalBuilder& InitData(Args&&... args) {
    new (wal_->payload_) T(std::forward<Args>(args)...);
    return *this;
  }

  void Submit() {
    wal_->crc32_ = wal_->ComputeCRC32();
    CoroEnv::CurLogging().AdvanceWalBuffer(wal_->size_);
    LEAN_DLOG("SubmitWal: wal={}", utils::ToJsonString(wal_));
  }

private:
  static uint64_t WalSize(uint64_t payload_size) {
    payload_size = ((payload_size + 7) / 8) * 8; // align up to 8 bytes
    return sizeof(cr::WalEntryComplex) + sizeof(T) + payload_size;
  }

private:
  uint64_t wal_size_ = 0;
  cr::WalEntryComplex* wal_ = nullptr;
};

} // namespace leanstore