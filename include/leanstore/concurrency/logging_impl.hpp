#include "leanstore/concurrency/logging.hpp"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/concurrency/wal_entry.hpp"
#include "leanstore/concurrency/wal_payload_handler.hpp"
#include "leanstore/units.hpp"
#include "leanstore/utils/defer.hpp"
#include "utils/coroutine/coro_env.hpp"

namespace leanstore::cr {

template <typename T, typename... Args>
WalPayloadHandler<T> Logging::ReserveWALEntryComplex(uint64_t payload_size, PID page_id, LID psn,
                                                     TREEID tree_id, Args&&... args) {
  // write transaction start on demand
  auto prev_lsn = prev_lsn_;
  if (!CoroEnv::CurTxMgr().ActiveTx().has_wrote_) {
    // no prevLsn for the first wal entry in a transaction
    prev_lsn = 0;
    CoroEnv::CurTxMgr().ActiveTx().has_wrote_ = true;
  }

  // update prev lsn in the end
  SCOPED_DEFER(prev_lsn_ = active_walentry_complex_->lsn_);

  auto entry_lsn = lsn_clock_++;
  auto* entry_ptr = wal_buffer_ + wal_buffered_;
  auto entry_size = sizeof(WalEntryComplex) + payload_size;
  ReserveContiguousBuffer(entry_size);

  auto worker_id = CoroEnv::CurTxMgr().worker_id_;
  auto start_ts = CoroEnv::CurTxMgr().ActiveTx().start_ts_;

  active_walentry_complex_ = new (entry_ptr)
      WalEntryComplex(entry_lsn, prev_lsn, entry_size, worker_id, start_ts, psn, page_id, tree_id);

  auto* payload_ptr = active_walentry_complex_->payload_;
  auto wal_payload = new (payload_ptr) T(std::forward<Args>(args)...);
  return {wal_payload, entry_size};
}

} // namespace leanstore::cr