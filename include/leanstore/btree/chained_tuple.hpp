#pragma once

#include "coroutine/coro_env.hpp"
#include "coroutine/mvcc_manager.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/core/btree_iter_mut.hpp"
#include "leanstore/common/portable.h"
#include "leanstore/common/types.h"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/cpp/base/constants.hpp"
#include "tuple.hpp"

namespace leanstore {

/// History versions of chained tuple are stored in the history tree of the
/// current worker thread.
/// Chained: only scheduled gc.
class PACKED ChainedTuple : public Tuple {
public:
  uint16_t total_updates_ = 0;

  uint16_t oldest_tx_ = 0;

  uint8_t is_tombstone_ = 1;

  // latest version in-place
  uint8_t payload_[];

public:
  /// Construct a ChainedTuple, copy the value to its payload
  ///
  /// NOTE: Payload space should be allocated in advance. This constructor is
  /// usually called by a placmenet new operator.
  ChainedTuple(lean_wid_t worker_id, lean_txid_t tx_id, Slice val)
      : Tuple(TupleFormat::kChained, worker_id, tx_id),
        is_tombstone_(false) {
    std::memcpy(payload_, val.data(), val.size());
  }

  ChainedTuple(lean_wid_t worker_id, lean_txid_t tx_id, lean_cmdid_t command_id, Slice val)
      : Tuple(TupleFormat::kChained, worker_id, tx_id, command_id),
        is_tombstone_(false) {
    std::memcpy(payload_, val.data(), val.size());
  }

  /// Construct a ChainedTuple from an existing FatTuple, the new ChainedTuple
  /// may share the same space with the input FatTuple, so std::memmove is
  /// used to handle the overlap bytes.
  ///
  /// NOTE: This constructor is usually called by a placmenet new operator on
  /// the address of the FatTuple
  ChainedTuple(FatTuple& old_fat_tuple)
      : Tuple(TupleFormat::kChained, old_fat_tuple.worker_id_, old_fat_tuple.tx_id_,
              old_fat_tuple.cmd_id_),
        is_tombstone_(false) {
    std::memmove(payload_, old_fat_tuple.payload_, old_fat_tuple.val_size_);
  }

public:
  inline Slice GetValue(size_t size) const {
    return Slice(payload_, size);
  }

  std::tuple<OpCode, uint16_t> GetVisibleTuple(Slice payload, ValCallback callback) const;

  void UpdateStats() {
    if (CoroEnv::CurTxMgr().cc_.VisibleForAll(tx_id_) ||
        oldest_tx_ !=
            static_cast<uint16_t>(
                CoroEnv::CurTxMgr().store_->GetMvccManager().GlobalWmkInfo().oldest_active_tx_ &
                0xFFFF)) {
      oldest_tx_ = 0;
      total_updates_ = 0;
      return;
    }
    total_updates_++;
  }

  bool ShouldConvertToFatTuple() {
    auto& tx_mgr = CoroEnv::CurTxMgr();
    auto* store = tx_mgr.store_;

    bool command_valid = cmd_id_ != kCmdInvalid;
    bool has_long_running_olap = store->GetMvccManager().GlobalWmkInfo().HasActiveLongRunningTx();
    bool frequently_updated = total_updates_ > store->store_option_->worker_threads_;
    bool recent_updated_by_others =
        worker_id_ != tx_mgr.worker_id_ || tx_id_ != tx_mgr.ActiveTx().start_ts_;
    return command_valid && has_long_running_olap && recent_updated_by_others && frequently_updated;
  }

  void Update(BTreeIterMut* x_iter, Slice key, MutValCallback update_call_back,
              UpdateDesc& update_desc);

public:
  inline static const ChainedTuple* From(const uint8_t* buffer) {
    return reinterpret_cast<const ChainedTuple*>(buffer);
  }

  inline static ChainedTuple* From(uint8_t* buffer) {
    return reinterpret_cast<ChainedTuple*>(buffer);
  }
};

} // namespace leanstore