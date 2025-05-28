#pragma once

#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/core/pessimistic_exclusive_iterator.hpp"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/concurrency/worker_context.hpp"
#include "leanstore/units.hpp"
#include "tuple.hpp"

namespace leanstore::storage::btree {

/// History versions of chained tuple are stored in the history tree of the
/// current worker thread.
/// Chained: only scheduled gc.
class __attribute__((packed)) ChainedTuple : public Tuple {
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
  ChainedTuple(WORKERID worker_id, TXID tx_id, Slice val)
      : Tuple(TupleFormat::kChained, worker_id, tx_id),
        is_tombstone_(false) {
    std::memcpy(payload_, val.data(), val.size());
  }

  ChainedTuple(WORKERID worker_id, TXID tx_id, COMMANDID command_id, Slice val)
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
              old_fat_tuple.command_id_),
        is_tombstone_(false) {
    std::memmove(payload_, old_fat_tuple.payload_, old_fat_tuple.val_size_);
  }

public:
  inline Slice GetValue(size_t size) const {
    return Slice(payload_, size);
  }

  std::tuple<OpCode, uint16_t> GetVisibleTuple(Slice payload, ValCallback callback) const;

  void UpdateStats() {
    if (cr::WorkerContext::My().cc_.VisibleForAll(tx_id_) ||
        oldest_tx_ !=
            static_cast<uint16_t>(
                cr::WorkerContext::My().store_->crmanager_->global_wmk_info_.oldest_active_tx_ &
                0xFFFF)) {
      oldest_tx_ = 0;
      total_updates_ = 0;
      return;
    }
    total_updates_++;
  }

  bool ShouldConvertToFatTuple() {
    bool command_valid = command_id_ != kInvalidCommandid;
    bool has_long_running_olap =
        cr::WorkerContext::My().store_->crmanager_->global_wmk_info_.HasActiveLongRunningTx();
    bool frequently_updated =
        total_updates_ > cr::WorkerContext::My().store_->store_option_->worker_threads_;
    bool recent_updated_by_others =
        worker_id_ != cr::WorkerContext::My().worker_id_ || tx_id_ != cr::ActiveTx().start_ts_;
    return command_valid && has_long_running_olap && recent_updated_by_others && frequently_updated;
  }

  void Update(PessimisticExclusiveIterator& x_iter, Slice key, MutValCallback update_call_back,
              UpdateDesc& update_desc);

public:
  inline static const ChainedTuple* From(const uint8_t* buffer) {
    return reinterpret_cast<const ChainedTuple*>(buffer);
  }

  inline static ChainedTuple* From(uint8_t* buffer) {
    return reinterpret_cast<ChainedTuple*>(buffer);
  }
};

} // namespace leanstore::storage::btree