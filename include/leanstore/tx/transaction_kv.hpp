#pragma once

#include "leanstore/base/result.hpp"
#include "leanstore/btree/b_tree_generic.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/btree_iter_mut.hpp"
#include "leanstore/buffer/guarded_buffer_frame.hpp"
#include "leanstore/c/types.h"
#include "leanstore/c/wal_record.h"
#include "leanstore/kv_interface.hpp"
#include "leanstore/tx/chained_tuple.hpp"
#include "leanstore/tx/tuple.hpp"
#include "leanstore/tx/tx_manager.hpp"

#include <string>
#include <tuple>

/// Forward declarations
struct lean_wal_tx_insert;
struct lean_wal_tx_update;
struct lean_wal_tx_remove;

namespace leanstore {

/// forward declarations
class LeanStore;

// Assumptions made in this implementation:
// 1. We don't insert an already removed key
// 2. Secondary Versions contain delta
//
// Keep in mind that garbage collection may leave pages completely empty
// Missing points: FatTuple::remove, garbage leaves can escape from us
class TransactionKV : public BasicKV {
public:
  /// Graveyard to store removed tuples for long-running transactions.
  BasicKV* graveyard_;

  TransactionKV() {
    tree_type_ = BTreeType::kTransactionKV;
  }

  OpCode Lookup(Slice key, ValCallback val_callback) override;

  OpCode ScanAsc(Slice start_key, ScanCallback) override;

  OpCode ScanDesc(Slice start_key, ScanCallback) override;

  OpCode Insert(Slice key, Slice val) override;

  OpCode UpdatePartial(Slice key, MutValCallback update_call_back,
                       UpdateDesc& update_desc) override;

  OpCode Remove(Slice key) override;

  void Init(LeanStore* store, lean_treeid_t tree_id, lean_btree_config config, BasicKV* graveyard);

  SpaceCheckResult CheckSpaceUtilization(BufferFrame& bf) override;

  // This undo implementation works only for rollback and not for undo
  // operations during recovery
  void Undo(const lean_wal_record* record) override;

  void GarbageCollect(const uint8_t* entry_ptr, lean_wid_t version_worker_id,
                      lean_txid_t version_tx_id, bool called_before) override;

  void Unlock(const uint8_t* wal_entry_ptr) override;

  std::tuple<OpCode, uint16_t> GetVisibleTuple(Slice payload, ValCallback callback);

private:
  OpCode LookupOptimistic(Slice key, ValCallback val_callback);

  template <bool asc = true>
  OpCode Scan4ShortRunningTx(Slice key, ScanCallback callback);

  template <bool asc = true>
  OpCode Scan4LongRunningTx(Slice key, ScanCallback callback);

  static bool TriggerPageWiseGarbageCollection(GuardedBufferFrame<BTreeNode>& guarded_node) {
    return guarded_node->has_garbage_;
  }

  void InsertAfterRemove(BTreeIterMut* x_iter, Slice key, Slice val);

  void UndoLastInsert(const lean_wal_tx_insert* wal_insert);

  void UndoLastUpdate(const lean_wal_tx_update* wal_update);

  void UndoLastRemove(const lean_wal_tx_remove* wal_remove);

public:
  static Result<TransactionKV*> Create(LeanStore* store, const std::string& tree_name,
                                       lean_btree_config config, BasicKV* graveyard);

  static void InsertToNode(GuardedBufferFrame<BTreeNode>& guarded_node, Slice key, Slice val,
                           lean_wid_t worker_id, lean_txid_t tx_start_ts, int32_t& slot_id) {
    auto total_val_size = sizeof(ChainedTuple) + val.size();
    slot_id = guarded_node->InsertDoNotCopyPayload(key, total_val_size, slot_id);
    auto* tuple_addr = guarded_node->ValData(slot_id);
    new (tuple_addr) ChainedTuple(worker_id, tx_start_ts, val);
  }

  static uint64_t ConvertToFatTupleThreshold() {
    auto& store_option = CoroEnv::CurTxMgr().store_->store_option_;
    return store_option->worker_threads_ * store_option->max_concurrent_transaction_per_worker_;
  }

  /// Updates the value stored in FatTuple. The former newest version value is
  /// moved to the tail.
  /// @return false to fallback to chained mode
  static bool UpdateInFatTuple(BTreeIterMut* x_iter, Slice key, MutValCallback update_call_back,
                               UpdateDesc& update_desc);
};

} // namespace leanstore
