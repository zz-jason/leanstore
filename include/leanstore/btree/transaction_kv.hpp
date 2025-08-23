#pragma once

#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/chained_tuple.hpp"
#include "leanstore/btree/core/b_tree_generic.hpp"
#include "leanstore/btree/core/btree_iter_mut.hpp"
#include "leanstore/btree/tuple.hpp"
#include "leanstore/buffer-manager/guarded_buffer_frame.hpp"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/units.hpp"
#include "leanstore/utils/result.hpp"

#include <expected>
#include <string>
#include <tuple>

/// forward declarations
namespace leanstore {

class LeanStore;

} // namespace leanstore

/// forward declarations
namespace leanstore::storage::btree {

class WalTxInsert;
class WalTxUpdate;
class WalTxRemove;

} // namespace leanstore::storage::btree

namespace leanstore::storage::btree {

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

  void Init(leanstore::LeanStore* store, TREEID tree_id, BTreeConfig config, BasicKV* graveyard);

  SpaceCheckResult CheckSpaceUtilization(BufferFrame& bf) override;

  // This undo implementation works only for rollback and not for undo
  // operations during recovery
  void Undo(const uint8_t* wal_entry_ptr, const uint64_t) override;

  void GarbageCollect(const uint8_t* entry_ptr, WORKERID version_worker_id, TXID version_tx_id,
                      bool called_before) override;

  void Unlock(const uint8_t* wal_entry_ptr) override;

private:
  OpCode LookupOptimistic(Slice key, ValCallback val_callback);

  template <bool asc = true>
  OpCode scan4ShortRunningTx(Slice key, ScanCallback callback);

  template <bool asc = true>
  OpCode scan4LongRunningTx(Slice key, ScanCallback callback);

  inline static bool trigger_page_wise_garbage_collection(
      GuardedBufferFrame<BTreeNode>& guarded_node) {
    return guarded_node->has_garbage_;
  }

  std::tuple<OpCode, uint16_t> get_visible_tuple(Slice payload, ValCallback callback);

  void InsertAfterRemove(BTreeIterMut* x_iter, Slice key, Slice val);

  void undo_last_insert(const WalTxInsert* wal_insert);

  void undo_last_update(const WalTxUpdate* wal_update);

  void undo_last_remove(const WalTxRemove* wal_remove);

public:
  static Result<TransactionKV*> Create(leanstore::LeanStore* store, const std::string& tree_name,
                                       BTreeConfig config, BasicKV* graveyard);

  inline static void InsertToNode(GuardedBufferFrame<BTreeNode>& guarded_node, Slice key, Slice val,
                                  WORKERID worker_id, TXID tx_start_ts, int32_t& slot_id) {
    auto total_val_size = sizeof(ChainedTuple) + val.size();
    slot_id = guarded_node->InsertDoNotCopyPayload(key, total_val_size, slot_id);
    auto* tuple_addr = guarded_node->ValData(slot_id);
    new (tuple_addr) ChainedTuple(worker_id, tx_start_ts, val);
  }

  inline static uint64_t ConvertToFatTupleThreshold() {
    auto& store_option = CoroEnv::CurTxMgr().store_->store_option_;
    return store_option->worker_threads_ * store_option->max_concurrent_tx_per_worker_;
  }

  /// Updates the value stored in FatTuple. The former newest version value is
  /// moved to the tail.
  /// @return false to fallback to chained mode
  static bool UpdateInFatTuple(BTreeIterMut* x_iter, Slice key, MutValCallback update_call_back,
                               UpdateDesc& update_desc);
};

} // namespace leanstore::storage::btree
