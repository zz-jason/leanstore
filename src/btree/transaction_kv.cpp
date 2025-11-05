#include "leanstore/btree/transaction_kv.hpp"

#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/chained_tuple.hpp"
#include "leanstore/btree/core/b_tree_generic.hpp"
#include "leanstore/btree/core/btree_iter.hpp"
#include "leanstore/btree/tuple.hpp"
#include "leanstore/common/types.h"
#include "leanstore/common/wal_record.h"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/slice.hpp"
#include "leanstore/sync/hybrid_guard.hpp"
#include "leanstore/units.hpp"
#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/error.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/managed_thread.hpp"
#include "leanstore/utils/misc.hpp"
#include "leanstore/utils/result.hpp"
#include "utils/coroutine/mvcc_manager.hpp"
#include "utils/small_vector.hpp"
#include "wal/wal_builder.hpp"

#include <cstring>
#include <format>
#include <string_view>

namespace leanstore::storage::btree {

Result<TransactionKV*> TransactionKV::Create(leanstore::LeanStore* store,
                                             const std::string& tree_name, lean_btree_config config,
                                             BasicKV* graveyard) {
  auto [tree_ptr, tree_id] = store->tree_registry_->CreateTree(tree_name, [&]() {
    return std::unique_ptr<BufferManagedTree>(static_cast<BufferManagedTree*>(new TransactionKV()));
  });

  if (tree_ptr == nullptr) {
    return std::unexpected(utils::Error::General(std::format(
        "Failed to create TransactionKV, treeName has been taken, treeName={}", tree_name)));
  }

  auto* tree = DownCast<TransactionKV*>(tree_ptr);
  tree->Init(store, tree_id, std::move(config), graveyard);

  return tree;
}

void TransactionKV::Init(leanstore::LeanStore* store, lean_treeid_t tree_id,
                         lean_btree_config config, BasicKV* graveyard) {
  this->graveyard_ = graveyard;
  BasicKV::Init(store, tree_id, std::move(config));
}

OpCode TransactionKV::LookupOptimistic(Slice key, ValCallback val_callback) {
  JUMPMU_TRY() {
    GuardedBufferFrame<BTreeNode> guarded_leaf;
    FindLeafCanJump(key, guarded_leaf, LatchMode::kOptimisticOrJump);
    auto slot_id = guarded_leaf->LowerBound<true>(key);
    if (slot_id != -1) {
      auto [ret, versions_read] = GetVisibleTuple(guarded_leaf->Value(slot_id), val_callback);
      guarded_leaf.JumpIfModifiedByOthers();
      JUMPMU_RETURN ret;
    }

    guarded_leaf.JumpIfModifiedByOthers();
    JUMPMU_RETURN OpCode::kNotFound;
  }
  JUMPMU_CATCH() {
  }

  // lock optimistically failed, return kOther to retry
  return OpCode::kOther;
}

OpCode TransactionKV::Lookup(Slice key, ValCallback val_callback) {
  LEAN_DCHECK(CoroEnv::CurTxMgr().IsTxStarted(),
              "TxManager is not in a transaction, workerId={}, startTs={}",
              CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_);
  auto lookup_in_graveyard = [&]() {
    auto g_iter = graveyard_->NewBTreeIter();
    if (g_iter->SeekToEqual(key); !g_iter->Valid()) {
      return OpCode::kNotFound;
    }
    auto [ret, versions_read] = GetVisibleTuple(g_iter->Val(), val_callback);
    return ret;
  };

  auto optimistic_ret = LookupOptimistic(key, val_callback);
  if (optimistic_ret == OpCode::kOK) {
    return OpCode::kOK;
  }
  if (optimistic_ret == OpCode::kNotFound) {
    // In a lookup-after-remove(other worker) scenario, the tuple may be garbage
    // collected and moved to the graveyard, check the graveyard for the key.
    return CoroEnv::CurTxMgr().ActiveTx().IsLongRunning() ? lookup_in_graveyard()
                                                          : OpCode::kNotFound;
  }

  // lookup pessimistically
  auto iter = NewBTreeIter();
  if (iter->SeekToEqual(key); !iter->Valid()) {
    // In a lookup-after-remove(other worker) scenario, the tuple may be garbage
    // collected and moved to the graveyard, check the graveyard for the key.
    return CoroEnv::CurTxMgr().ActiveTx().IsLongRunning() ? lookup_in_graveyard()
                                                          : OpCode::kNotFound;
  }

  auto [ret, versions_read] = GetVisibleTuple(iter->Val(), val_callback);
  if (CoroEnv::CurTxMgr().ActiveTx().IsLongRunning() && ret == OpCode::kNotFound) {
    ret = lookup_in_graveyard();
  }
  return ret;
}

OpCode TransactionKV::UpdatePartial(Slice key, MutValCallback update_call_back,
                                    UpdateDesc& update_desc) {
  LEAN_DCHECK(CoroEnv::CurTxMgr().IsTxStarted());
  JUMPMU_TRY() {
    auto x_iter = NewBTreeIterMut();
    if (x_iter->SeekToEqual(key); !x_iter->Valid()) {
      // Conflict detected, the tuple to be updated by the long-running
      // transaction is removed by newer transactions, abort it.
      if (CoroEnv::CurTxMgr().ActiveTx().IsLongRunning() &&
          graveyard_->Lookup(key, [&](Slice) {}) == OpCode::kOK) {
        JUMPMU_RETURN OpCode::kAbortTx;
      }
      Log::Error("Update failed, key not found, key={}, txMode={}", key.ToString(),
                 ToString(CoroEnv::CurTxMgr().ActiveTx().tx_mode_));
      JUMPMU_RETURN OpCode::kNotFound;
    }

    // Record is found
    while (true) {
      auto mut_raw_val = x_iter->MutableVal();
      auto& tuple = *Tuple::From(mut_raw_val.Data());
      auto visible_for_me = CoroEnv::CurTxMgr().cc_.VisibleForMe(tuple.worker_id_, tuple.tx_id_);
      if (tuple.IsWriteLocked() || !visible_for_me) {
        // conflict detected, the tuple is write locked by other worker or not
        // visible for me
        JUMPMU_RETURN OpCode::kAbortTx;
      }

      // write lock the tuple
      tuple.WriteLock();
      SCOPED_DEFER({
        LEAN_DCHECK(!Tuple::From(mut_raw_val.Data())->IsWriteLocked(),
                    "Tuple should be write unlocked after update");
      });

      switch (tuple.format_) {
      case TupleFormat::kFat: {
        auto succeed = UpdateInFatTuple(x_iter.get(), key, update_call_back, update_desc);
        x_iter->UpdateContentionStats();
        Tuple::From(mut_raw_val.Data())->WriteUnlock();
        if (!succeed) {
          JUMPMU_CONTINUE;
        }
        JUMPMU_RETURN OpCode::kOK;
      }
      case TupleFormat::kChained: {
        auto& chained_tuple = *ChainedTuple::From(mut_raw_val.Data());
        if (chained_tuple.is_tombstone_) {
          chained_tuple.WriteUnlock();
          JUMPMU_RETURN OpCode::kNotFound;
        }

        chained_tuple.UpdateStats();

        // convert to fat tuple if it's frequently updated by me and other
        // workers
        if (store_->store_option_->enable_fat_tuple_ && chained_tuple.ShouldConvertToFatTuple()) {
          chained_tuple.total_updates_ = 0;
          auto succeed = Tuple::ToFat(x_iter.get());
          if (succeed) {
            x_iter->guarded_leaf_->has_garbage_ = true;
          }
          Tuple::From(mut_raw_val.Data())->WriteUnlock();
          JUMPMU_CONTINUE;
        }

        // update the chained tuple
        chained_tuple.Update(x_iter.get(), key, update_call_back, update_desc);
        JUMPMU_RETURN OpCode::kOK;
      }
      default: {
        Log::Error("Unhandled tuple format: {}", TupleFormatUtil::ToString(tuple.format_));
      }
      }
    }
  }
  JUMPMU_CATCH() {
  }
  return OpCode::kOther;
}

OpCode TransactionKV::Insert(Slice key, Slice val) {
  LEAN_DCHECK(CoroEnv::CurTxMgr().IsTxStarted());
  uint16_t payload_size = val.size() + sizeof(ChainedTuple);

  while (true) {
    auto x_iter = NewBTreeIterMut();
    auto ret = x_iter->SeekToInsert(key);

    if (ret == OpCode::kDuplicated) {
      auto mut_raw_val = x_iter->MutableVal();
      auto* chained_tuple = ChainedTuple::From(mut_raw_val.Data());
      auto last_worker_id = chained_tuple->worker_id_;
      auto last_tx_id = chained_tuple->tx_id_;
      auto is_write_locked = chained_tuple->IsWriteLocked();
      LEAN_DCHECK(!chained_tuple->write_locked_,
                  "Duplicate tuple should not be write locked, workerId={}, startTs={}, key={}, "
                  "tupleLastWriter={}, tupleLastStartTs={}, tupleWriteLocked={}",
                  CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_,
                  key.ToString(), last_worker_id, last_tx_id, is_write_locked);

      auto visible_for_me =
          CoroEnv::CurTxMgr().cc_.VisibleForMe(chained_tuple->worker_id_, chained_tuple->tx_id_);

      if (chained_tuple->is_tombstone_ && visible_for_me) {
        InsertAfterRemove(x_iter.get(), key, val);
        return OpCode::kOK;
      }

      // conflict on tuple not visible for me
      if (!visible_for_me) {
        auto last_worker_id = chained_tuple->worker_id_;
        auto last_tx_id = chained_tuple->tx_id_;
        auto is_write_locked = chained_tuple->IsWriteLocked();
        auto is_tombsone = chained_tuple->is_tombstone_;
        Log::Info("Insert conflicted, current transaction should be aborted, workerId={}, "
                  "startTs={}, key={}, tupleLastWriter={}, tupleLastTxId={}, "
                  "tupleIsWriteLocked={}, tupleIsRemoved={}, tupleVisibleForMe={}",
                  CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_,
                  ToString(key), last_worker_id, last_tx_id, is_write_locked, is_tombsone,
                  visible_for_me);
        return OpCode::kAbortTx;
      }

      // duplicated on tuple inserted by former committed transactions
      auto is_tombsone = chained_tuple->is_tombstone_;
      Log::Info("Insert duplicated, workerId={}, startTs={}, key={}, tupleLastWriter={}, "
                "tupleLastTxId={}, tupleIsWriteLocked={}, tupleIsRemoved={}, tupleVisibleForMe={}",
                CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_,
                key.ToString(), last_worker_id, last_tx_id, is_write_locked, is_tombsone,
                visible_for_me);
      return OpCode::kDuplicated;
    }

    if (!x_iter->HasEnoughSpaceFor(key.size(), payload_size)) {
      x_iter->SplitForKey(key);
      continue;
    }

    // WAL
    WalTxBuilder<lean_wal_tx_insert>(this->tree_id_, key.size() + val.size())
        .SetPageInfo(x_iter->guarded_leaf_.bf_)
        .SetPrevVersion(0, 0, kCmdInvalid)
        .BuildTxInsert(key, val)
        .Submit();

    CoroEnv::CurTxMgr().ActiveTx().has_wrote_ = true;

    // insert
    TransactionKV::InsertToNode(x_iter->guarded_leaf_, key, val, CoroEnv::CurTxMgr().worker_id_,
                                CoroEnv::CurTxMgr().ActiveTx().start_ts_, x_iter->slot_id_);
    return OpCode::kOK;
  }
}

std::tuple<OpCode, uint16_t> TransactionKV::GetVisibleTuple(Slice payload, ValCallback callback) {
  std::tuple<OpCode, uint16_t> ret;
  while (true) {
    JUMPMU_TRY() {
      const auto* const tuple = Tuple::From(payload.data());
      switch (tuple->format_) {
      case TupleFormat::kChained: {
        const auto* const chained_tuple = ChainedTuple::From(payload.data());
        ret = chained_tuple->GetVisibleTuple(payload, callback);
        JUMPMU_RETURN ret;
      }
      case TupleFormat::kFat: {
        const auto* const fat_tuple = FatTuple::From(payload.data());
        ret = fat_tuple->GetVisibleTuple(callback);
        JUMPMU_RETURN ret;
      }
      default: {
        Log::Fatal("Unhandled tuple format: {}", TupleFormatUtil::ToString(tuple->format_));
      }
      }
    }
    JUMPMU_CATCH() {
    }
  }
}

void TransactionKV::InsertAfterRemove(BTreeIterMut* x_iter, Slice key, Slice val) {
  auto mut_raw_val = x_iter->MutableVal();
  auto* chained_tuple = ChainedTuple::From(mut_raw_val.Data());
  auto last_worker_id = chained_tuple->worker_id_;
  auto last_tx_id = chained_tuple->tx_id_;
  auto last_command_id = chained_tuple->cmd_id_;
  auto is_write_locked [[maybe_unused]] = chained_tuple->IsWriteLocked();
  LEAN_DCHECK(chained_tuple->is_tombstone_,
              "Tuple should be removed before insert, workerId={}, "
              "startTs={}, key={}, tupleLastWriter={}, "
              "tupleLastStartTs={}, tupleWriteLocked={}",
              CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_,
              key.ToString(), last_worker_id, last_tx_id, is_write_locked);

  // create an insert version
  auto version_size = sizeof(InsertVersion) + val.size() + key.size();
  auto command_id =
      CoroEnv::CurTxMgr().cc_.PutVersion(tree_id_, false, version_size, [&](uint8_t* version_buf) {
        new (version_buf) InsertVersion(last_worker_id, last_tx_id, last_command_id, key, val);
      });

  // WAL
  WalTxBuilder<lean_wal_tx_insert>(this->tree_id_, key.size() + val.size())
      .SetPageInfo(x_iter->guarded_leaf_.bf_)
      .SetPrevVersion(chained_tuple->worker_id_, chained_tuple->tx_id_, chained_tuple->cmd_id_)
      .BuildTxInsert(key, val)
      .Submit();

  CoroEnv::CurTxMgr().ActiveTx().has_wrote_ = true;

  // store the old chained tuple update stats
  auto total_updates_copy = chained_tuple->total_updates_;
  auto oldest_tx_copy = chained_tuple->oldest_tx_;

  // make room for the new chained tuple
  auto chained_tuple_size = val.size() + sizeof(ChainedTuple);
  if (mut_raw_val.Size() < chained_tuple_size) {
    auto succeed [[maybe_unused]] = x_iter->ExtendPayload(chained_tuple_size);
    LEAN_DCHECK(succeed,
                "Failed to extend btree node slot to store the expanded "
                "chained tuple, workerId={}, startTs={}, key={}, "
                "curRawValSize={}, chainedTupleSize={}",
                CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_,
                key.ToString(), mut_raw_val.Size(), chained_tuple_size);

  } else if (mut_raw_val.Size() > chained_tuple_size) {
    x_iter->ShortenWithoutCompaction(chained_tuple_size);
  }

  // get the new value place and recreate a new chained tuple there
  auto new_mut_raw_val = x_iter->MutableVal();
  auto* new_chained_tuple = new (new_mut_raw_val.Data()) ChainedTuple(
      CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_, command_id, val);
  new_chained_tuple->total_updates_ = total_updates_copy;
  new_chained_tuple->oldest_tx_ = oldest_tx_copy;
  new_chained_tuple->UpdateStats();
}

OpCode TransactionKV::Remove(Slice key) {
  LEAN_DCHECK(CoroEnv::CurTxMgr().IsTxStarted());
  JUMPMU_TRY() {
    auto x_iter = NewBTreeIterMut();
    if (x_iter->SeekToEqual(key); !x_iter->Valid()) {
      // Conflict detected, the tuple to be removed by the long-running transaction is removed by
      // newer transactions, abort it.
      if (CoroEnv::CurTxMgr().ActiveTx().IsLongRunning() &&
          graveyard_->Lookup(key, [&](Slice) {}) == OpCode::kOK) {
        JUMPMU_RETURN OpCode::kAbortTx;
      }
      JUMPMU_RETURN OpCode::kNotFound;
    }

    auto mut_raw_val = x_iter->MutableVal();
    auto* tuple = Tuple::From(mut_raw_val.Data());

    // remove fat tuple is not supported yet
    if (tuple->format_ == TupleFormat::kFat) {
      Log::Error("Remove failed, fat tuple is not supported yet");
      JUMPMU_RETURN OpCode::kNotFound;
    }

    // remove the chained tuple
    auto& chained_tuple = *static_cast<ChainedTuple*>(tuple);
    auto last_worker_id = chained_tuple.worker_id_;
    auto last_tx_id = chained_tuple.tx_id_;
    if (chained_tuple.IsWriteLocked() ||
        !CoroEnv::CurTxMgr().cc_.VisibleForMe(last_worker_id, last_tx_id)) {
      Log::Info("Remove conflicted, current transaction should be aborted, workerId={}, "
                "startTs={}, key={}, tupleLastWriter={}, tupleLastStartTs={}, tupleVisibleForMe={}",
                CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_,
                key.ToString(), last_worker_id, last_tx_id,
                CoroEnv::CurTxMgr().cc_.VisibleForMe(last_worker_id, last_tx_id));
      JUMPMU_RETURN OpCode::kAbortTx;
    }

    if (chained_tuple.is_tombstone_) {
      JUMPMU_RETURN OpCode::kNotFound;
    }

    chained_tuple.WriteLock();
    SCOPED_DEFER({
      LEAN_DCHECK(!Tuple::From(mut_raw_val.Data())->IsWriteLocked(),
                  "Tuple should be write unlocked after remove");
    });

    // 1. move current (key, value) pair to the version storage
    DanglingPointer dangling_pointer(x_iter.get());
    auto val_size = x_iter->Val().size() - sizeof(ChainedTuple);
    auto val = chained_tuple.GetValue(val_size);
    auto version_size = sizeof(RemoveVersion) + val.size() + key.size();
    auto command_id =
        CoroEnv::CurTxMgr().cc_.PutVersion(tree_id_, true, version_size, [&](uint8_t* version_buf) {
          new (version_buf) RemoveVersion(chained_tuple.worker_id_, chained_tuple.tx_id_,
                                          chained_tuple.cmd_id_, key, val, dangling_pointer);
        });

    // 2. write wal
    WalTxBuilder<lean_wal_tx_remove>(this->tree_id_, key.size() + val.size())
        .SetPageInfo(x_iter->guarded_leaf_.bf_)
        .SetPrevVersion(chained_tuple.worker_id_, chained_tuple.tx_id_, chained_tuple.cmd_id_)
        .BuildTxRemove(key, val)
        .Submit();

    CoroEnv::CurTxMgr().ActiveTx().has_wrote_ = true;

    // 3. remove the tuple, leave a tombsone
    if (mut_raw_val.Size() > sizeof(ChainedTuple)) {
      x_iter->ShortenWithoutCompaction(sizeof(ChainedTuple));
    }
    chained_tuple.is_tombstone_ = true;
    chained_tuple.worker_id_ = CoroEnv::CurTxMgr().worker_id_;
    chained_tuple.tx_id_ = CoroEnv::CurTxMgr().ActiveTx().start_ts_;
    chained_tuple.cmd_id_ = command_id;

    chained_tuple.WriteUnlock();
    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode TransactionKV::ScanDesc(Slice start_key, ScanCallback callback) {
  LEAN_DCHECK(CoroEnv::CurTxMgr().IsTxStarted());
  if (CoroEnv::CurTxMgr().ActiveTx().IsLongRunning()) {
    TODOException();
    return OpCode::kAbortTx;
  }
  return scan4ShortRunningTx<false>(start_key, callback);
}

OpCode TransactionKV::ScanAsc(Slice start_key, ScanCallback callback) {
  LEAN_DCHECK(CoroEnv::CurTxMgr().IsTxStarted());
  if (CoroEnv::CurTxMgr().ActiveTx().IsLongRunning()) {
    return scan4LongRunningTx(start_key, callback);
  }
  return scan4ShortRunningTx<true>(start_key, callback);
}

void TransactionKV::Undo(const lean_wal_record* record) {
  switch (record->type_) {
  case LEAN_WAL_TYPE_TX_INSERT: {
    return undo_last_insert(reinterpret_cast<const lean_wal_tx_insert*>(record));
  }
  case LEAN_WAL_TYPE_TX_UPDATE: {
    return undo_last_update(reinterpret_cast<const lean_wal_tx_update*>(record));
  }
  case LEAN_WAL_TYPE_TX_REMOVE: {
    return undo_last_remove(reinterpret_cast<const lean_wal_tx_remove*>(record));
  }
  default: {
    LEAN_DCHECK(false, "Unknown wal record type: {}", (uint64_t)record->type_);
    Log::Error("Unknown wal record type: {}", (uint64_t)record->type_);
  }
  }
}

void TransactionKV::undo_last_insert(const lean_wal_tx_insert* wal_insert) {
  // Assuming no insert after remove
  auto key = Slice{lean_wal_tx_insert_get_key(wal_insert), wal_insert->key_size_};
  while (true) {
    JUMPMU_TRY() {
      auto x_iter = NewBTreeIterMut();
      x_iter->SeekToEqual(key);
      LEAN_DCHECK(x_iter->Valid(),
                  "Cannot find the inserted key in btree, worker_id={}, start_ts={}, key={}",
                  CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_,
                  key.ToString());
      // TODO(jian.z): write compensation wal entry
      if (wal_insert->prev_cmd_id_ != kCmdInvalid) {
        // only remove the inserted value and mark the chained tuple as removed
        auto mut_raw_val = x_iter->MutableVal();
        auto* chained_tuple = ChainedTuple::From(mut_raw_val.Data());

        if (mut_raw_val.Size() > sizeof(ChainedTuple)) {
          x_iter->ShortenWithoutCompaction(sizeof(ChainedTuple));
        }

        // mark as removed
        chained_tuple->is_tombstone_ = true;
        chained_tuple->worker_id_ = wal_insert->prev_wid_;
        chained_tuple->tx_id_ = wal_insert->prev_txid_;
        chained_tuple->cmd_id_ = wal_insert->prev_cmd_id_;
      } else {
        // It's the first insert of of the value, remove the whole key-value from the btree.
        auto ret = x_iter->RemoveCurrent();
        if (ret != OpCode::kOK) {
          Log::Error("Undo last insert failed, failed to remove current key, "
                     "workerId={}, startTs={}, key={}, ret={}",
                     CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_,
                     key.ToString(), ToString(ret));
        }
      }

      x_iter->TryMergeIfNeeded();
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
      Log::Warn("Undo insert failed, workerId={}, startTs={}", CoroEnv::CurTxMgr().worker_id_,
                CoroEnv::CurTxMgr().ActiveTx().start_ts_);
    }
  }
}

void TransactionKV::undo_last_update(const lean_wal_tx_update* wal_update) {
  auto key = Slice{lean_wal_tx_update_get_key(wal_update), wal_update->key_size_};
  while (true) {
    JUMPMU_TRY() {
      auto x_iter = NewBTreeIterMut();
      x_iter->SeekToEqual(key);
      LEAN_DCHECK(x_iter->Valid(),
                  "Cannot find the updated key in btree, workerId={}, "
                  "startTs={}, key={}",
                  CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_,
                  key.ToString());

      auto mut_raw_val = x_iter->MutableVal();
      auto& tuple = *Tuple::From(mut_raw_val.Data());
      LEAN_DCHECK(!tuple.IsWriteLocked(), "Tuple is write locked, workerId={}, startTs={}, key={}",
                  CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_,
                  key.ToString());
      if (tuple.format_ == TupleFormat::kFat) {
        FatTuple::From(mut_raw_val.Data())->UndoLastUpdate();
      } else {
        auto& chained_tuple = *ChainedTuple::From(mut_raw_val.Data());
        chained_tuple.worker_id_ = wal_update->prev_wid_;
        chained_tuple.tx_id_ = wal_update->prev_txid_;
        chained_tuple.cmd_id_ ^= wal_update->xor_cmd_id_;

        // TODO: Uncomment this after implementing the xor-based update

        auto& update_desc =
            *reinterpret_cast<UpdateDesc*>(lean_wal_tx_update_get_update_desc(wal_update));
        auto* delta_ptr = reinterpret_cast<uint8_t*>(lean_wal_tx_update_get_delta(wal_update));
        auto delta_size = wal_update->delta_size_;

        // 1. copy the new value to buffer
        SmallBuffer256 tmp_buf(delta_size);
        auto* buff = tmp_buf.Data();
        std::memcpy(buff, delta_ptr, delta_size);

        // 2. calculate the old value based on xor result and old value
        BasicKV::XorToBuffer(update_desc, chained_tuple.payload_, buff);

        // 3. replace new value with old value
        BasicKV::CopyToValue(update_desc, buff, chained_tuple.payload_);
      }
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
      Log::Warn("Undo update failed, workerId={}, startTs={}", CoroEnv::CurTxMgr().worker_id_,
                CoroEnv::CurTxMgr().ActiveTx().start_ts_);
    }
  }
}

void TransactionKV::undo_last_remove(const lean_wal_tx_remove* wal_remove) {
  Slice removed_key{lean_wal_tx_remove_get_key(wal_remove), wal_remove->key_size_};
  while (true) {
    JUMPMU_TRY() {
      auto x_iter = NewBTreeIterMut();
      x_iter->SeekToEqual(removed_key);
      LEAN_DCHECK(x_iter->Valid(),
                  "Cannot find the tombstone of removed key, workerId={}, "
                  "startTs={}, removedKey={}",
                  CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_,
                  removed_key.ToString());

      // resize the current slot to store the removed tuple
      auto chained_tuple_size = wal_remove->val_size_ + sizeof(ChainedTuple);
      auto cur_raw_val = x_iter->Val();
      if (cur_raw_val.size() < chained_tuple_size) {
        auto succeed [[maybe_unused]] = x_iter->ExtendPayload(chained_tuple_size);
        LEAN_DCHECK(succeed,
                    "Failed to extend btree node slot to store the "
                    "recovered chained tuple, workerId={}, startTs={}, "
                    "removedKey={}, curRawValSize={}, chainedTupleSize={}",
                    CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_,
                    removed_key.ToString(), cur_raw_val.size(), chained_tuple_size);
      } else if (cur_raw_val.size() > chained_tuple_size) {
        x_iter->ShortenWithoutCompaction(chained_tuple_size);
      }

      auto cur_mut_raw_val = x_iter->MutableVal();
      Slice removed_val{lean_wal_tx_remove_get_val(wal_remove), wal_remove->val_size_};
      new (cur_mut_raw_val.Data()) ChainedTuple(wal_remove->prev_wid_, wal_remove->prev_txid_,
                                                wal_remove->prev_cmd_id_, removed_val);

      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
      Log::Warn("Undo remove failed, workerId={}, startTs={}", CoroEnv::CurTxMgr().worker_id_,
                CoroEnv::CurTxMgr().ActiveTx().start_ts_);
    }
  }
}

bool TransactionKV::UpdateInFatTuple(BTreeIterMut* x_iter, Slice key,
                                     MutValCallback update_call_back, UpdateDesc& update_desc) {
  while (true) {
    auto* fat_tuple = reinterpret_cast<FatTuple*>(x_iter->MutableVal().Data());
    LEAN_DCHECK(fat_tuple->IsWriteLocked(), "Tuple should be write locked");

    if (!fat_tuple->HasSpaceFor(update_desc)) {
      fat_tuple->GarbageCollection();
      if (fat_tuple->HasSpaceFor(update_desc)) {
        continue;
      }

      // Not enough space to store the fat tuple, convert to chained
      auto chained_tuple_size = fat_tuple->val_size_ + sizeof(ChainedTuple);
      LEAN_DCHECK(chained_tuple_size < x_iter->Val().length());
      fat_tuple->ConvertToChained(x_iter->btree_.tree_id_);
      x_iter->ShortenWithoutCompaction(chained_tuple_size);
      return false;
    }

    auto perform_update = [&]() {
      fat_tuple->Append(update_desc);
      fat_tuple->worker_id_ = CoroEnv::CurTxMgr().worker_id_;
      fat_tuple->tx_id_ = CoroEnv::CurTxMgr().ActiveTx().start_ts_;
      fat_tuple->cmd_id_ = CoroEnv::CurTxMgr().cmd_id_++;
      update_call_back(fat_tuple->GetMutableValue());
      LEAN_DCHECK(fat_tuple->payload_capacity_ >= fat_tuple->payload_size_);
    };

    if (!x_iter->btree_.config_.enable_wal_) {
      perform_update();
      return true;
    }

    auto size_of_desc_and_delta = update_desc.SizeWithDelta();
    auto prev_worker_id = fat_tuple->worker_id_;
    auto prev_tx_id = fat_tuple->tx_id_;
    auto prev_command_id = fat_tuple->cmd_id_;

    WalTxBuilder<lean_wal_tx_update> builder(x_iter->btree_.tree_id_,
                                             key.size() + size_of_desc_and_delta);
    builder.SetPageInfo(x_iter->guarded_leaf_.bf_)
        .SetPrevVersion(prev_worker_id, prev_tx_id, prev_command_id)
        .BuildTxUpdate(key, update_desc);
    auto* delta_ptr = reinterpret_cast<uint8_t*>(lean_wal_tx_update_get_delta(builder.GetWal()));

    // 1. copy old value to wal buffer
    BasicKV::CopyToBuffer(update_desc, fat_tuple->GetValPtr(), delta_ptr);

    // 2. update the value in-place
    perform_update();

    // 3. xor with the updated new value and store to wal buffer
    BasicKV::XorToBuffer(update_desc, fat_tuple->GetValPtr(), delta_ptr);

    builder.Submit();
    CoroEnv::CurTxMgr().ActiveTx().has_wrote_ = true;

    return true;
  }
}

SpaceCheckResult TransactionKV::CheckSpaceUtilization(BufferFrame& bf) {
  if (!store_->store_option_->enable_xmerge_) {
    return SpaceCheckResult::kNothing;
  }

  HybridGuard bf_guard(&bf.header_.latch_);
  bf_guard.ToOptimisticOrJump();
  if (bf.page_.btree_id_ != tree_id_) {
    leanstore::JumpContext::Jump();
  }

  GuardedBufferFrame<BTreeNode> guarded_node(store_->buffer_manager_.get(), std::move(bf_guard),
                                             &bf);
  if (!guarded_node->is_leaf_ || !trigger_page_wise_garbage_collection(guarded_node)) {
    return BTreeGeneric::CheckSpaceUtilization(bf);
  }

  guarded_node.ToExclusiveMayJump();
  lean_txid_t sys_tx_id = CoroEnv::CurStore()->MvccManager()->AllocSysTxTs();
  guarded_node.SyncSystemTxId(sys_tx_id);

  for (uint16_t i = 0; i < guarded_node->num_slots_; i++) {
    auto& tuple = *Tuple::From(guarded_node->ValData(i));
    if (tuple.format_ == TupleFormat::kFat) {
      auto& fat_tuple = *FatTuple::From(guarded_node->ValData(i));
      const uint32_t new_length = fat_tuple.val_size_ + sizeof(ChainedTuple);
      fat_tuple.ConvertToChained(tree_id_);
      LEAN_DCHECK(new_length < guarded_node->ValSize(i));
      guarded_node->ShortenPayload(i, new_length);
      LEAN_DCHECK(tuple.format_ == TupleFormat::kChained);
    }
  }
  guarded_node->has_garbage_ = false;
  guarded_node.unlock();

  const SpaceCheckResult result = BTreeGeneric::CheckSpaceUtilization(bf);
  if (result == SpaceCheckResult::kPickAnotherBf) {
    return SpaceCheckResult::kPickAnotherBf;
  }
  return SpaceCheckResult::kRestartSameBf;
}

// Only point-gc and for removed tuples
void TransactionKV::GarbageCollect(const uint8_t* version_data, lean_wid_t version_worker_id,
                                   lean_txid_t version_tx_id, bool called_before) {
  const auto& version = *RemoveVersion::From(version_data);

  // Delete tombstones caused by transactions below cc_.local_wmk_of_all_tx_.
  if (version_tx_id <= CoroEnv::CurTxMgr().cc_.local_wmk_of_all_tx_) {
    LEAN_DLOG("Delete tombstones caused by transactions below "
              "cc_.local_wmk_of_all_tx_, versionWorkerId={}, versionTxId={}",
              version_worker_id, version_tx_id);
    LEAN_DCHECK(version.dangling_pointer_.bf_ != nullptr);
    JUMPMU_TRY() {
      BTreeIterMut x_iter(*static_cast<BTreeGeneric*>(this), version.dangling_pointer_.bf_,
                          version.dangling_pointer_.latch_version_should_be_);
      auto& node = x_iter.guarded_leaf_;
      auto& chained_tuple [[maybe_unused]] =
          *ChainedTuple::From(node->ValData(version.dangling_pointer_.head_slot_));
      LEAN_DCHECK(chained_tuple.format_ == TupleFormat::kChained &&
                  !chained_tuple.IsWriteLocked() && chained_tuple.worker_id_ == version_worker_id &&
                  chained_tuple.tx_id_ == version_tx_id && chained_tuple.is_tombstone_);
      node->RemoveSlot(version.dangling_pointer_.head_slot_);
      x_iter.TryMergeIfNeeded();
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
      LEAN_DLOG("Delete tombstones caused by transactions below cc_.local_wmk_of_all_tx_ "
                "page has been modified since last delete");
    }
    return;
  }

  auto removed_key = version.RemovedKey();

  // Delete the removedKey from graveyard since no transaction needs it
  if (called_before) {
    LEAN_DLOG("Meet the removedKey again, delete it from graveyard, "
              "versionWorkerId={}, versionTxId={}, removedKey={}",
              version_worker_id, version_tx_id, removed_key.ToString());
    JUMPMU_TRY() {
      auto x_iter = graveyard_->NewBTreeIterMut();
      if (x_iter->SeekToEqual(removed_key); x_iter->Valid()) {
        auto ret [[maybe_unused]] = x_iter->RemoveCurrent();
        LEAN_DCHECK(ret == OpCode::kOK,
                    "Failed to delete the removedKey from graveyard, ret={}, "
                    "versionWorkerId={}, versionTxId={}, removedKey={}",
                    ToString(ret), version_worker_id, version_tx_id, removed_key.ToString());
      } else {
        Log::Fatal("Cannot find the removedKey in graveyard, "
                   "versionWorkerId={}, versionTxId={}, removedKey={}",
                   version_worker_id, version_tx_id, removed_key.ToString());
      }
    }
    JUMPMU_CATCH() {
    }
    return;
  }

  // Move the removedKey to graveyard, it's removed by short-running
  // transaction but still visible for long-running transactions
  //
  // TODO(jian.z): handle corner cases in insert-after-remove scenario
  JUMPMU_TRY() {
    auto x_iter = NewBTreeIterMut();
    if (x_iter->SeekToEqual(removed_key); !x_iter->Valid()) {
      Log::Fatal("Cannot find the removedKey in TransactionKV, should not "
                 "happen, versionWorkerId={}, versionTxId={}, removedKey={}",
                 version_worker_id, version_tx_id, removed_key.ToString());
      JUMPMU_RETURN;
    }

    MutableSlice mut_raw_val = x_iter->MutableVal();
    auto& tuple = *Tuple::From(mut_raw_val.Data());
    if (tuple.format_ == TupleFormat::kFat) {
      LEAN_DLOG("Skip moving removedKey to graveyard for FatTuple, "
                "versionWorkerId={}, versionTxId={}, removedKey={}",
                version_worker_id, version_tx_id, removed_key.ToString());
      JUMPMU_RETURN;
    }

    ChainedTuple& chained_tuple = *ChainedTuple::From(mut_raw_val.Data());
    if (chained_tuple.IsWriteLocked()) {
      Log::Fatal("The removedKey is write locked, should not happen, "
                 "versionWorkerId={}, versionTxId={}, removedKey={}",
                 version_worker_id, version_tx_id, removed_key.ToString());
      JUMPMU_RETURN;
    }

    if (chained_tuple.worker_id_ == version_worker_id && chained_tuple.tx_id_ == version_tx_id &&
        chained_tuple.is_tombstone_) {

      LEAN_DCHECK(chained_tuple.tx_id_ > CoroEnv::CurTxMgr().cc_.local_wmk_of_all_tx_,
                  "The removedKey is under cc_.local_wmk_of_all_tx_, should "
                  "not happen, cc_.local_wmk_of_all_tx_={}, "
                  "versionWorkerId={}, versionTxId={}, removedKey={}",
                  CoroEnv::CurTxMgr().cc_.local_wmk_of_all_tx_, version_worker_id, version_tx_id,
                  removed_key.ToString());
      if (chained_tuple.tx_id_ <= CoroEnv::CurTxMgr().cc_.local_wmk_of_short_tx_) {
        LEAN_DLOG("Move the removedKey to graveyard, versionWorkerId={}, "
                  "versionTxId={}, removedKey={}",
                  version_worker_id, version_tx_id, removed_key.ToString());
        // insert the removed key value to graveyard
        auto graveyard_x_iter = graveyard_->NewBTreeIterMut();
        auto g_ret = graveyard_x_iter->InsertKV(removed_key, x_iter->Val());
        if (g_ret != OpCode::kOK) {
          Log::Fatal("Failed to insert the removedKey to graveyard, ret={}, "
                     "versionWorkerId={}, versionTxId={}, removedKey={}, "
                     "removedVal={}",
                     ToString(g_ret), version_worker_id, version_tx_id, removed_key.ToString(),
                     x_iter->Val().ToString());
        }

        // remove the tombsone from main tree
        auto ret [[maybe_unused]] = x_iter->RemoveCurrent();
        LEAN_DCHECK(ret == OpCode::kOK,
                    "Failed to delete the removedKey tombstone from main tree, ret={}, "
                    "versionWorkerId={}, versionTxId={}, removedKey={}",
                    ToString(ret), version_worker_id, version_tx_id, removed_key.ToString());
        x_iter->TryMergeIfNeeded();
      } else {
        Log::Fatal("Meet a remove version upper than cc_.local_wmk_of_short_tx_, "
                   "should not happen, cc_.local_wmk_of_short_tx_={}, "
                   "versionWorkerId={}, versionTxId={}, removedKey={}",
                   CoroEnv::CurTxMgr().cc_.local_wmk_of_short_tx_, version_worker_id, version_tx_id,
                   removed_key.ToString());
      }
    } else {
      LEAN_DLOG("Skip moving removedKey to graveyard, tuple changed after "
                "remove, versionWorkerId={}, versionTxId={}, removedKey={}",
                version_worker_id, version_tx_id, removed_key.ToString());
    }
  }
  JUMPMU_CATCH() {
    LEAN_DLOG("GarbageCollect failed, try for next round, "
              "versionWorkerId={}, versionTxId={}, removedKey={}",
              version_worker_id, version_tx_id, removed_key.ToString());
  }
}

void TransactionKV::Unlock(const uint8_t* wal_record) {
  const lean_wal_record* wal = reinterpret_cast<const lean_wal_record*>(wal_record);
  Slice key;

  switch (wal->type_) {
  case LEAN_WAL_TYPE_TX_INSERT: {
    auto* wal_insert = reinterpret_cast<const lean_wal_tx_insert*>(wal);
    key = Slice{lean_wal_tx_insert_get_key(wal_insert), wal_insert->key_size_};
    break;
  }
  case LEAN_WAL_TYPE_TX_UPDATE: {
    auto* wal_update = reinterpret_cast<const lean_wal_tx_update*>(wal);
    key = Slice{lean_wal_tx_update_get_key(wal_update), wal_update->key_size_};
    break;
  }
  case LEAN_WAL_TYPE_TX_REMOVE: {
    auto* wal_remove = reinterpret_cast<const lean_wal_tx_remove*>(wal);
    key = Slice{lean_wal_tx_remove_get_key(wal_remove), wal_remove->key_size_};
    break;
  }
  default: {
    return;
    break;
  }
  }

  JUMPMU_TRY() {
    auto x_iter = NewBTreeIterMut();
    x_iter->SeekToEqual(key);
    LEAN_DCHECK(x_iter->Valid(), "Cannot find the key in btree, workerId={}, startTs={}, key={}",
                CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_,
                key.ToString());
    auto& tuple = *Tuple::From(x_iter->MutableVal().Data());
    ENSURE(tuple.format_ == TupleFormat::kChained);
  }
  JUMPMU_CATCH() {
    UNREACHABLE();
  }
}

// TODO: index range lock for serializability
template <bool asc>
OpCode TransactionKV::scan4ShortRunningTx(Slice key, ScanCallback callback) {
  bool keep_scanning = true;
  JUMPMU_TRY() {
    auto iter = NewBTreeIter();
    if (asc) {
      iter->SeekToFirstGreaterEqual(key);
    } else {
      iter->SeekToLastLessEqual(key);
    }

    while (iter->Valid()) {
      iter->AssembleKey();
      Slice scanned_key = iter->Key();
      GetVisibleTuple(iter->Val(), [&](Slice scanned_val) {
        keep_scanning = callback(scanned_key, scanned_val);
      });
      if (!keep_scanning) {
        JUMPMU_RETURN OpCode::kOK;
      }

      if (asc) {
        iter->Next();
      } else {
        iter->Prev();
      }
    }
    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
    LEAN_DCHECK(false, "Scan failed, key={}", key.ToString());
  }
  JUMPMU_RETURN OpCode::kOther;
}

// TODO: support scanning desc
template <bool asc>
OpCode TransactionKV::scan4LongRunningTx(Slice key, ScanCallback callback) {
  bool keep_scanning = true;
  JUMPMU_TRY() {
    auto iter = NewBTreeIter();
    OpCode o_ret;

    auto g_iter = graveyard_->NewBTreeIter();
    OpCode g_ret;

    Slice graveyard_lower_bound, graveyard_upper_bound;
    graveyard_lower_bound = key;

    if (iter->SeekToFirstGreaterEqual(key); !iter->Valid()) {
      JUMPMU_RETURN OpCode::kOK;
    }
    o_ret = OpCode::kOK;
    iter->AssembleKey();

    // Now it begins
    graveyard_upper_bound = iter->guarded_leaf_->GetUpperFence();
    auto g_range = [&]() {
      g_iter->Reset();
      if (graveyard_->IsRangeEmpty(graveyard_lower_bound, graveyard_upper_bound)) {
        g_ret = OpCode::kOther;
        return;
      }
      if (g_iter->SeekToFirstGreaterEqual(graveyard_lower_bound); !g_iter->Valid()) {
        g_ret = OpCode::kNotFound;
        return;
      }

      g_iter->AssembleKey();
      if (g_iter->Key() > graveyard_upper_bound) {
        g_ret = OpCode::kOther;
        g_iter->Reset();
        return;
      }

      g_ret = OpCode::kOK;
    };

    g_range();
    auto take_from_oltp = [&]() {
      GetVisibleTuple(iter->Val(),
                      [&](Slice value) { keep_scanning = callback(iter->Key(), value); });
      if (!keep_scanning) {
        return false;
      }
      const bool is_last_one = iter->IsLastOne();
      if (is_last_one) {
        g_iter->Reset();
      }
      iter->Next();
      o_ret = iter->Valid() ? OpCode::kOK : OpCode::kNotFound;
      if (is_last_one) {
        if (iter->buffer_.size() < iter->fence_size_ + 1u) {
          std::basic_string<uint8_t> new_buffer(iter->buffer_.size() + 1, 0);
          memcpy(new_buffer.data(), iter->buffer_.data(), iter->fence_size_);
          iter->buffer_ = std::move(new_buffer);
        }
        graveyard_lower_bound = Slice(&iter->buffer_[0], iter->fence_size_ + 1);
        graveyard_upper_bound = iter->guarded_leaf_->GetUpperFence();
        g_range();
      }
      return true;
    };
    while (true) {
      if (g_ret != OpCode::kOK && o_ret == OpCode::kOK) {
        iter->AssembleKey();
        if (!take_from_oltp()) {
          JUMPMU_RETURN OpCode::kOK;
        }
      } else if (g_ret == OpCode::kOK && o_ret != OpCode::kOK) {
        g_iter->AssembleKey();
        Slice g_key = g_iter->Key();
        GetVisibleTuple(g_iter->Val(),
                        [&](Slice value) { keep_scanning = callback(g_key, value); });
        if (!keep_scanning) {
          JUMPMU_RETURN OpCode::kOK;
        }
        g_iter->Next();
        g_ret = g_iter->Valid() ? OpCode::kOK : OpCode::kNotFound;
      } else if (g_ret == OpCode::kOK && o_ret == OpCode::kOK) {
        iter->AssembleKey();
        g_iter->AssembleKey();
        Slice g_key = g_iter->Key();
        Slice oltp_key = iter->Key();
        if (oltp_key <= g_key) {
          if (!take_from_oltp()) {
            JUMPMU_RETURN OpCode::kOK;
          }
        } else {
          GetVisibleTuple(g_iter->Val(),
                          [&](Slice value) { keep_scanning = callback(g_key, value); });
          if (!keep_scanning) {
            JUMPMU_RETURN OpCode::kOK;
          }
          g_iter->Next();
          g_ret = g_iter->Valid() ? OpCode::kOK : OpCode::kNotFound;
        }
      } else {
        JUMPMU_RETURN OpCode::kOK;
      }
    }
  }
  JUMPMU_CATCH() {
    LEAN_DCHECK(false);
  }
  JUMPMU_RETURN OpCode::kOther;
}

} // namespace leanstore::storage::btree
