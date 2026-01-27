#include "leanstore/btree/basic_kv.hpp"

#include "leanstore/base/error.hpp"
#include "leanstore/base/log.hpp"
#include "leanstore/btree/b_tree_generic.hpp"
#include "leanstore/btree/btree_iter.hpp"
#include "leanstore/btree/btree_iter_mut.hpp"
#include "leanstore/btree/column_store/column_leaf_ops.hpp"
#include "leanstore/c/wal_record.h"
#include "leanstore/coro/coro_env.hpp"
#include "leanstore/coro/mvcc_manager.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/sync/hybrid_mutex.hpp"
#include "leanstore/utils/misc.hpp"
#include "wal/wal_builder.hpp"

#include <cstring>
#include <format>
#include <string>

#include <sys/types.h>

namespace leanstore {

namespace {

int32_t FindCandidateBlockSlot(BTreeNode& leaf, Slice key) {
  // Choose the first block with max_key >= key.
  if (leaf.num_slots_ == 0) {
    return -1;
  }
  int32_t slot = leaf.LowerBound<false>(key);
  if (slot < 0 || slot == leaf.num_slots_) {
    return -1;
  }
  return slot;
}

OpCode ColumnLeafError(const char* context, const Error& error) {
  Log::Error("Column leaf {} failed: {}", context, error.ToString());
  return OpCode::kOther;
}

OpCode LookupColumnLeaf(BasicKV* tree, GuardedBufferFrame<BTreeNode>& guarded_leaf, Slice key,
                        ValCallback val_callback) {
  auto slot_id = FindCandidateBlockSlot(guarded_leaf.ref(), key);
  if (slot_id < 0) {
    return OpCode::kNotFound;
  }

  column_store::ColumnBlockRef ref{};
  if (guarded_leaf->ValSize(slot_id) != sizeof(ref)) {
    Log::Error("Column leaf entry size mismatch: expected {}, got {}", sizeof(ref),
               guarded_leaf->ValSize(slot_id));
    return OpCode::kOther;
  }
  std::memcpy(&ref, guarded_leaf->ValData(slot_id), sizeof(ref));

  EncodedRow encoded;
  auto lookup_res = column_store::LookupColumnBlock(tree->store_, ref, key, &encoded);
  if (!lookup_res) {
    return ColumnLeafError("LookupColumnBlock", lookup_res.error());
  }
  if (!lookup_res.value()) {
    return OpCode::kNotFound;
  }
  val_callback(Slice(encoded.value_));
  return OpCode::kOK;
}

} // namespace

Result<BasicKV*> BasicKV::Create(leanstore::LeanStore* store, const std::string& tree_name,
                                 lean_btree_config config) {
  auto [tree_ptr, tree_id] = store->tree_registry_->CreateTree(tree_name, [&]() {
    return std::unique_ptr<BufferManagedTree>(static_cast<BufferManagedTree*>(new BasicKV()));
  });
  if (tree_ptr == nullptr) {
    return Error::General("Tree name has been taken");
  }
  auto* tree = DownCast<BasicKV*>(tree_ptr);
  tree->Init(store, tree_id, std::move(config));
  return tree;
}

OpCode BasicKV::LookupOptimistic(Slice key, ValCallback val_callback) {
  JUMPMU_TRY() {
    GuardedBufferFrame<BTreeNode> guarded_leaf;
    FindLeafCanJump(key, guarded_leaf, LatchMode::kOptimisticOrJump);
    if (column_store::IsColumnLeaf(*guarded_leaf.bf_)) {
      // Column leaves store block references; lookups decode and binary-search rows.
      auto rc = LookupColumnLeaf(this, guarded_leaf, key, val_callback);
      guarded_leaf.JumpIfModifiedByOthers();
      JUMPMU_RETURN rc;
    }
    auto slot_id = guarded_leaf->LowerBound<true>(key);
    if (slot_id != -1) {
      val_callback(guarded_leaf->Value(slot_id));
      guarded_leaf.JumpIfModifiedByOthers();
      JUMPMU_RETURN OpCode::kOK;
    }

    guarded_leaf.JumpIfModifiedByOthers();
    JUMPMU_RETURN OpCode::kNotFound;
  }
  JUMPMU_CATCH() {
    return OpCode::kOther;
  }
}

OpCode BasicKV::LookupPessimistic(Slice key, ValCallback val_callback) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> guarded_leaf;
      FindLeafCanJump(key, guarded_leaf, LatchMode::kSharedPessimistic);
      if (column_store::IsColumnLeaf(*guarded_leaf.bf_)) {
        JUMPMU_RETURN LookupColumnLeaf(this, guarded_leaf, key, val_callback);
      }
      auto slot_id = guarded_leaf->LowerBound<true>(key);
      if (slot_id != -1) {
        val_callback(guarded_leaf->Value(slot_id));
        JUMPMU_RETURN OpCode::kOK;
      }

      JUMPMU_RETURN OpCode::kNotFound;
    }
    JUMPMU_CATCH() {
    }
  }
}

OpCode BasicKV::Lookup(Slice key, ValCallback val_callback) {
  if (auto ret = LookupOptimistic(key, val_callback); ret != OpCode::kOther) {
    return ret;
  }
  return LookupPessimistic(std::move(key), std::move(val_callback));
}

bool BasicKV::IsRangeEmpty(Slice start_key, Slice end_key) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> guarded_leaf;
      FindLeafCanJump(start_key, guarded_leaf);

      Slice upper_fence = guarded_leaf->GetUpperFence();
      LEAN_DCHECK(start_key >= guarded_leaf->GetLowerFence());

      if ((guarded_leaf->upper_fence_.IsInfinity() || end_key <= upper_fence) &&
          guarded_leaf->num_slots_ == 0) {
        int32_t pos = guarded_leaf->LowerBound<false>(start_key);
        if (pos == guarded_leaf->num_slots_) {
          guarded_leaf.JumpIfModifiedByOthers();
          JUMPMU_RETURN true;
        }

        guarded_leaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN false;
      }

      guarded_leaf.JumpIfModifiedByOthers();
      JUMPMU_RETURN false;
    }
    JUMPMU_CATCH() {
    }
  }
  UNREACHABLE();
  return false;
}

OpCode BasicKV::ScanAsc(Slice start_key, ScanCallback callback) {
  JUMPMU_TRY() {
    auto iter = NewBTreeIter();
    if (iter->SeekToFirstGreaterEqual(start_key); !iter->Valid()) {
      JUMPMU_RETURN OpCode::kNotFound;
    }
    if (column_store::IsColumnLeaf(*iter->guarded_leaf_.bf_)) {
      // Scan the column blocks in order and re-encode rows for callbacks.
      column_store::ColumnBlockScanState scan_state{
          .need_start_row_ = start_key.size() > 0,
          .key_decoded_ = false,
          .key_datums_ = {},
      };
      bool emitted = false;
      for (; iter->Valid(); iter->Next()) {
        column_store::ColumnBlockRef ref{};
        auto val = iter->Val();
        if (val.size() != sizeof(ref)) {
          Log::Error("Column leaf entry size mismatch: expected {}, got {}", sizeof(ref),
                     val.size());
          JUMPMU_RETURN OpCode::kOther;
        }
        std::memcpy(&ref, val.data(), sizeof(ref));
        auto scan_res =
            column_store::ScanColumnBlockAsc(store_, ref, start_key, &scan_state, callback);
        if (!scan_res) {
          JUMPMU_RETURN ColumnLeafError("ScanColumnBlockAsc", scan_res.error());
        }
        const auto& scan = scan_res.value();
        emitted = emitted || scan.emitted_;
        if (scan.stop_) {
          JUMPMU_RETURN OpCode::kOK;
        }
      }
      JUMPMU_RETURN emitted ? OpCode::kOK : OpCode::kNotFound;
    }

    for (; iter->Valid(); iter->Next()) {
      iter->AssembleKey();
      auto key = iter->Key();
      auto value = iter->Val();
      if (!callback(key, value)) {
        break;
      }
    }

    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BasicKV::ScanDesc(Slice scan_key, ScanCallback callback) {
  JUMPMU_TRY() {
    auto iter = NewBTreeIter();
    if (iter->SeekToLastLessEqual(scan_key); !iter->Valid()) {
      JUMPMU_RETURN OpCode::kNotFound;
    }
    if (column_store::IsColumnLeaf(*iter->guarded_leaf_.bf_)) {
      // Desc scan starts from the last row in the candidate block.
      column_store::ColumnBlockScanState scan_state{
          .need_start_row_ = scan_key.size() > 0,
          .key_decoded_ = false,
          .key_datums_ = {},
      };
      bool emitted = false;
      for (; iter->Valid(); iter->Prev()) {
        column_store::ColumnBlockRef ref{};
        auto val = iter->Val();
        if (val.size() != sizeof(ref)) {
          Log::Error("Column leaf entry size mismatch: expected {}, got {}", sizeof(ref),
                     val.size());
          JUMPMU_RETURN OpCode::kOther;
        }
        std::memcpy(&ref, val.data(), sizeof(ref));
        auto scan_res =
            column_store::ScanColumnBlockDesc(store_, ref, scan_key, &scan_state, callback);
        if (!scan_res) {
          JUMPMU_RETURN ColumnLeafError("ScanColumnBlockDesc", scan_res.error());
        }
        const auto& scan = scan_res.value();
        emitted = emitted || scan.emitted_;
        if (scan.stop_) {
          JUMPMU_RETURN OpCode::kOK;
        }
      }
      JUMPMU_RETURN emitted ? OpCode::kOK : OpCode::kNotFound;
    }

    for (; iter->Valid(); iter->Prev()) {
      iter->AssembleKey();
      auto key = iter->Key();
      auto value = iter->Val();
      if (!callback(key, value)) {
        break;
      }
    }

    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BasicKV::Insert(Slice key, Slice val) {
  JUMPMU_TRY() {
    auto x_iter = NewBTreeIterMut();
    auto ret = x_iter->InsertKV(key, val);

    if (ret == OpCode::kDuplicated) {
      Log::Info("Insert duplicated, key={}, treeId={}", key.ToString(), tree_id_);
      JUMPMU_RETURN OpCode::kDuplicated;
    }

    if (ret != OpCode::kOK) {
      Log::Info("Insert failed, key={}, ret={}", key.ToString(), ToString(ret));
      JUMPMU_RETURN ret;
    }

    if (config_.enable_wal_) {
      x_iter->guarded_leaf_.UpdatePageVersion();
      WalBuilder<lean_wal_insert>(this->tree_id_, key.size() + val.size())
          .SetPageInfo(x_iter->guarded_leaf_.bf_)
          .BuildInsert(key, val)
          .Submit();
    }
  }
  JUMPMU_CATCH() {
  }
  return OpCode::kOK;
}

OpCode BasicKV::PrefixLookup(Slice prefix_key, PrefixLookupCallback callback) {
  JUMPMU_TRY() {
    auto iter = NewBTreeIter();
    if (iter->SeekToFirstGreaterEqual(prefix_key); !iter->Valid()) {
      JUMPMU_RETURN OpCode::kNotFound;
    }
    bool found_prefix_key = false;
    uint16_t prefix_size = prefix_key.size();
    for (; iter->Valid(); iter->Next()) {
      iter->AssembleKey();
      auto key = iter->Key();
      auto value = iter->Val();
      if ((key.size() < prefix_size) || (bcmp(key.data(), prefix_key.data(), prefix_size) != 0)) {
        break;
      }
      callback(key, value);
      found_prefix_key = true;
    }
    if (!found_prefix_key) {
      JUMPMU_RETURN OpCode::kNotFound;
    }
    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BasicKV::PrefixLookupForPrev(Slice key, PrefixLookupCallback callback) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> guarded_leaf;
      FindLeafCanJump(key, guarded_leaf);

      bool is_equal = false;
      int16_t cur = guarded_leaf->LowerBound<false>(key, &is_equal);
      if (is_equal == true) {
        callback(key, guarded_leaf->Value(cur));
        guarded_leaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OpCode::kOK;
      }

      if (cur > 0) {
        cur -= 1;
        auto full_key_size = guarded_leaf->GetFullKeyLen(cur);
        auto full_key_buf = utils::JumpScopedArray<uint8_t>(full_key_size);
        guarded_leaf->CopyFullKey(cur, full_key_buf->get());
        guarded_leaf.JumpIfModifiedByOthers();

        callback(Slice(full_key_buf->get(), full_key_size), guarded_leaf->Value(cur));
        guarded_leaf.JumpIfModifiedByOthers();

        JUMPMU_RETURN OpCode::kOK;
      }

      OpCode ret = ScanDesc(key, [&](Slice scanned_key, Slice scanned_val) {
        callback(scanned_key, scanned_val);
        return false;
      });
      JUMPMU_RETURN ret;
    }
    JUMPMU_CATCH() {
    }
  }

  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BasicKV::UpdatePartial(Slice key, MutValCallback update_call_back, UpdateDesc& update_desc) {
  JUMPMU_TRY() {
    auto x_iter = NewBTreeIterMut();
    if (x_iter->SeekToEqual(key); !x_iter->Valid()) {
      JUMPMU_RETURN OpCode::kNotFound;
    }
    auto current_val = x_iter->MutableVal();
    if (config_.enable_wal_) {
      x_iter->guarded_leaf_.UpdatePageVersion();
      LEAN_DCHECK(update_desc.num_slots_ > 0);
      auto size_of_desc_and_delta = update_desc.SizeWithDelta();
      auto builder =
          WalBuilder<lean_wal_update>(this->tree_id_, key.size() + size_of_desc_and_delta)
              .SetPageInfo(x_iter->guarded_leaf_.bf_)
              .BuildUpdate(key, update_desc);
      auto* delta_ptr = lean_wal_update_get_delta(builder.GetWal());

      // 1. copy old value to wal buffer
      BasicKV::CopyToBuffer(update_desc, current_val.data(), delta_ptr);

      // 2. update with the new value
      update_call_back(current_val);

      // 3. xor with new value, store the result in wal buffer
      BasicKV::XorToBuffer(update_desc, current_val.data(), delta_ptr);

      builder.Submit();
    } else {
      // The actual update by the client
      update_call_back(current_val);
    }

    x_iter->UpdateContentionStats();
    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BasicKV::Remove(Slice key) {
  JUMPMU_TRY() {
    auto x_iter = NewBTreeIterMut();
    if (x_iter->SeekToEqual(key); !x_iter->Valid()) {
      JUMPMU_RETURN OpCode::kNotFound;
    }

    Slice value = x_iter->Val();
    if (config_.enable_wal_) {
      x_iter->guarded_leaf_.UpdatePageVersion();
      WalBuilder<lean_wal_remove>(this->tree_id_, key.size() + value.size())
          .SetPageInfo(x_iter->guarded_leaf_.bf_)
          .BuildRemove(key, value)
          .Submit();
    }
    auto ret = x_iter->RemoveCurrent();
    ENSURE(ret == OpCode::kOK);
    x_iter->TryMergeIfNeeded();
    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BasicKV::RangeRemove(Slice start_key, Slice end_key, bool page_wise) {
  JUMPMU_TRY() {
    auto x_iter = NewBTreeIterMut();
    x_iter->SetExitLeafCallback([&](GuardedBufferFrame<BTreeNode>& guarded_leaf) {
      if (guarded_leaf->FreeSpaceAfterCompaction() >= BTreeNode::UnderFullSize()) {
        x_iter->SetCleanUpCallback([&, to_merge = guarded_leaf.bf_] {
          JUMPMU_TRY() {
            lean_txid_t sys_tx_id = store_->GetMvccManager().AllocSysTxTs();
            this->TryMergeMayJump(sys_tx_id, *to_merge);
          }
          JUMPMU_CATCH() {
          }
        });
      }
    });

    ENSURE(config_.enable_wal_ == false);
    if (!page_wise) {
      x_iter->SeekToFirstGreaterEqual(start_key);
      if (!x_iter->Valid()) {
        JUMPMU_RETURN OpCode::kNotFound;
      }
      while (true) {
        x_iter->AssembleKey();
        auto current_key = x_iter->Key();
        if (current_key >= start_key && current_key <= end_key) {
          auto ret = x_iter->RemoveCurrent();
          ENSURE(ret == OpCode::kOK);
          if (x_iter->slot_id_ == x_iter->guarded_leaf_->num_slots_) {
            x_iter->Next();
            ret = x_iter->Valid() ? OpCode::kOK : OpCode::kNotFound;
          }
        } else {
          break;
        }
      }
      JUMPMU_RETURN OpCode::kOK;
    }

    bool did_purge_full_page = false;
    x_iter->SetEnterLeafCallback([&](GuardedBufferFrame<BTreeNode>& guarded_leaf) {
      if (guarded_leaf->num_slots_ == 0) {
        return;
      }

      // page start key
      auto first_key_size = guarded_leaf->GetFullKeyLen(0);
      auto first_key = utils::JumpScopedArray<uint8_t>(first_key_size);
      guarded_leaf->CopyFullKey(0, first_key->get());
      Slice page_start_key(first_key->get(), first_key_size);

      // page end key
      auto last_key_size = guarded_leaf->GetFullKeyLen(guarded_leaf->num_slots_ - 1);
      auto last_key = utils::JumpScopedArray<uint8_t>(last_key_size);
      guarded_leaf->CopyFullKey(guarded_leaf->num_slots_ - 1, last_key->get());
      Slice page_end_key(last_key->get(), last_key_size);

      if (page_start_key >= start_key && page_end_key <= end_key) {
        // Purge the whole page
        guarded_leaf->Reset();
        did_purge_full_page = true;
      }
    });

    while (true) {
      x_iter->SeekToFirstGreaterEqual(start_key);
      if (did_purge_full_page) {
        did_purge_full_page = false;
        continue;
      }
      break;
    }
  }

  JUMPMU_CATCH() {
  }
  return OpCode::kOK;
}

uint64_t BasicKV::CountEntries() {
  return BTreeGeneric::CountEntries();
}

} // namespace leanstore
