#include "leanstore/btree/basic_kv.hpp"

#include "btree/core/b_tree_wal_payload.hpp"
#include "leanstore/btree/core/b_tree_generic.hpp"
#include "leanstore/btree/core/pessimistic_exclusive_iterator.hpp"
#include "leanstore/btree/core/pessimistic_shared_iterator.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/sync/hybrid_latch.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/misc.hpp"

#include <format>
#include <string>

#include <sys/types.h>

using namespace std;
using namespace leanstore::storage;

namespace leanstore::storage::btree {

Result<BasicKV*> BasicKV::Create(leanstore::LeanStore* store, const std::string& tree_name,
                                 BTreeConfig config) {
  auto [tree_ptr, tree_id] = store->tree_registry_->CreateTree(tree_name, [&]() {
    return std::unique_ptr<BufferManagedTree>(static_cast<BufferManagedTree*>(new BasicKV()));
  });
  if (tree_ptr == nullptr) {
    return std::unexpected<utils::Error>(utils::Error::General("Tree name has been taken"));
  }
  auto* tree = DownCast<BasicKV*>(tree_ptr);
  tree->Init(store, tree_id, std::move(config));
  return tree;
}

OpCode BasicKV::LookupOptimistic(Slice key, ValCallback val_callback) {
  JUMPMU_TRY() {
    GuardedBufferFrame<BTreeNode> guarded_leaf;
    FindLeafCanJump(key, guarded_leaf, LatchMode::kOptimisticOrJump);
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
      LS_DCHECK(start_key >= guarded_leaf->GetLowerFence());

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
    auto iter = GetIterator();
    if (iter.SeekToFirstGreaterEqual(start_key); !iter.Valid()) {
      JUMPMU_RETURN OpCode::kNotFound;
    }
    for (; iter.Valid(); iter.Next()) {
      iter.AssembleKey();
      auto key = iter.Key();
      auto value = iter.Val();
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
    auto iter = GetIterator();
    if (iter.SeekToLastLessEqual(scan_key); !iter.Valid()) {
      JUMPMU_RETURN OpCode::kNotFound;
    }
    for (; iter.Valid(); iter.Prev()) {
      iter.AssembleKey();
      auto key = iter.Key();
      auto value = iter.Val();
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
    auto x_iter = GetExclusiveIterator();
    auto ret = x_iter.InsertKV(key, val);

    if (ret == OpCode::kDuplicated) {
      Log::Info("Insert duplicated, workerId={}, key={}, treeId={}",
                cr::WorkerContext::My().worker_id_, key.ToString(), tree_id_);
      JUMPMU_RETURN OpCode::kDuplicated;
    }

    if (ret != OpCode::kOK) {
      Log::Info("Insert failed, workerId={}, key={}, ret={}", cr::WorkerContext::My().worker_id_,
                key.ToString(), ToString(ret));
      JUMPMU_RETURN ret;
    }

    if (config_.enable_wal_) {
      auto wal_size = key.length() + val.length();
      x_iter.guarded_leaf_.WriteWal<WalInsert>(wal_size, key, val);
    }
  }
  JUMPMU_CATCH() {
  }
  return OpCode::kOK;
}

OpCode BasicKV::PrefixLookup(Slice prefix_key, PrefixLookupCallback callback) {
  JUMPMU_TRY() {
    auto iter = GetIterator();
    if (iter.SeekToFirstGreaterEqual(prefix_key); !iter.Valid()) {
      JUMPMU_RETURN OpCode::kNotFound;
    }
    bool found_prefix_key = false;
    uint16_t prefix_size = prefix_key.size();
    for (; iter.Valid(); iter.Next()) {
      iter.AssembleKey();
      auto key = iter.Key();
      auto value = iter.Val();
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
    auto x_iter = GetExclusiveIterator();
    if (x_iter.SeekToEqual(key); !x_iter.Valid()) {
      JUMPMU_RETURN OpCode::kNotFound;
    }
    auto current_val = x_iter.MutableVal();
    if (config_.enable_wal_) {
      LS_DCHECK(update_desc.num_slots_ > 0);
      auto size_of_desc_and_delta = update_desc.SizeWithDelta();
      auto wal_handler =
          x_iter.guarded_leaf_.ReserveWALPayload<WalUpdate>(key.length() + size_of_desc_and_delta);
      wal_handler->type_ = WalPayload::Type::kWalUpdate;
      wal_handler->key_size_ = key.length();
      wal_handler->delta_length_ = size_of_desc_and_delta;
      auto* wal_ptr = wal_handler->payload_;
      std::memcpy(wal_ptr, key.data(), key.length());
      wal_ptr += key.length();
      std::memcpy(wal_ptr, &update_desc, update_desc.Size());
      wal_ptr += update_desc.Size();

      // 1. copy old value to wal buffer
      BasicKV::CopyToBuffer(update_desc, current_val.Data(), wal_ptr);

      // 2. update with the new value
      update_call_back(current_val);

      // 3. xor with new value, store the result in wal buffer
      BasicKV::XorToBuffer(update_desc, current_val.Data(), wal_ptr);
      wal_handler.SubmitWal();
    } else {
      // The actual update by the client
      update_call_back(current_val);
    }

    x_iter.UpdateContentionStats();
    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BasicKV::Remove(Slice key) {
  JUMPMU_TRY() {
    auto x_iter = GetExclusiveIterator();
    if (x_iter.SeekToEqual(key); !x_iter.Valid()) {
      JUMPMU_RETURN OpCode::kNotFound;
    }

    Slice value = x_iter.Val();
    if (config_.enable_wal_) {
      auto wal_handler =
          x_iter.guarded_leaf_.ReserveWALPayload<WalRemove>(key.size() + value.size(), key, value);
      wal_handler.SubmitWal();
    }
    auto ret = x_iter.RemoveCurrent();
    ENSURE(ret == OpCode::kOK);
    x_iter.TryMergeIfNeeded();
    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BasicKV::RangeRemove(Slice start_key, Slice end_key, bool page_wise) {
  JUMPMU_TRY() {
    auto x_iter = GetExclusiveIterator();
    x_iter.SetExitLeafCallback([&](GuardedBufferFrame<BTreeNode>& guarded_leaf) {
      if (guarded_leaf->FreeSpaceAfterCompaction() >= BTreeNode::UnderFullSize()) {
        x_iter.SetCleanUpCallback([&, to_merge = guarded_leaf.bf_] {
          JUMPMU_TRY() {
            TXID sys_tx_id = store_->AllocSysTxTs();
            this->TryMergeMayJump(sys_tx_id, *to_merge);
          }
          JUMPMU_CATCH() {
          }
        });
      }
    });

    ENSURE(config_.enable_wal_ == false);
    if (!page_wise) {
      x_iter.SeekToFirstGreaterEqual(start_key);
      if (!x_iter.Valid()) {
        JUMPMU_RETURN OpCode::kNotFound;
      }
      while (true) {
        x_iter.AssembleKey();
        auto current_key = x_iter.Key();
        if (current_key >= start_key && current_key <= end_key) {
          auto ret = x_iter.RemoveCurrent();
          ENSURE(ret == OpCode::kOK);
          if (x_iter.slot_id_ == x_iter.guarded_leaf_->num_slots_) {
            x_iter.Next();
            ret = x_iter.Valid() ? OpCode::kOK : OpCode::kNotFound;
          }
        } else {
          break;
        }
      }
      JUMPMU_RETURN OpCode::kOK;
    }

    bool did_purge_full_page = false;
    x_iter.SetEnterLeafCallback([&](GuardedBufferFrame<BTreeNode>& guarded_leaf) {
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
      x_iter.SeekToFirstGreaterEqual(start_key);
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

} // namespace leanstore::storage::btree
