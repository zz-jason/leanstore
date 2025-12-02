#include "leanstore/concurrency/history_storage.hpp"

#include "coroutine/mvcc_manager.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/core/b_tree_node.hpp"
#include "leanstore/btree/core/btree_iter_mut.hpp"
#include "leanstore/common/types.h"
#include "leanstore/lean_store.hpp"
#include "leanstore/sync/hybrid_mutex.hpp"
#include "leanstore/sync/scoped_hybrid_guard.hpp"
#include "leanstore/utils/jump_mu.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/managed_thread.hpp"
#include "leanstore/utils/misc.hpp"
#include "utils/small_vector.hpp"

#include <functional>

using namespace leanstore;

namespace leanstore {

void HistoryStorage::PutVersion(lean_txid_t tx_id, lean_cmdid_t command_id, lean_treeid_t tree_id,
                                bool is_remove, uint64_t version_size,
                                std::function<void(uint8_t*)> insert_call_back, bool same_thread) {
  // Compose the key to be inserted
  auto* btree = is_remove ? remove_index_ : update_index_;
  const auto key_size = sizeof(tx_id) + sizeof(command_id);
  uint8_t key_buffer[key_size];
  uint64_t offset = 0;
  offset += utils::Fold(key_buffer + offset, tx_id);
  offset += utils::Fold(key_buffer + offset, command_id);
  version_size += sizeof(VersionMeta);

  Session* volatile session = nullptr;
  if (same_thread) {
    session = (is_remove) ? &remove_session_ : &update_session_;
  }
  if (session != nullptr && session->rightmost_bf_ != nullptr) {
    JUMPMU_TRY() {
      Slice key(key_buffer, key_size);
      BTreeIterMut x_iter(*static_cast<BTreeGeneric*>(const_cast<BasicKV*>(btree)),
                          session->rightmost_bf_, session->rightmost_version_);
      if (x_iter.HasEnoughSpaceFor(key.size(), version_size) && x_iter.KeyInCurrentNode(key)) {

        if (session->last_tx_id_ == tx_id) {
          // Only need to keep one version for each txId?
          x_iter.guarded_leaf_->InsertDoNotCopyPayload(key, version_size, session->rightmost_pos_);
          x_iter.slot_id_ = session->rightmost_pos_;
        } else {
          x_iter.InsertToCurrentNode(key, version_size);
        }

        auto& version_meta = *new (x_iter.MutableVal().Data()) VersionMeta();
        version_meta.tree_id_ = tree_id;
        insert_call_back(version_meta.payload_);
        x_iter.guarded_leaf_.unlock();
        JUMPMU_RETURN;
      }
    }
    JUMPMU_CATCH() {
    }
  }

  while (true) {
    JUMPMU_TRY() {
      Slice key(key_buffer, key_size);
      auto x_iter = const_cast<BasicKV*>(btree)->NewBTreeIterMut();
      OpCode ret = x_iter->SeekToInsert(key);
      if (ret == OpCode::kDuplicated) {
        // remove the last inserted version for the key
        x_iter->RemoveCurrent();
      } else {
        ENSURE(ret == OpCode::kOK);
      }
      if (!x_iter->HasEnoughSpaceFor(key.size(), version_size)) {
        x_iter->SplitForKey(key);
        JUMPMU_CONTINUE;
      }
      x_iter->InsertToCurrentNode(key, version_size);
      auto& version_meta = *new (x_iter->MutableVal().Data()) VersionMeta();
      version_meta.tree_id_ = tree_id;
      insert_call_back(version_meta.payload_);

      if (session != nullptr) {
        session->rightmost_bf_ = x_iter->guarded_leaf_.bf_;
        session->rightmost_version_ = x_iter->guarded_leaf_.guard_.version_ + 1;
        session->rightmost_pos_ = x_iter->slot_id_ + 1;
        session->last_tx_id_ = tx_id;
      }

      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
    }
  }
}

bool HistoryStorage::GetVersion(lean_txid_t newer_tx_id, lean_cmdid_t newer_command_id,
                                const bool is_remove_command,
                                std::function<void(const uint8_t*, uint64_t)> cb) {
  volatile BasicKV* btree = (is_remove_command) ? remove_index_ : update_index_;
  const uint64_t key_size = sizeof(newer_tx_id) + sizeof(newer_command_id);
  uint8_t key_buffer[key_size];
  uint64_t offset = 0;
  offset += utils::Fold(key_buffer + offset, newer_tx_id);
  offset += utils::Fold(key_buffer + offset, newer_command_id);

  JUMPMU_TRY() {
    BasicKV* kv = const_cast<BasicKV*>(btree);
    auto ret = kv->Lookup(Slice(key_buffer, key_size), [&](const Slice& payload) {
      const auto& version_container = *VersionMeta::From(payload.data());
      cb(version_container.payload_, payload.length() - sizeof(VersionMeta));
    });

    if (ret == OpCode::kNotFound) {
      JUMPMU_RETURN false;
    }
    JUMPMU_RETURN true;
  }
  JUMPMU_CATCH() {
    Log::Error("Can not retrieve older version"
               ", newerTxId: {}, newerCommandId: {}, isRemoveCommand: {}",
               newer_tx_id, newer_command_id, is_remove_command);
  }
  UNREACHABLE();
  return false;
}

void HistoryStorage::PurgeVersions(lean_txid_t from_tx_id, lean_txid_t to_tx_id,
                                   RemoveVersionCallback on_remove_version,
                                   [[maybe_unused]] const uint64_t limit) {
  auto key_size = sizeof(to_tx_id);
  SmallBuffer256 key_buffer_holder(CoroEnv::CurStore().store_option_->page_size_);
  auto* key_buffer = key_buffer_holder.Data();

  utils::Fold(key_buffer, from_tx_id);
  Slice key(key_buffer, key_size);

  SmallBuffer<4096> pl_holder(CoroEnv::CurStore().store_option_->page_size_);
  auto* payload = pl_holder.Data();
  uint16_t payload_size;
  uint64_t versions_removed = 0;

  // purge remove versions
  auto* btree = remove_index_;
  JUMPMU_TRY() {
  restartrem: {
    auto x_iter = btree->NewBTreeIterMut();
    x_iter->SetExitLeafCallback([&](GuardedBufferFrame<BTreeNode>& guarded_leaf) {
      if (guarded_leaf->FreeSpaceAfterCompaction() >= BTreeNode::UnderFullSize()) {
        x_iter->SetCleanUpCallback([&, to_merge = guarded_leaf.bf_] {
          JUMPMU_TRY() {
            lean_txid_t sys_tx_id = btree->store_->GetMvccManager()->AllocSysTxTs();
            btree->TryMergeMayJump(sys_tx_id, *to_merge);
          }
          JUMPMU_CATCH() {
          }
        });
      }
    });
    for (x_iter->SeekToFirstGreaterEqual(key); x_iter->Valid();
         x_iter->SeekToFirstGreaterEqual(key)) {
      // finished if we are out of the transaction range
      x_iter->AssembleKey();
      lean_txid_t cur_tx_id;
      utils::Unfold(x_iter->Key().data(), cur_tx_id);
      if (cur_tx_id < from_tx_id || cur_tx_id > to_tx_id) {
        break;
      }

      auto& version_container = *reinterpret_cast<VersionMeta*>(x_iter->MutableVal().Data());
      const lean_treeid_t tree_id = version_container.tree_id_;
      const bool called_before = version_container.called_before_;
      version_container.called_before_ = true;

      // set the next key to be seeked
      key_size = x_iter->Key().size();
      std::memcpy(key_buffer, x_iter->Key().data(), key_size);
      key = Slice(key_buffer, key_size + 1);

      // get the remove version
      payload_size = x_iter->Val().size() - sizeof(VersionMeta);
      std::memcpy(payload, version_container.payload_, payload_size);

      // remove the version from history
      x_iter->RemoveCurrent();
      versions_removed = versions_removed + 1;
      x_iter->Reset();

      on_remove_version(cur_tx_id, tree_id, payload, payload_size, called_before);
      goto restartrem;
    }
  }
  }
  JUMPMU_CATCH() {
    UNREACHABLE();
  }

  // purge update versions
  btree = update_index_;
  utils::Fold(key_buffer, from_tx_id);

  // Attention: no cross worker gc in sync
  Session* volatile session = &update_session_;
  volatile bool should_try = true;
  if (from_tx_id == 0 && session->left_most_bf_ != nullptr) {
    JUMPMU_TRY() {
      BufferFrame* bf = session->left_most_bf_;

      // optimistic lock, jump if invalid
      ScopedHybridGuard bf_guard(bf->header_.latch_, session->left_most_version_);

      // lock successfull, check whether the page can be purged
      auto* leaf_node = reinterpret_cast<BTreeNode*>(bf->page_.payload_);
      if (leaf_node->lower_fence_.IsInfinity() && leaf_node->num_slots_ > 0) {
        auto last_key_size = leaf_node->GetFullKeyLen(leaf_node->num_slots_ - 1);
        SmallBuffer256 last_key_holder(last_key_size);
        auto* last_key = last_key_holder.Data();
        leaf_node->CopyFullKey(leaf_node->num_slots_ - 1, last_key);

        // optimistic unlock, jump if invalid
        bf_guard.Unlock();

        // now we can safely use the copied key
        lean_txid_t tx_id_in_lastkey;
        utils::Unfold(last_key, tx_id_in_lastkey);
        if (tx_id_in_lastkey > to_tx_id) {
          should_try = false;
        }
      }
    }
    JUMPMU_CATCH() {
    }
  }

  while (should_try) {
    JUMPMU_TRY() {
      auto x_iter = btree->NewBTreeIterMut();
      // check whether the page can be merged when exit a leaf
      x_iter->SetExitLeafCallback([&](GuardedBufferFrame<BTreeNode>& guarded_leaf) {
        if (guarded_leaf->FreeSpaceAfterCompaction() >= BTreeNode::UnderFullSize()) {
          x_iter->SetCleanUpCallback([&, to_merge = guarded_leaf.bf_] {
            JUMPMU_TRY() {
              lean_txid_t sys_tx_id = btree->store_->GetMvccManager()->AllocSysTxTs();
              btree->TryMergeMayJump(sys_tx_id, *to_merge);
            }
            JUMPMU_CATCH() {
            }
          });
        }
      });

      bool is_full_page_purged = false;
      // check whether the whole page can be purged when enter a leaf
      x_iter->SetEnterLeafCallback([&](GuardedBufferFrame<BTreeNode>& guarded_leaf) {
        if (guarded_leaf->num_slots_ == 0) {
          return;
        }

        // get the transaction id in the first key
        auto first_key_size = guarded_leaf->GetFullKeyLen(0);
        SmallBuffer256 first_key_holder(first_key_size);
        auto* first_key = first_key_holder.Data();
        guarded_leaf->CopyFullKey(0, first_key);
        lean_txid_t tx_id_in_first_key;
        utils::Unfold(first_key, tx_id_in_first_key);

        // get the transaction id in the last key
        auto last_key_size = guarded_leaf->GetFullKeyLen(guarded_leaf->num_slots_ - 1);
        SmallBuffer256 last_key_holder(last_key_size);
        auto* last_key = last_key_holder.Data();
        guarded_leaf->CopyFullKey(guarded_leaf->num_slots_ - 1, last_key);
        lean_txid_t tx_id_in_last_key;
        utils::Unfold(last_key, tx_id_in_last_key);

        // purge the whole page if it is in the range
        if (from_tx_id <= tx_id_in_first_key && tx_id_in_last_key <= to_tx_id) {
          versions_removed += guarded_leaf->num_slots_;
          guarded_leaf->Reset();
          is_full_page_purged = true;
        }
      });

      x_iter->SeekToFirstGreaterEqual(key);
      if (is_full_page_purged) {
        is_full_page_purged = false;
        JUMPMU_CONTINUE;
      }
      session->left_most_bf_ = x_iter->guarded_leaf_.bf_;
      session->left_most_version_ = x_iter->guarded_leaf_.guard_.version_ + 1;
      JUMPMU_BREAK;
    }
    JUMPMU_CATCH() {
      UNREACHABLE();
    }
  }
}

void HistoryStorage::VisitRemovedVersions(lean_txid_t from_tx_id, lean_txid_t to_tx_id,
                                          RemoveVersionCallback on_remove_version) {
  auto* remove_tree = remove_index_;
  auto key_size = sizeof(to_tx_id);
  SmallBuffer<4096> key_buffer_holder(CoroEnv::CurStore().store_option_->page_size_);
  auto* key_buffer = key_buffer_holder.Data();

  uint64_t offset = 0;
  offset += utils::Fold(key_buffer + offset, from_tx_id);
  Slice key(key_buffer, key_size);
  SmallBuffer<4096> payload_holder(CoroEnv::CurStore().store_option_->page_size_);
  auto* payload = payload_holder.Data();
  uint16_t payload_size;

  JUMPMU_TRY() {
  restart: {
    auto x_iter = remove_tree->NewBTreeIterMut();
    for (x_iter->SeekToFirstGreaterEqual(key); x_iter->Valid();
         x_iter->SeekToFirstGreaterEqual(key)) {
      // skip versions out of the transaction range
      x_iter->AssembleKey();
      lean_txid_t cur_tx_id;
      utils::Unfold(x_iter->Key().data(), cur_tx_id);
      if (cur_tx_id < from_tx_id || cur_tx_id > to_tx_id) {
        break;
      }

      auto& version_container = *VersionMeta::From(x_iter->MutableVal().Data());
      const lean_treeid_t tree_id = version_container.tree_id_;
      const bool called_before = version_container.called_before_;
      LEAN_DCHECK(called_before == false,
                  "Each remove version should be visited only once, treeId={}, txId={}", tree_id,
                  cur_tx_id);

      version_container.called_before_ = true;

      // set the next key to be seeked
      key_size = x_iter->Key().length();
      std::memcpy(key_buffer, x_iter->Key().data(), key_size);
      key = Slice(key_buffer, key_size + 1);

      // get the remove version
      payload_size = x_iter->Val().length() - sizeof(VersionMeta);
      std::memcpy(payload, version_container.payload_, payload_size);

      x_iter->Reset();
      on_remove_version(cur_tx_id, tree_id, payload, payload_size, called_before);
      goto restart;
    }
  }
  }
  JUMPMU_CATCH() {
  }
}

} // namespace leanstore
