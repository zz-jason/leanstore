#include "HistoryTree.hpp"

#include "Units.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "storage/btree/core/BTreeExclusiveIterator.hpp"
#include "storage/btree/core/BTreeSharedIterator.hpp"

#include <alloca.h>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <thread>
#include <unordered_map>
#include <vector>

namespace leanstore {
namespace cr {
using namespace leanstore::storage::btree;

void HistoryTree::insertVersion(WORKERID session_id, TXID tx_id,
                                COMMANDID command_id, TREEID treeId,
                                bool is_remove, u64 payload_length,
                                std::function<void(u8*)> cb, bool same_thread) {
  if (!FLAGS_history_tree_inserts) {
    return;
  }
  const u64 key_length = sizeof(tx_id) + sizeof(command_id);
  u8 key_buffer[key_length];
  u64 offset = 0;
  offset += utils::fold(key_buffer + offset, tx_id);
  offset += utils::fold(key_buffer + offset, command_id);
  Slice key(key_buffer, key_length);
  payload_length += sizeof(VersionMeta);

  BTreeLL* volatile btree =
      (is_remove) ? remove_btrees[session_id] : update_btrees[session_id];
  Session* volatile session = nullptr;
  if (same_thread) {
    session = (is_remove) ? &remove_sessions[session_id]
                          : &update_sessions[session_id];
  }
  if (session != nullptr && session->rightmost_init) {
    JUMPMU_TRY() {
      BTreeExclusiveIterator iterator(
          *static_cast<BTreeGeneric*>(const_cast<BTreeLL*>(btree)),
          session->rightmost_bf, session->rightmost_version);
      // -------------------------------------------------------------------------------------
      OP_RESULT ret = iterator.enoughSpaceInCurrentNode(key, payload_length);
      if (ret == OP_RESULT::OK && iterator.keyInCurrentBoundaries(key)) {
        if (session->last_tx_id == tx_id) {
          iterator.mGuardedLeaf->insertDoNotCopyPayload(key, payload_length,
                                                        session->rightmost_pos);
          iterator.mSlotId = session->rightmost_pos;
        } else {
          iterator.insertInCurrentNode(key, payload_length);
        }
        auto& version_meta =
            *new (iterator.mutableValue().data()) VersionMeta();
        version_meta.mTreeId = treeId;
        cb(version_meta.payload);
        iterator.markAsDirty();
        COUNTERS_BLOCK() {
          WorkerCounters::myCounters().cc_versions_space_inserted_opt[treeId]++;
        }
        iterator.mGuardedLeaf.unlock();
        JUMPMU_RETURN;
      }
    }
    JUMPMU_CATCH() {
    }
  }
  // -------------------------------------------------------------------------------------
  while (true) {
    JUMPMU_TRY() {
      BTreeExclusiveIterator iterator(
          *static_cast<BTreeGeneric*>(const_cast<BTreeLL*>(btree)));
      // -------------------------------------------------------------------------------------
      OP_RESULT ret = iterator.seekToInsert(key);
      if (ret == OP_RESULT::DUPLICATE) {
        iterator.removeCurrent(); // TODO: verify, this implies upsert semantic
      } else {
        ENSURE(ret == OP_RESULT::OK);
      }
      ret = iterator.enoughSpaceInCurrentNode(key, payload_length);
      if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
        iterator.splitForKey(key);
        JUMPMU_CONTINUE;
      }
      iterator.insertInCurrentNode(key, payload_length);
      auto& version_meta = *new (iterator.mutableValue().data()) VersionMeta();
      version_meta.mTreeId = treeId;
      cb(version_meta.payload);
      iterator.markAsDirty();
      // -------------------------------------------------------------------------------------
      if (session != nullptr) {
        session->rightmost_bf = iterator.mGuardedLeaf.mBf;
        session->rightmost_version = iterator.mGuardedLeaf.mGuard.mVersion + 1;
        session->rightmost_pos = iterator.mSlotId + 1;
        session->last_tx_id = tx_id;
        session->rightmost_init = true;
      }
      // -------------------------------------------------------------------------------------
      COUNTERS_BLOCK() {
        WorkerCounters::myCounters().cc_versions_space_inserted[treeId]++;
      }
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
    }
  }
}
// -------------------------------------------------------------------------------------
bool HistoryTree::retrieveVersion(WORKERID workerId, TXID tx_id,
                                  COMMANDID command_id, const bool is_remove,
                                  std::function<void(const u8*, u64)> cb) {
  BTreeLL* volatile btree =
      (is_remove) ? remove_btrees[workerId] : update_btrees[workerId];
  // -------------------------------------------------------------------------------------
  const u64 key_length = sizeof(tx_id) + sizeof(command_id);
  u8 key_buffer[key_length];
  u64 offset = 0;
  offset += utils::fold(key_buffer + offset, tx_id);
  offset += utils::fold(key_buffer + offset, command_id);
  // -------------------------------------------------------------------------------------
  Slice key(key_buffer, key_length);
  JUMPMU_TRY() {
    BTreeSharedIterator iterator(
        *static_cast<BTreeGeneric*>(const_cast<BTreeLL*>(btree)),
        LATCH_FALLBACK_MODE::SHARED);
    OP_RESULT ret = iterator.seekExact(key);
    if (ret != OP_RESULT::OK) {
      JUMPMU_RETURN false;
    }
    Slice payload = iterator.value();
    const auto& version_container =
        *reinterpret_cast<const VersionMeta*>(payload.data());
    cb(version_container.payload, payload.length() - sizeof(VersionMeta));
    JUMPMU_RETURN true;
  }
  JUMPMU_CATCH() {
    UNREACHABLE();
    jumpmu::jump();
  }
  UNREACHABLE();
  return false;
}

// [from, to]
void HistoryTree::purgeVersions(WORKERID workerId, TXID from_tx_id,
                                TXID to_tx_id, RemoveVersionCallback cb,
                                [[maybe_unused]] const u64 limit) {
  u16 key_length = sizeof(to_tx_id);
  u8 key_buffer[PAGE_SIZE];
  utils::fold(key_buffer, from_tx_id);
  Slice key(key_buffer, key_length);
  u8 payload[PAGE_SIZE];
  u16 payload_length;
  volatile u64 removed_versions = 0;
  BTreeLL* volatile btree = remove_btrees[workerId];

  {
    JUMPMU_TRY() {
    restartrem : {
      leanstore::storage::btree::BTreeExclusiveIterator iterator(
          *static_cast<BTreeGeneric*>(const_cast<BTreeLL*>(btree)));
      iterator.exitLeafCallback([&](HybridPageGuard<BTreeNode>& leaf) {
        if (leaf->freeSpaceAfterCompaction() >=
            BTreeNodeHeader::sUnderFullSize) {
          iterator.cleanUpCallback([&, to_find = leaf.mBf] {
            JUMPMU_TRY() {
              btree->tryMerge(*to_find);
            }
            JUMPMU_CATCH() {
            }
          });
        }
      });
      // -------------------------------------------------------------------------------------
      OP_RESULT ret = iterator.seek(key);
      while (ret == OP_RESULT::OK) {
        iterator.assembleKey();
        TXID current_tx_id;
        utils::unfold(iterator.key().data(), current_tx_id);
        if (current_tx_id >= from_tx_id && current_tx_id <= to_tx_id) {
          auto& version_container =
              *reinterpret_cast<VersionMeta*>(iterator.mutableValue().data());
          const TREEID treeId = version_container.mTreeId;
          const bool called_before = version_container.called_before;
          version_container.called_before = true;
          key_length = iterator.key().length();
          std::memcpy(key_buffer, iterator.key().data(), key_length);
          payload_length = iterator.value().length() - sizeof(VersionMeta);
          std::memcpy(payload, version_container.payload, payload_length);
          key = Slice(key_buffer, key_length + 1);
          iterator.removeCurrent();
          removed_versions = removed_versions + 1;
          iterator.markAsDirty();
          iterator.reset();
          cb(current_tx_id, treeId, payload, payload_length, called_before);
          goto restartrem;
        } else {
          break;
        }
      }
    }
    }
    JUMPMU_CATCH() {
      UNREACHABLE();
    }
  }

  btree = update_btrees[workerId];
  utils::fold(key_buffer, from_tx_id);

  // Attention: no cross worker gc in sync
  Session* volatile session = &update_sessions[workerId];
  volatile bool should_try = true;
  if (from_tx_id == 0) {
    JUMPMU_TRY() {
      if (session->leftmost_init) {
        BufferFrame* bf = session->leftmost_bf;
        Guard bf_guard(bf->header.mLatch, session->leftmost_version);
        bf_guard.JumpIfModifiedByOthers();
        HybridPageGuard<BTreeNode> leaf(std::move(bf_guard), bf);

        if (leaf->mLowerFence.length == 0) {
          // Allocate in the stack, freed when the calling function exits.
          u8* last_key =
              (u8*)alloca(leaf->getFullKeyLen(leaf->mNumSeps - 1) * sizeof(u8));
          leaf->copyFullKey(leaf->mNumSeps - 1, last_key);
          TXID last_key_tx_id;
          utils::unfold(last_key, last_key_tx_id);
          if (last_key_tx_id > to_tx_id) {
            should_try = false;
          }
        }
      }
    }
    JUMPMU_CATCH() {
    }
  }
  while (should_try) {
    JUMPMU_TRY() {
      leanstore::storage::btree::BTreeExclusiveIterator iterator(
          *static_cast<BTreeGeneric*>(const_cast<BTreeLL*>(btree)));
      iterator.exitLeafCallback([&](HybridPageGuard<BTreeNode>& leaf) {
        if (leaf->freeSpaceAfterCompaction() >=
            BTreeNodeHeader::sUnderFullSize) {
          iterator.cleanUpCallback([&, to_find = leaf.mBf] {
            JUMPMU_TRY() {
              btree->tryMerge(*to_find);
            }
            JUMPMU_CATCH() {
            }
          });
        }
      });

      // ATTENTION: we use this also for purging the current aborted tx so we
      // can not simply assume from_tx_id = 0
      bool did_purge_full_page = false;
      iterator.enterLeafCallback([&](HybridPageGuard<BTreeNode>& leaf) {
        if (leaf->mNumSeps == 0) {
          return;
        }
        // Allocate in the stack, freed when the calling function exits.
        auto first_key = (u8*)alloca(leaf->getFullKeyLen(0) * sizeof(u8));
        leaf->copyFullKey(0, first_key);
        TXID first_key_tx_id;
        utils::unfold(first_key, first_key_tx_id);

        // Allocate in the stack, freed when the calling function exits.
        auto last_key =
            (u8*)alloca(leaf->getFullKeyLen(leaf->mNumSeps - 1) * sizeof(u8));
        leaf->copyFullKey(leaf->mNumSeps - 1, last_key);
        TXID last_key_tx_id;
        utils::unfold(last_key, last_key_tx_id);
        if (first_key_tx_id >= from_tx_id && to_tx_id >= last_key_tx_id) {
          // Purge the whole page
          removed_versions = removed_versions + leaf->mNumSeps;
          leaf->reset();
          did_purge_full_page = true;
        }
      });

      iterator.seek(key);
      if (did_purge_full_page) {
        did_purge_full_page = false;
        JUMPMU_CONTINUE;
      } else {
        session->leftmost_bf = iterator.mGuardedLeaf.mBf;
        session->leftmost_version = iterator.mGuardedLeaf.mGuard.mVersion + 1;
        session->leftmost_init = true;
        JUMPMU_BREAK;
      }
    }
    JUMPMU_CATCH() {
      UNREACHABLE();
    }
  }
  COUNTERS_BLOCK() {
    CRCounters::myCounters().cc_versions_space_removed += removed_versions;
  }
}

// Pre: TXID is unsigned integer
void HistoryTree::visitRemoveVersions(
    WORKERID workerId, TXID from_tx_id, TXID to_tx_id,
    std::function<void(const TXID, const TREEID, const u8*, u64,
                       const bool visited_before)>
        cb) {
  // [from, to]
  BTreeLL* btree = remove_btrees[workerId];
  u16 key_length = sizeof(to_tx_id);
  u8 key_buffer[PAGE_SIZE];
  u64 offset = 0;
  offset += utils::fold(key_buffer + offset, from_tx_id);
  Slice key(key_buffer, key_length);
  u8 payload[PAGE_SIZE];
  u16 payload_length;

  JUMPMU_TRY() {
  restart : {
    leanstore::storage::btree::BTreeExclusiveIterator iterator(
        *static_cast<BTreeGeneric*>(btree));
    OP_RESULT ret = iterator.seek(key);
    while (ret == OP_RESULT::OK) {
      iterator.assembleKey();
      TXID current_tx_id;
      utils::unfold(iterator.key().data(), current_tx_id);
      if (current_tx_id >= from_tx_id && current_tx_id <= to_tx_id) {
        auto& version_container =
            *reinterpret_cast<VersionMeta*>(iterator.mutableValue().data());
        const TREEID treeId = version_container.mTreeId;
        const bool called_before = version_container.called_before;
        ENSURE(called_before == false);
        version_container.called_before = true;
        key_length = iterator.key().length();
        std::memcpy(key_buffer, iterator.key().data(), key_length);
        payload_length = iterator.value().length() - sizeof(VersionMeta);
        std::memcpy(payload, version_container.payload, payload_length);
        key = Slice(key_buffer, key_length + 1);
        if (!called_before) {
          iterator.markAsDirty();
        }
        iterator.reset();
        cb(current_tx_id, treeId, payload, payload_length, called_before);
        goto restart;
      } else {
        break;
      }
    }
  }
  }
  JUMPMU_CATCH() {
  }
}

} // namespace cr
} // namespace leanstore
