#include "HistoryTree.hpp"

#include "Units.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "storage/btree/core/BTreeExclusiveIterator.hpp"
#include "storage/btree/core/BTreeSharedIterator.hpp"
#include "utils/Misc.hpp"

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

void HistoryTree::insertVersion(WORKERID workerId, TXID txId,
                                COMMANDID commandId, TREEID treeId,
                                bool isRemove, u64 payload_length,
                                std::function<void(u8*)> cb, bool same_thread) {
  if (!FLAGS_history_tree_inserts) {
    return;
  }
  const u64 keySize = sizeof(txId) + sizeof(commandId);
  u8 keyBuffer[keySize];
  u64 offset = 0;
  offset += utils::fold(keyBuffer + offset, txId);
  offset += utils::fold(keyBuffer + offset, commandId);
  Slice key(keyBuffer, keySize);
  payload_length += sizeof(VersionMeta);

  BTreeLL* btree =
      (isRemove) ? remove_btrees[workerId] : update_btrees[workerId];
  Session* session = nullptr;
  if (same_thread) {
    session =
        (isRemove) ? &remove_sessions[workerId] : &update_sessions[workerId];
  }
  if (session != nullptr && session->rightmost_init) {
    JUMPMU_TRY() {
      BTreeExclusiveIterator iterator(
          *static_cast<BTreeGeneric*>(const_cast<BTreeLL*>(btree)),
          session->rightmost_bf, session->rightmost_version);

      OP_RESULT ret = iterator.enoughSpaceInCurrentNode(key, payload_length);
      if (ret == OP_RESULT::OK && iterator.keyInCurrentBoundaries(key)) {
        if (session->last_tx_id == txId) {
          iterator.mGuardedLeaf->insertDoNotCopyPayload(key, payload_length,
                                                        session->rightmost_pos);
          iterator.mSlotId = session->rightmost_pos;
        } else {
          iterator.insertInCurrentNode(key, payload_length);
        }
        auto& version_meta = *new (iterator.MutableVal().data()) VersionMeta();
        version_meta.mTreeId = treeId;
        cb(version_meta.payload);
        iterator.MarkAsDirty();
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

  while (true) {
    JUMPMU_TRY() {
      BTreeExclusiveIterator iterator(
          *static_cast<BTreeGeneric*>(const_cast<BTreeLL*>(btree)));

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
      auto& version_meta = *new (iterator.MutableVal().data()) VersionMeta();
      version_meta.mTreeId = treeId;
      cb(version_meta.payload);
      iterator.MarkAsDirty();

      if (session != nullptr) {
        session->rightmost_bf = iterator.mGuardedLeaf.mBf;
        session->rightmost_version = iterator.mGuardedLeaf.mGuard.mVersion + 1;
        session->rightmost_pos = iterator.mSlotId + 1;
        session->last_tx_id = txId;
        session->rightmost_init = true;
      }

      COUNTERS_BLOCK() {
        WorkerCounters::myCounters().cc_versions_space_inserted[treeId]++;
      }
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
    }
  }
}

bool HistoryTree::retrieveVersion(WORKERID prevWorkerId, TXID prevTxId,
                                  COMMANDID prevCommandId,
                                  const bool isRemoveCommand,
                                  std::function<void(const u8*, u64)> cb) {
  BTreeLL* btree = (isRemoveCommand) ? remove_btrees[prevWorkerId]
                                     : update_btrees[prevWorkerId];

  const u64 keySize = sizeof(prevTxId) + sizeof(prevCommandId);
  u8 keyBuffer[keySize];
  u64 offset = 0;
  offset += utils::fold(keyBuffer + offset, prevTxId);
  offset += utils::fold(keyBuffer + offset, prevCommandId);

  Slice key(keyBuffer, keySize);
  JUMPMU_TRY() {
    BTreeSharedIterator iterator(
        *static_cast<BTreeGeneric*>(const_cast<BTreeLL*>(btree)),
        LATCH_FALLBACK_MODE::SHARED);
    OP_RESULT ret = iterator.seekExact(key);
    if (ret != OP_RESULT::OK) {
      JUMPMU_RETURN false;
    }
    Slice payload = iterator.value();
    const auto& versionContainer = *VersionMeta::From(payload.data());
    cb(versionContainer.payload, payload.length() - sizeof(VersionMeta));
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
  auto keySize = sizeof(to_tx_id);
  auto keyBuffer = utils::ScopedArray<u8>(FLAGS_page_size);
  utils::fold(keyBuffer.get(), from_tx_id);
  Slice key(keyBuffer.get(), keySize);
  auto payload = utils::ScopedArray<u8>(FLAGS_page_size);
  u16 payload_length;
  volatile u64 removed_versions = 0;
  BTreeLL* volatile btree = remove_btrees[workerId];

  {
    JUMPMU_TRY() {
    restartrem : {
      leanstore::storage::btree::BTreeExclusiveIterator iterator(
          *static_cast<BTreeGeneric*>(const_cast<BTreeLL*>(btree)));
      iterator.exitLeafCallback(
          [&](GuardedBufferFrame<BTreeNode>& guardedLeaf) {
            if (guardedLeaf->freeSpaceAfterCompaction() >=
                BTreeNode::UnderFullSize()) {
              iterator.cleanUpCallback([&, to_find = guardedLeaf.mBf] {
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
          auto& versionContainer =
              *reinterpret_cast<VersionMeta*>(iterator.MutableVal().data());
          const TREEID treeId = versionContainer.mTreeId;
          const bool called_before = versionContainer.called_before;
          versionContainer.called_before = true;
          keySize = iterator.key().length();
          std::memcpy(keyBuffer.get(), iterator.key().data(), keySize);
          payload_length = iterator.value().length() - sizeof(VersionMeta);
          std::memcpy(payload.get(), versionContainer.payload, payload_length);
          key = Slice(keyBuffer.get(), keySize + 1);
          iterator.removeCurrent();
          removed_versions = removed_versions + 1;
          iterator.MarkAsDirty();
          iterator.reset();
          cb(current_tx_id, treeId, payload.get(), payload_length,
             called_before);
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
  utils::fold(keyBuffer.get(), from_tx_id);

  // Attention: no cross worker gc in sync
  Session* volatile session = &update_sessions[workerId];
  volatile bool should_try = true;
  if (from_tx_id == 0) {
    JUMPMU_TRY() {
      if (session->leftmost_init) {
        BufferFrame* bf = session->leftmost_bf;
        HybridGuard bf_guard(bf->header.mLatch, session->leftmost_version);
        bf_guard.JumpIfModifiedByOthers();
        GuardedBufferFrame<BTreeNode> guardedLeaf(std::move(bf_guard), bf);

        if (guardedLeaf->mLowerFence.length == 0) {
          // Allocate in the stack, freed when the calling function exits.
          auto lastKey = utils::ScopedArray<u8>(
              guardedLeaf->getFullKeyLen(guardedLeaf->mNumSeps - 1));
          guardedLeaf->copyFullKey(guardedLeaf->mNumSeps - 1, lastKey.get());
          TXID last_key_tx_id;
          utils::unfold(lastKey.get(), last_key_tx_id);
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
      iterator.exitLeafCallback(
          [&](GuardedBufferFrame<BTreeNode>& guardedLeaf) {
            if (guardedLeaf->freeSpaceAfterCompaction() >=
                BTreeNode::UnderFullSize()) {
              iterator.cleanUpCallback([&, to_find = guardedLeaf.mBf] {
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
      iterator.enterLeafCallback(
          [&](GuardedBufferFrame<BTreeNode>& guardedLeaf) {
            if (guardedLeaf->mNumSeps == 0) {
              return;
            }
            // Allocate in the stack, freed when the calling function exits.
            auto firstKey =
                utils::ScopedArray<u8>(guardedLeaf->getFullKeyLen(0));
            guardedLeaf->copyFullKey(0, firstKey.get());
            TXID first_key_tx_id;
            utils::unfold(firstKey.get(), first_key_tx_id);

            // Allocate in the stack, freed when the calling function exits.
            auto lastKey = utils::ScopedArray<u8>(
                guardedLeaf->getFullKeyLen(guardedLeaf->mNumSeps - 1));
            guardedLeaf->copyFullKey(guardedLeaf->mNumSeps - 1, lastKey.get());
            TXID last_key_tx_id;
            utils::unfold(lastKey.get(), last_key_tx_id);
            if (first_key_tx_id >= from_tx_id && to_tx_id >= last_key_tx_id) {
              // Purge the whole page
              removed_versions = removed_versions + guardedLeaf->mNumSeps;
              guardedLeaf->reset();
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
  auto keySize = sizeof(to_tx_id);
  auto keyBuffer = utils::ScopedArray<u8>(FLAGS_page_size);
  u64 offset = 0;
  offset += utils::fold(keyBuffer.get() + offset, from_tx_id);
  Slice key(keyBuffer.get(), keySize);
  auto payload = utils::ScopedArray<u8>(FLAGS_page_size);
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
        auto& versionContainer =
            *reinterpret_cast<VersionMeta*>(iterator.MutableVal().data());
        const TREEID treeId = versionContainer.mTreeId;
        const bool called_before = versionContainer.called_before;
        ENSURE(called_before == false);
        versionContainer.called_before = true;
        keySize = iterator.key().length();
        std::memcpy(keyBuffer.get(), iterator.key().data(), keySize);
        payload_length = iterator.value().length() - sizeof(VersionMeta);
        std::memcpy(payload.get(), versionContainer.payload, payload_length);
        key = Slice(keyBuffer.get(), keySize + 1);
        if (!called_before) {
          iterator.MarkAsDirty();
        }
        iterator.reset();
        cb(current_tx_id, treeId, payload.get(), payload_length, called_before);
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
