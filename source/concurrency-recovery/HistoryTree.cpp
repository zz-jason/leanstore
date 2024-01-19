#include "HistoryTree.hpp"

#include "profiling/counters/CRCounters.hpp"
#include "shared-headers/Units.hpp"
#include "storage/btree/core/BTreeExclusiveIterator.hpp"
#include "storage/btree/core/BTreeSharedIterator.hpp"
#include "utils/Misc.hpp"

#include <functional>

using namespace leanstore::storage::btree;

namespace leanstore::cr {

void HistoryTree::PutVersion(WORKERID workerId, TXID txId, COMMANDID commandId,
                             TREEID treeId, bool isRemove, u64 versionSize,
                             std::function<void(u8*)> insertCallBack,
                             bool sameThread) {
  if (!FLAGS_history_tree_inserts) {
    return;
  }
  const u64 keySize = sizeof(txId) + sizeof(commandId);
  u8 keyBuffer[keySize];
  u64 offset = 0;
  offset += utils::fold(keyBuffer + offset, txId);
  offset += utils::fold(keyBuffer + offset, commandId);
  Slice key(keyBuffer, keySize);
  versionSize += sizeof(VersionMeta);

  BasicKV* btree =
      (isRemove) ? mRemoveBTrees[workerId] : mUpdateBTrees[workerId];
  Session* session = nullptr;
  if (sameThread) {
    session =
        (isRemove) ? &mRemoveSessions[workerId] : &mUpdateSessions[workerId];
  }
  if (session != nullptr && session->rightmost_init) {
    JUMPMU_TRY() {
      BTreeExclusiveIterator xIter(
          *static_cast<BTreeGeneric*>(const_cast<BasicKV*>(btree)),
          session->rightmost_bf, session->rightmost_version);
      if (xIter.HasEnoughSpaceFor(key.size(), versionSize) &&
          xIter.KeyInCurrentNode(key)) {
        if (session->last_tx_id == txId) {
          xIter.mGuardedLeaf->insertDoNotCopyPayload(key, versionSize,
                                                     session->rightmost_pos);
          xIter.mSlotId = session->rightmost_pos;
        } else {
          xIter.InsertToCurrentNode(key, versionSize);
        }
        auto& versionMeta = *new (xIter.MutableVal().Data()) VersionMeta();
        versionMeta.mTreeId = treeId;
        insertCallBack(versionMeta.payload);
        xIter.MarkAsDirty();
        COUNTERS_BLOCK() {
          WorkerCounters::MyCounters().cc_versions_space_inserted_opt[treeId]++;
        }
        xIter.mGuardedLeaf.unlock();
        JUMPMU_RETURN;
      }
    }
    JUMPMU_CATCH() {
    }
  }

  while (true) {
    JUMPMU_TRY() {
      BTreeExclusiveIterator xIter(
          *static_cast<BTreeGeneric*>(const_cast<BasicKV*>(btree)));

      OpCode ret = xIter.SeekToInsert(key);
      if (ret == OpCode::kDuplicated) {
        xIter.RemoveCurrent();
      } else {
        ENSURE(ret == OpCode::kOK);
      }
      if (!xIter.HasEnoughSpaceFor(key.size(), versionSize)) {
        xIter.SplitForKey(key);
        JUMPMU_CONTINUE;
      }
      xIter.InsertToCurrentNode(key, versionSize);
      auto& versionMeta = *new (xIter.MutableVal().Data()) VersionMeta();
      versionMeta.mTreeId = treeId;
      insertCallBack(versionMeta.payload);
      xIter.MarkAsDirty();

      if (session != nullptr) {
        session->rightmost_bf = xIter.mGuardedLeaf.mBf;
        session->rightmost_version = xIter.mGuardedLeaf.mGuard.mVersion + 1;
        session->rightmost_pos = xIter.mSlotId + 1;
        session->last_tx_id = txId;
        session->rightmost_init = true;
      }

      COUNTERS_BLOCK() {
        WorkerCounters::MyCounters().cc_versions_space_inserted[treeId]++;
      }
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
    }
  }
}

bool HistoryTree::GetVersion(WORKERID prevWorkerId, TXID prevTxId,
                             COMMANDID prevCommandId,
                             const bool isRemoveCommand,
                             std::function<void(const u8*, u64)> cb) {
  BasicKV* btree = (isRemoveCommand) ? mRemoveBTrees[prevWorkerId]
                                     : mUpdateBTrees[prevWorkerId];
  const u64 keySize = sizeof(prevTxId) + sizeof(prevCommandId);
  u8 keyBuffer[keySize];
  u64 offset = 0;
  offset += utils::fold(keyBuffer + offset, prevTxId);
  offset += utils::fold(keyBuffer + offset, prevCommandId);

  Slice key(keyBuffer, keySize);
  JUMPMU_TRY() {
    BTreeSharedIterator iter(
        *static_cast<BTreeGeneric*>(const_cast<BasicKV*>(btree)),
        LatchMode::kShared);
    if (!iter.SeekExact(key)) {
      JUMPMU_RETURN false;
    }
    Slice payload = iter.value();
    const auto& versionContainer = *VersionMeta::From(payload.data());
    cb(versionContainer.payload, payload.length() - sizeof(VersionMeta));
    JUMPMU_RETURN true;
  }
  JUMPMU_CATCH() {
    LOG(ERROR) << "Can not retrieve version"
               << ", prevWorkerId: " << prevWorkerId
               << ", prevTxId: " << prevTxId
               << ", prevCommandId: " << prevCommandId
               << ", isRemoveCommand: " << isRemoveCommand;
  }
  UNREACHABLE();
  return false;
}

// [from, to]
void HistoryTree::PurgeVersions(WORKERID workerId, TXID fromTxId, TXID toTxId,
                                RemoveVersionCallback cb,
                                [[maybe_unused]] const u64 limit) {
  auto keySize = sizeof(toTxId);
  auto keyBuffer = utils::JumpScopedArray<u8>(FLAGS_page_size);
  utils::fold(keyBuffer->get(), fromTxId);
  Slice key(keyBuffer->get(), keySize);
  auto payload = utils::JumpScopedArray<u8>(FLAGS_page_size);
  u16 payloadSize;
  u64 versionsRemoved = 0;
  auto* btree = mRemoveBTrees[workerId];

  JUMPMU_TRY() {
  restartrem : {
    leanstore::storage::btree::BTreeExclusiveIterator xIter(
        *static_cast<BTreeGeneric*>(const_cast<BasicKV*>(btree)));
    xIter.SetExitLeafCallback([&](GuardedBufferFrame<BTreeNode>& guardedLeaf) {
      if (guardedLeaf->FreeSpaceAfterCompaction() >=
          BTreeNode::UnderFullSize()) {
        xIter.SetCleanUpCallback([&, toMerge = guardedLeaf.mBf] {
          JUMPMU_TRY() {
            btree->TryMergeMayJump(*toMerge);
          }
          JUMPMU_CATCH() {
          }
        });
      }
    });
    while (xIter.Seek(key)) {
      xIter.AssembleKey();
      TXID curTxId;
      utils::unfold(xIter.key().data(), curTxId);
      if (curTxId < fromTxId || curTxId > toTxId) {
        break;
      }

      auto& versionContainer =
          *reinterpret_cast<VersionMeta*>(xIter.MutableVal().Data());
      const TREEID treeId = versionContainer.mTreeId;
      const bool calledBefore = versionContainer.called_before;
      versionContainer.called_before = true;
      keySize = xIter.key().size();
      std::memcpy(keyBuffer->get(), xIter.key().data(), keySize);
      payloadSize = xIter.value().size() - sizeof(VersionMeta);
      std::memcpy(payload->get(), versionContainer.payload, payloadSize);
      key = Slice(keyBuffer->get(), keySize + 1);
      xIter.RemoveCurrent();
      versionsRemoved = versionsRemoved + 1;
      xIter.MarkAsDirty();
      xIter.Reset();
      cb(curTxId, treeId, payload->get(), payloadSize, calledBefore);
      goto restartrem;
    }
  }
  }
  JUMPMU_CATCH() {
    UNREACHABLE();
  }

  btree = mUpdateBTrees[workerId];
  utils::fold(keyBuffer->get(), fromTxId);

  // Attention: no cross worker gc in sync
  Session* volatile session = &mUpdateSessions[workerId];
  volatile bool should_try = true;
  if (fromTxId == 0) {
    JUMPMU_TRY() {
      if (session->leftmost_init) {
        BufferFrame* bf = session->leftmost_bf;
        HybridGuard bf_guard(bf->header.mLatch, session->leftmost_version);
        bf_guard.JumpIfModifiedByOthers();
        GuardedBufferFrame<BTreeNode> guardedLeaf(std::move(bf_guard), bf);

        if (guardedLeaf->mLowerFence.length == 0) {
          // Allocate in the stack, freed when the calling function exits.
          auto lastKey = utils::JumpScopedArray<u8>(
              guardedLeaf->getFullKeyLen(guardedLeaf->mNumSeps - 1));
          guardedLeaf->copyFullKey(guardedLeaf->mNumSeps - 1, lastKey->get());
          TXID last_key_tx_id;
          utils::unfold(lastKey->get(), last_key_tx_id);
          if (last_key_tx_id > toTxId) {
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
          *static_cast<BTreeGeneric*>(const_cast<BasicKV*>(btree)));
      iterator.SetExitLeafCallback(
          [&](GuardedBufferFrame<BTreeNode>& guardedLeaf) {
            if (guardedLeaf->FreeSpaceAfterCompaction() >=
                BTreeNode::UnderFullSize()) {
              iterator.SetCleanUpCallback([&, toMerge = guardedLeaf.mBf] {
                JUMPMU_TRY() {
                  btree->TryMergeMayJump(*toMerge);
                }
                JUMPMU_CATCH() {
                }
              });
            }
          });

      // ATTENTION: we use this also for purging the current aborted tx so we
      // can not simply assume fromTxId = 0
      bool did_purge_full_page = false;
      iterator.SetEnterLeafCallback(
          [&](GuardedBufferFrame<BTreeNode>& guardedLeaf) {
            if (guardedLeaf->mNumSeps == 0) {
              return;
            }
            // Allocate in the stack, freed when the calling function exits.
            auto firstKey =
                utils::JumpScopedArray<u8>(guardedLeaf->getFullKeyLen(0));
            guardedLeaf->copyFullKey(0, firstKey->get());
            TXID first_key_tx_id;
            utils::unfold(firstKey->get(), first_key_tx_id);

            // Allocate in the stack, freed when the calling function exits.
            auto lastKey = utils::JumpScopedArray<u8>(
                guardedLeaf->getFullKeyLen(guardedLeaf->mNumSeps - 1));
            guardedLeaf->copyFullKey(guardedLeaf->mNumSeps - 1, lastKey->get());
            TXID last_key_tx_id;
            utils::unfold(lastKey->get(), last_key_tx_id);
            if (first_key_tx_id >= fromTxId && toTxId >= last_key_tx_id) {
              // Purge the whole page
              versionsRemoved = versionsRemoved + guardedLeaf->mNumSeps;
              guardedLeaf->Reset();
              did_purge_full_page = true;
            }
          });

      iterator.Seek(key);
      if (did_purge_full_page) {
        did_purge_full_page = false;
        JUMPMU_CONTINUE;
      }
      session->leftmost_bf = iterator.mGuardedLeaf.mBf;
      session->leftmost_version = iterator.mGuardedLeaf.mGuard.mVersion + 1;
      session->leftmost_init = true;
      JUMPMU_BREAK;
    }
    JUMPMU_CATCH() {
      UNREACHABLE();
    }
  }
  COUNTERS_BLOCK() {
    CRCounters::MyCounters().cc_versions_space_removed += versionsRemoved;
  }
}

void HistoryTree::VisitRemovedVersions(WORKERID workerId, TXID fromTxId,
                                       TXID toTxId, RemoveVersionCallback cb) {
  auto* removeTree = mRemoveBTrees[workerId];
  auto keySize = sizeof(toTxId);
  auto keyBuffer = utils::JumpScopedArray<u8>(FLAGS_page_size);
  u64 offset = 0;
  offset += utils::fold(keyBuffer->get() + offset, fromTxId);
  Slice key(keyBuffer->get(), keySize);
  auto payload = utils::JumpScopedArray<u8>(FLAGS_page_size);
  u16 payloadSize;

  JUMPMU_TRY() {
  restart : {
    leanstore::storage::btree::BTreeExclusiveIterator iterator(
        *static_cast<BTreeGeneric*>(removeTree));
    while (iterator.Seek(key)) {
      iterator.AssembleKey();
      TXID curTxId;
      utils::unfold(iterator.key().data(), curTxId);
      if (curTxId >= fromTxId && curTxId <= toTxId) {
        auto& versionContainer =
            *reinterpret_cast<VersionMeta*>(iterator.MutableVal().Data());
        const TREEID treeId = versionContainer.mTreeId;
        const bool calledBefore = versionContainer.called_before;
        ENSURE(calledBefore == false);
        versionContainer.called_before = true;
        keySize = iterator.key().length();
        std::memcpy(keyBuffer->get(), iterator.key().data(), keySize);
        payloadSize = iterator.value().length() - sizeof(VersionMeta);
        std::memcpy(payload->get(), versionContainer.payload, payloadSize);
        key = Slice(keyBuffer->get(), keySize + 1);
        if (!calledBefore) {
          iterator.MarkAsDirty();
        }
        iterator.Reset();
        cb(curTxId, treeId, payload->get(), payloadSize, calledBefore);
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

} // namespace leanstore::cr
