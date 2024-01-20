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

void HistoryTree::PurgeVersions(WORKERID workerId, TXID fromTxId, TXID toTxId,
                                RemoveVersionCallback onRemoveVersion,
                                [[maybe_unused]] const u64 limit) {
  auto keySize = sizeof(toTxId);
  u8 keyBuffer[FLAGS_page_size];
  utils::fold(keyBuffer, fromTxId);
  Slice key(keyBuffer, keySize);

  u8 payload[FLAGS_page_size];
  u16 payloadSize;
  u64 versionsRemoved = 0;

  // purge remove versions
  auto* btree = mRemoveBTrees[workerId];
  JUMPMU_TRY() {
  restartrem : {
    BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(btree));
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
      // finished if we are out of the transaction range
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

      // set the next key to be seeked
      keySize = xIter.key().size();
      std::memcpy(keyBuffer, xIter.key().data(), keySize);
      key = Slice(keyBuffer, keySize + 1);

      // get the remove version
      payloadSize = xIter.value().size() - sizeof(VersionMeta);
      std::memcpy(payload, versionContainer.payload, payloadSize);

      // remove the version from history
      xIter.RemoveCurrent();
      versionsRemoved = versionsRemoved + 1;
      xIter.MarkAsDirty();
      xIter.Reset();

      onRemoveVersion(curTxId, treeId, payload, payloadSize, calledBefore);
      goto restartrem;
    }
  }
  }
  JUMPMU_CATCH() {
    UNREACHABLE();
  }

  // purge update versions
  btree = mUpdateBTrees[workerId];
  utils::fold(keyBuffer, fromTxId);

  // Attention: no cross worker gc in sync
  Session* volatile session = &mUpdateSessions[workerId];
  volatile bool shouldTry = true;
  if (fromTxId == 0 && session->leftmost_init) {
    JUMPMU_TRY() {
      BufferFrame* bf = session->leftmost_bf;
      HybridGuard bfGuard(bf->header.mLatch, session->leftmost_version);
      bfGuard.JumpIfModifiedByOthers();
      GuardedBufferFrame<BTreeNode> guardedLeaf(std::move(bfGuard), bf);

      if (guardedLeaf->mLowerFence.length == 0) {
        auto lastKeySize =
            guardedLeaf->getFullKeyLen(guardedLeaf->mNumSeps - 1);
        u8 lastKey[lastKeySize];
        guardedLeaf->copyFullKey(guardedLeaf->mNumSeps - 1, lastKey);
        TXID txIdInLastkey;
        utils::unfold(lastKey, txIdInLastkey);
        if (txIdInLastkey > toTxId) {
          shouldTry = false;
        }
      }
    }
    JUMPMU_CATCH() {
    }
  }

  while (shouldTry) {
    JUMPMU_TRY() {
      BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(btree));
      // check whether the page can be merged when exit a leaf
      xIter.SetExitLeafCallback(
          [&](GuardedBufferFrame<BTreeNode>& guardedLeaf) {
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

      bool isFullPagePurged = false;
      // check whether the whole page can be purged when enter a leaf
      xIter.SetEnterLeafCallback(
          [&](GuardedBufferFrame<BTreeNode>& guardedLeaf) {
            if (guardedLeaf->mNumSeps == 0) {
              return;
            }

            // get the transaction id in the first key
            auto firstKeySize = guardedLeaf->getFullKeyLen(0);
            u8 firstKey[firstKeySize];
            guardedLeaf->copyFullKey(0, firstKey);
            TXID txIdInFirstKey;
            utils::unfold(firstKey, txIdInFirstKey);

            // get the transaction id in the last key
            auto lastKeySize =
                guardedLeaf->getFullKeyLen(guardedLeaf->mNumSeps - 1);
            u8 lastKey[lastKeySize];
            guardedLeaf->copyFullKey(guardedLeaf->mNumSeps - 1, lastKey);
            TXID txIdInLastKey;
            utils::unfold(lastKey, txIdInLastKey);

            // purge the whole page if it is in the range
            if (fromTxId <= txIdInFirstKey && txIdInLastKey <= toTxId) {
              versionsRemoved += guardedLeaf->mNumSeps;
              guardedLeaf->Reset();
              isFullPagePurged = true;
            }
          });

      xIter.Seek(key);
      if (isFullPagePurged) {
        isFullPagePurged = false;
        JUMPMU_CONTINUE;
      }
      session->leftmost_bf = xIter.mGuardedLeaf.mBf;
      session->leftmost_version = xIter.mGuardedLeaf.mGuard.mVersion + 1;
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
