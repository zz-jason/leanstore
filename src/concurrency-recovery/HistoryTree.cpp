#include "HistoryTree.hpp"

#include "leanstore/Units.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "btree/BasicKV.hpp"
#include "btree/core/BTreeNode.hpp"
#include "btree/core/BTreePessimisticExclusiveIterator.hpp"
#include "btree/core/BTreePessimisticSharedIterator.hpp"
#include "sync/HybridLatch.hpp"
#include "sync/ScopedHybridGuard.hpp"
#include "utils/Misc.hpp"

#include <functional>

using namespace leanstore::storage::btree;

namespace leanstore::cr {

void HistoryTree::PutVersion(WORKERID workerId, TXID txId, COMMANDID commandId,
                             TREEID treeId, bool isRemove, uint64_t versionSize,
                             std::function<void(uint8_t*)> insertCallBack,
                             bool sameThread) {
  // Compose the key to be inserted
  auto keySize = sizeof(txId) + sizeof(commandId);
  uint8_t keyBuffer[keySize];
  uint64_t offset = 0;
  offset += utils::Fold(keyBuffer + offset, txId);
  offset += utils::Fold(keyBuffer + offset, commandId);
  Slice key(keyBuffer, keySize);

  versionSize += sizeof(VersionMeta);

  volatile auto* btree =
      (isRemove) ? mRemoveBTrees[workerId] : mUpdateBTrees[workerId];
  Session* session = nullptr;
  if (sameThread) {
    session =
        (isRemove) ? &mRemoveSessions[workerId] : &mUpdateSessions[workerId];
  }
  if (session != nullptr && session->mRightmostInited) {
    JUMPMU_TRY() {
      BTreePessimisticExclusiveIterator xIter(
          *static_cast<BTreeGeneric*>(const_cast<BasicKV*>(btree)),
          session->mRightmostBf, session->mRightmostVersion);
      if (xIter.HasEnoughSpaceFor(key.size(), versionSize) &&
          xIter.KeyInCurrentNode(key)) {

        if (session->mLastTxId == txId) {
          // Only need to keep one version for each txId?
          xIter.mGuardedLeaf->insertDoNotCopyPayload(key, versionSize,
                                                     session->mRightmostPos);
          xIter.mSlotId = session->mRightmostPos;
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
      auto xIter = const_cast<BasicKV*>(btree)->GetExclusiveIterator();
      OpCode ret = xIter.SeekToInsert(key);
      if (ret == OpCode::kDuplicated) {
        // remove the last inserted version for the key
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
        session->mRightmostInited = true;
        session->mRightmostBf = xIter.mGuardedLeaf.mBf;
        session->mRightmostVersion = xIter.mGuardedLeaf.mGuard.mVersion + 1;
        session->mRightmostPos = xIter.mSlotId + 1;
        session->mLastTxId = txId;
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
                             std::function<void(const uint8_t*, uint64_t)> cb) {
  volatile BasicKV* btree = (isRemoveCommand) ? mRemoveBTrees[prevWorkerId]
                                              : mUpdateBTrees[prevWorkerId];
  const uint64_t keySize = sizeof(prevTxId) + sizeof(prevCommandId);
  uint8_t keyBuffer[keySize];
  uint64_t offset = 0;
  offset += utils::Fold(keyBuffer + offset, prevTxId);
  offset += utils::Fold(keyBuffer + offset, prevCommandId);

  Slice key(keyBuffer, keySize);
  JUMPMU_TRY() {
    auto iter = const_cast<BasicKV*>(btree)->GetIterator();
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
                                [[maybe_unused]] const uint64_t limit) {
  auto keySize = sizeof(toTxId);
  uint8_t keyBuffer[FLAGS_page_size];
  utils::Fold(keyBuffer, fromTxId);
  Slice key(keyBuffer, keySize);

  uint8_t payload[FLAGS_page_size];
  uint16_t payloadSize;
  uint64_t versionsRemoved = 0;

  // purge remove versions
  auto* btree = mRemoveBTrees[workerId];
  JUMPMU_TRY() {
  restartrem : {
    auto xIter = btree->GetExclusiveIterator();
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
      utils::Unfold(xIter.key().data(), curTxId);
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
  utils::Fold(keyBuffer, fromTxId);

  // Attention: no cross worker gc in sync
  Session* volatile session = &mUpdateSessions[workerId];
  volatile bool shouldTry = true;
  if (fromTxId == 0 && session->leftmost_init) {
    JUMPMU_TRY() {
      BufferFrame* bf = session->leftmost_bf;

      // optimistic lock, jump if invalid
      ScopedHybridGuard bfGuard(bf->header.mLatch, session->leftmost_version);

      // lock successfull, check whether the page can be purged
      auto* leafNode = reinterpret_cast<BTreeNode*>(bf->page.mPayload);
      if (leafNode->mLowerFence.length == 0) {
        auto lastKeySize = leafNode->getFullKeyLen(leafNode->mNumSeps - 1);
        uint8_t lastKey[lastKeySize];
        leafNode->copyFullKey(leafNode->mNumSeps - 1, lastKey);

        // optimistic unlock, jump if invalid
        bfGuard.Unlock();

        // now we can safely use the copied key
        TXID txIdInLastkey;
        utils::Unfold(lastKey, txIdInLastkey);
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
      auto xIter = btree->GetExclusiveIterator();
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
            uint8_t firstKey[firstKeySize];
            guardedLeaf->copyFullKey(0, firstKey);
            TXID txIdInFirstKey;
            utils::Unfold(firstKey, txIdInFirstKey);

            // get the transaction id in the last key
            auto lastKeySize =
                guardedLeaf->getFullKeyLen(guardedLeaf->mNumSeps - 1);
            uint8_t lastKey[lastKeySize];
            guardedLeaf->copyFullKey(guardedLeaf->mNumSeps - 1, lastKey);
            TXID txIdInLastKey;
            utils::Unfold(lastKey, txIdInLastKey);

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
                                       TXID toTxId,
                                       RemoveVersionCallback onRemoveVersion) {
  auto* removeTree = mRemoveBTrees[workerId];
  auto keySize = sizeof(toTxId);
  uint8_t keyBuffer[FLAGS_page_size];

  uint64_t offset = 0;
  offset += utils::Fold(keyBuffer + offset, fromTxId);
  Slice key(keyBuffer, keySize);
  uint8_t payload[FLAGS_page_size];
  uint16_t payloadSize;

  JUMPMU_TRY() {
  restart : {
    auto xIter = removeTree->GetExclusiveIterator();
    while (xIter.Seek(key)) {
      // skip versions out of the transaction range
      xIter.AssembleKey();
      TXID curTxId;
      utils::Unfold(xIter.key().data(), curTxId);
      if (curTxId < fromTxId || curTxId > toTxId) {
        break;
      }

      auto& versionContainer = *VersionMeta::From(xIter.MutableVal().Data());
      const TREEID treeId = versionContainer.mTreeId;
      const bool calledBefore = versionContainer.called_before;
      DCHECK(calledBefore == false)
          << "Each remove version should be visited only once"
          << ", workerId=" << workerId << ", treeId=" << treeId
          << ", txId=" << curTxId;

      versionContainer.called_before = true;

      // set the next key to be seeked
      keySize = xIter.key().length();
      std::memcpy(keyBuffer, xIter.key().data(), keySize);
      key = Slice(keyBuffer, keySize + 1);

      // get the remove version
      payloadSize = xIter.value().length() - sizeof(VersionMeta);
      std::memcpy(payload, versionContainer.payload, payloadSize);
      if (!calledBefore) {
        xIter.MarkAsDirty();
      }

      xIter.Reset();
      onRemoveVersion(curTxId, treeId, payload, payloadSize, calledBefore);
      goto restart;
    }
  }
  }
  JUMPMU_CATCH() {
  }
}

} // namespace leanstore::cr
