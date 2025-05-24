#include "leanstore/concurrency/HistoryStorage.hpp"

#include "leanstore/Units.hpp"
#include "leanstore/btree/BasicKV.hpp"
#include "leanstore/btree/core/BTreeNode.hpp"
#include "leanstore/btree/core/PessimisticExclusiveIterator.hpp"
#include "leanstore/sync/HybridLatch.hpp"
#include "leanstore/sync/ScopedHybridGuard.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/UserThread.hpp"

#include <functional>

using namespace leanstore::storage::btree;

namespace leanstore::cr {

void HistoryStorage::PutVersion(TXID txId, COMMANDID commandId, TREEID treeId, bool isRemove,
                                uint64_t versionSize, std::function<void(uint8_t*)> insertCallBack,
                                bool sameThread) {
  // Compose the key to be inserted
  auto* btree = isRemove ? mRemoveIndex : mUpdateIndex;
  auto keySize = sizeof(txId) + sizeof(commandId);
  uint8_t keyBuffer[keySize];
  uint64_t offset = 0;
  offset += utils::Fold(keyBuffer + offset, txId);
  offset += utils::Fold(keyBuffer + offset, commandId);
  versionSize += sizeof(VersionMeta);

  Session* session = nullptr;
  if (sameThread) {
    session = (isRemove) ? &mRemoveSession : &mUpdateSession;
  }
  if (session != nullptr && session->mRightmostBf != nullptr) {
    JUMPMU_TRY() {
      Slice key(keyBuffer, keySize);
      PessimisticExclusiveIterator xIter(*static_cast<BTreeGeneric*>(const_cast<BasicKV*>(btree)),
                                         session->mRightmostBf, session->mRightmostVersion);
      if (xIter.HasEnoughSpaceFor(key.size(), versionSize) && xIter.KeyInCurrentNode(key)) {

        if (session->mLastTxId == txId) {
          // Only need to keep one version for each txId?
          xIter.mGuardedLeaf->InsertDoNotCopyPayload(key, versionSize, session->mRightmostPos);
          xIter.mSlotId = session->mRightmostPos;
        } else {
          xIter.InsertToCurrentNode(key, versionSize);
        }

        auto& versionMeta = *new (xIter.MutableVal().Data()) VersionMeta();
        versionMeta.mTreeId = treeId;
        insertCallBack(versionMeta.mPayload);
        xIter.mGuardedLeaf.unlock();
        JUMPMU_RETURN;
      }
    }
    JUMPMU_CATCH() {
    }
  }

  while (true) {
    JUMPMU_TRY() {
      Slice key(keyBuffer, keySize);
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
      insertCallBack(versionMeta.mPayload);

      if (session != nullptr) {
        session->mRightmostBf = xIter.mGuardedLeaf.mBf;
        session->mRightmostVersion = xIter.mGuardedLeaf.mGuard.mVersion + 1;
        session->mRightmostPos = xIter.mSlotId + 1;
        session->mLastTxId = txId;
      }

      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
    }
  }
}

bool HistoryStorage::GetVersion(TXID newerTxId, COMMANDID newerCommandId,
                                const bool isRemoveCommand,
                                std::function<void(const uint8_t*, uint64_t)> cb) {
  volatile BasicKV* btree = (isRemoveCommand) ? mRemoveIndex : mUpdateIndex;
  const uint64_t keySize = sizeof(newerTxId) + sizeof(newerCommandId);
  uint8_t keyBuffer[keySize];
  uint64_t offset = 0;
  offset += utils::Fold(keyBuffer + offset, newerTxId);
  offset += utils::Fold(keyBuffer + offset, newerCommandId);

  JUMPMU_TRY() {
    BasicKV* kv = const_cast<BasicKV*>(btree);
    auto ret = kv->Lookup(Slice(keyBuffer, keySize), [&](const Slice& payload) {
      const auto& versionContainer = *VersionMeta::From(payload.data());
      cb(versionContainer.mPayload, payload.length() - sizeof(VersionMeta));
    });

    if (ret == OpCode::kNotFound) {
      JUMPMU_RETURN false;
    }
    JUMPMU_RETURN true;
  }
  JUMPMU_CATCH() {
    Log::Error("Can not retrieve older version"
               ", newerTxId: {}, newerCommandId: {}, isRemoveCommand: {}",
               newerTxId, newerCommandId, isRemoveCommand);
  }
  UNREACHABLE();
  return false;
}

void HistoryStorage::PurgeVersions(TXID fromTxId, TXID toTxId,
                                   RemoveVersionCallback onRemoveVersion,
                                   [[maybe_unused]] const uint64_t limit) {
  auto keySize = sizeof(toTxId);
  uint8_t keyBuffer[utils::tlsStore->mStoreOption->mPageSize];
  utils::Fold(keyBuffer, fromTxId);
  Slice key(keyBuffer, keySize);

  uint8_t payload[utils::tlsStore->mStoreOption->mPageSize];
  uint16_t payloadSize;
  uint64_t versionsRemoved = 0;

  // purge remove versions
  auto* btree = mRemoveIndex;
  JUMPMU_TRY() {
  restartrem: {
    auto xIter = btree->GetExclusiveIterator();
    xIter.SetExitLeafCallback([&](leanstore::storage::GuardedBufferFrame<BTreeNode>& guardedLeaf) {
      if (guardedLeaf->FreeSpaceAfterCompaction() >= BTreeNode::UnderFullSize()) {
        xIter.SetCleanUpCallback([&, toMerge = guardedLeaf.mBf] {
          JUMPMU_TRY() {
            TXID sysTxId = btree->mStore->AllocSysTxTs();
            btree->TryMergeMayJump(sysTxId, *toMerge);
          }
          JUMPMU_CATCH() {
          }
        });
      }
    });
    for (xIter.SeekToFirstGreaterEqual(key); xIter.Valid(); xIter.SeekToFirstGreaterEqual(key)) {
      // finished if we are out of the transaction range
      xIter.AssembleKey();
      TXID curTxId;
      utils::Unfold(xIter.Key().data(), curTxId);
      if (curTxId < fromTxId || curTxId > toTxId) {
        break;
      }

      auto& versionContainer = *reinterpret_cast<VersionMeta*>(xIter.MutableVal().Data());
      const TREEID treeId = versionContainer.mTreeId;
      const bool calledBefore = versionContainer.mCalledBefore;
      versionContainer.mCalledBefore = true;

      // set the next key to be seeked
      keySize = xIter.Key().size();
      std::memcpy(keyBuffer, xIter.Key().data(), keySize);
      key = Slice(keyBuffer, keySize + 1);

      // get the remove version
      payloadSize = xIter.Val().size() - sizeof(VersionMeta);
      std::memcpy(payload, versionContainer.mPayload, payloadSize);

      // remove the version from history
      xIter.RemoveCurrent();
      versionsRemoved = versionsRemoved + 1;
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
  btree = mUpdateIndex;
  utils::Fold(keyBuffer, fromTxId);

  // Attention: no cross worker gc in sync
  Session* volatile session = &mUpdateSession;
  volatile bool shouldTry = true;
  if (fromTxId == 0 && session->mLeftMostBf != nullptr) {
    JUMPMU_TRY() {
      leanstore::storage::BufferFrame* bf = session->mLeftMostBf;

      // optimistic lock, jump if invalid
      leanstore::storage::ScopedHybridGuard bfGuard(bf->mHeader.mLatch, session->mLeftMostVersion);

      // lock successfull, check whether the page can be purged
      auto* leafNode = reinterpret_cast<BTreeNode*>(bf->mPage.mPayload);
      if (leafNode->mLowerFence.IsInfinity() && leafNode->mNumSlots > 0) {
        auto lastKeySize = leafNode->GetFullKeyLen(leafNode->mNumSlots - 1);
        uint8_t lastKey[lastKeySize];
        leafNode->CopyFullKey(leafNode->mNumSlots - 1, lastKey);

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
          [&](leanstore::storage::GuardedBufferFrame<BTreeNode>& guardedLeaf) {
            if (guardedLeaf->FreeSpaceAfterCompaction() >= BTreeNode::UnderFullSize()) {
              xIter.SetCleanUpCallback([&, toMerge = guardedLeaf.mBf] {
                JUMPMU_TRY() {
                  TXID sysTxId = btree->mStore->AllocSysTxTs();
                  btree->TryMergeMayJump(sysTxId, *toMerge);
                }
                JUMPMU_CATCH() {
                }
              });
            }
          });

      bool isFullPagePurged = false;
      // check whether the whole page can be purged when enter a leaf
      xIter.SetEnterLeafCallback(
          [&](leanstore::storage::GuardedBufferFrame<BTreeNode>& guardedLeaf) {
            if (guardedLeaf->mNumSlots == 0) {
              return;
            }

            // get the transaction id in the first key
            auto firstKeySize = guardedLeaf->GetFullKeyLen(0);
            uint8_t firstKey[firstKeySize];
            guardedLeaf->CopyFullKey(0, firstKey);
            TXID txIdInFirstKey;
            utils::Unfold(firstKey, txIdInFirstKey);

            // get the transaction id in the last key
            auto lastKeySize = guardedLeaf->GetFullKeyLen(guardedLeaf->mNumSlots - 1);
            uint8_t lastKey[lastKeySize];
            guardedLeaf->CopyFullKey(guardedLeaf->mNumSlots - 1, lastKey);
            TXID txIdInLastKey;
            utils::Unfold(lastKey, txIdInLastKey);

            // purge the whole page if it is in the range
            if (fromTxId <= txIdInFirstKey && txIdInLastKey <= toTxId) {
              versionsRemoved += guardedLeaf->mNumSlots;
              guardedLeaf->Reset();
              isFullPagePurged = true;
            }
          });

      xIter.SeekToFirstGreaterEqual(key);
      if (isFullPagePurged) {
        isFullPagePurged = false;
        JUMPMU_CONTINUE;
      }
      session->mLeftMostBf = xIter.mGuardedLeaf.mBf;
      session->mLeftMostVersion = xIter.mGuardedLeaf.mGuard.mVersion + 1;
      JUMPMU_BREAK;
    }
    JUMPMU_CATCH() {
      UNREACHABLE();
    }
  }
}

void HistoryStorage::VisitRemovedVersions(TXID fromTxId, TXID toTxId,
                                          RemoveVersionCallback onRemoveVersion) {
  auto* removeTree = mRemoveIndex;
  auto keySize = sizeof(toTxId);
  uint8_t keyBuffer[utils::tlsStore->mStoreOption->mPageSize];

  uint64_t offset = 0;
  offset += utils::Fold(keyBuffer + offset, fromTxId);
  Slice key(keyBuffer, keySize);
  uint8_t payload[utils::tlsStore->mStoreOption->mPageSize];
  uint16_t payloadSize;

  JUMPMU_TRY() {
  restart: {
    auto xIter = removeTree->GetExclusiveIterator();
    for (xIter.SeekToFirstGreaterEqual(key); xIter.Valid(); xIter.SeekToFirstGreaterEqual(key)) {
      // skip versions out of the transaction range
      xIter.AssembleKey();
      TXID curTxId;
      utils::Unfold(xIter.Key().data(), curTxId);
      if (curTxId < fromTxId || curTxId > toTxId) {
        break;
      }

      auto& versionContainer = *VersionMeta::From(xIter.MutableVal().Data());
      const TREEID treeId = versionContainer.mTreeId;
      const bool calledBefore = versionContainer.mCalledBefore;
      LS_DCHECK(calledBefore == false,
                "Each remove version should be visited only once, treeId={}, txId={}", treeId,
                curTxId);

      versionContainer.mCalledBefore = true;

      // set the next key to be seeked
      keySize = xIter.Key().length();
      std::memcpy(keyBuffer, xIter.Key().data(), keySize);
      key = Slice(keyBuffer, keySize + 1);

      // get the remove version
      payloadSize = xIter.Val().length() - sizeof(VersionMeta);
      std::memcpy(payload, versionContainer.mPayload, payloadSize);

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
