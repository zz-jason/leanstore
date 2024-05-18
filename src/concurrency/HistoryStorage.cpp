#include "concurrency/HistoryStorage.hpp"

#include "btree/BasicKV.hpp"
#include "btree/core/BTreeNode.hpp"
#include "btree/core/BTreePessimisticExclusiveIterator.hpp"
#include "btree/core/BTreePessimisticSharedIterator.hpp"
#include "leanstore/Units.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "sync/HybridLatch.hpp"
#include "sync/ScopedHybridGuard.hpp"
#include "utils/Log.hpp"
#include "utils/Misc.hpp"
#include "utils/UserThread.hpp"

#include <functional>

using namespace leanstore::storage::btree;

namespace leanstore::cr {

void HistoryStorage::PutVersion(TXID txId, COMMANDID commandId, TREEID treeId,
                                bool isRemove, uint64_t versionSize,
                                std::function<void(uint8_t*)> insertCallBack,
                                bool sameThread) {
  // Compose the key to be inserted
  auto* btree = isRemove ? mRemoveIndex : mUpdateIndex;
  auto keySize = sizeof(txId) + sizeof(commandId);
  uint8_t keyBuffer[keySize];
  uint64_t offset = 0;
  offset += utils::Fold(keyBuffer + offset, txId);
  offset += utils::Fold(keyBuffer + offset, commandId);
  Slice key(keyBuffer, keySize);
  versionSize += sizeof(VersionMeta);

  Session* session = nullptr;
  if (sameThread) {
    session = (isRemove) ? &mRemoveSession : &mUpdateSession;
  }
  if (session != nullptr && session->mRightmostBf != nullptr) {
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
        insertCallBack(versionMeta.mPayload);
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
      insertCallBack(versionMeta.mPayload);

      if (session != nullptr) {
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

bool HistoryStorage::GetVersion(
    TXID newerTxId, COMMANDID newerCommandId, const bool isRemoveCommand,
    std::function<void(const uint8_t*, uint64_t)> cb) {
  volatile BasicKV* btree = (isRemoveCommand) ? mRemoveIndex : mUpdateIndex;
  const uint64_t keySize = sizeof(newerTxId) + sizeof(newerCommandId);
  uint8_t keyBuffer[keySize];
  uint64_t offset = 0;
  offset += utils::Fold(keyBuffer + offset, newerTxId);
  offset += utils::Fold(keyBuffer + offset, newerCommandId);

  Slice key(keyBuffer, keySize);
  JUMPMU_TRY() {
    auto iter = const_cast<BasicKV*>(btree)->GetIterator();
    if (!iter.SeekExact(key)) {
      JUMPMU_RETURN false;
    }
    Slice payload = iter.value();
    const auto& versionContainer = *VersionMeta::From(payload.data());
    cb(versionContainer.mPayload, payload.length() - sizeof(VersionMeta));
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
  uint8_t keyBuffer[utils::tlsStore->mStoreOption.mPageSize];
  utils::Fold(keyBuffer, fromTxId);
  Slice key(keyBuffer, keySize);

  uint8_t payload[utils::tlsStore->mStoreOption.mPageSize];
  uint16_t payloadSize;
  uint64_t versionsRemoved = 0;

  // purge remove versions
  auto* btree = mRemoveIndex;
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
      const bool calledBefore = versionContainer.mCalledBefore;
      versionContainer.mCalledBefore = true;

      // set the next key to be seeked
      keySize = xIter.key().size();
      std::memcpy(keyBuffer, xIter.key().data(), keySize);
      key = Slice(keyBuffer, keySize + 1);

      // get the remove version
      payloadSize = xIter.value().size() - sizeof(VersionMeta);
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
      BufferFrame* bf = session->mLeftMostBf;

      // optimistic lock, jump if invalid
      ScopedHybridGuard bfGuard(bf->mHeader.mLatch, session->mLeftMostVersion);

      // lock successfull, check whether the page can be purged
      auto* leafNode = reinterpret_cast<BTreeNode*>(bf->mPage.mPayload);
      if (leafNode->mLowerFence.mLength == 0 && leafNode->mNumSeps > 0) {
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
      session->mLeftMostBf = xIter.mGuardedLeaf.mBf;
      session->mLeftMostVersion = xIter.mGuardedLeaf.mGuard.mVersion + 1;
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

void HistoryStorage::VisitRemovedVersions(
    TXID fromTxId, TXID toTxId, RemoveVersionCallback onRemoveVersion) {
  auto* removeTree = mRemoveIndex;
  auto keySize = sizeof(toTxId);
  uint8_t keyBuffer[utils::tlsStore->mStoreOption.mPageSize];

  uint64_t offset = 0;
  offset += utils::Fold(keyBuffer + offset, fromTxId);
  Slice key(keyBuffer, keySize);
  uint8_t payload[utils::tlsStore->mStoreOption.mPageSize];
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
      const bool calledBefore = versionContainer.mCalledBefore;
      LS_DCHECK(
          calledBefore == false,
          "Each remove version should be visited only once, treeId={}, txId={}",
          treeId, curTxId);

      versionContainer.mCalledBefore = true;

      // set the next key to be seeked
      keySize = xIter.key().length();
      std::memcpy(keyBuffer, xIter.key().data(), keySize);
      key = Slice(keyBuffer, keySize + 1);

      // get the remove version
      payloadSize = xIter.value().length() - sizeof(VersionMeta);
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
