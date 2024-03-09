#include "BTreeGeneric.hpp"

#include "btree/core/BTreeNode.hpp"
#include "btree/core/BTreePessimisticExclusiveIterator.hpp"
#include "btree/core/BTreePessimisticSharedIterator.hpp"
#include "btree/core/BTreeWALPayload.hpp"
#include "buffer-manager/BufferFrame.hpp"
#include "buffer-manager/BufferManager.hpp"
#include "buffer-manager/GuardedBufferFrame.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "utils/Misc.hpp"

#include <glog/logging.h>

using namespace leanstore::storage;

namespace leanstore::storage::btree {

void BTreeGeneric::Init(leanstore::LeanStore* store, TREEID btreeId,
                        BTreeConfig config) {
  this->mStore = store;
  this->mTreeId = btreeId;
  this->mConfig = config;

  mMetaNodeSwip = &mStore->mBufferManager->AllocNewPage(btreeId);
  mMetaNodeSwip.AsBufferFrame().mHeader.mKeepInMemory = true;
  DCHECK(mMetaNodeSwip.AsBufferFrame().mHeader.mLatch.GetOptimisticVersion() ==
         0);

  auto guardedRoot = GuardedBufferFrame<BTreeNode>(
      mStore->mBufferManager.get(),
      &mStore->mBufferManager->AllocNewPage(btreeId));
  auto xGuardedRoot =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedRoot));
  xGuardedRoot.InitPayload(true);

  auto guardedMeta = GuardedBufferFrame<BTreeNode>(mStore->mBufferManager.get(),
                                                   mMetaNodeSwip);
  auto xGuardedMeta = ExclusiveGuardedBufferFrame(std::move(guardedMeta));
  xGuardedMeta->mIsLeaf = false;
  xGuardedMeta->mRightMostChildSwip = xGuardedRoot.bf();

  // Record WAL
  if (mConfig.mEnableWal) {
    auto rootWalHandler = xGuardedRoot.ReserveWALPayload<WalInitPage>(
        0, mTreeId, xGuardedRoot->mIsLeaf);
    rootWalHandler.SubmitWal();

    auto metaWalHandler = xGuardedMeta.ReserveWALPayload<WalInitPage>(
        0, mTreeId, xGuardedMeta->mIsLeaf);
    metaWalHandler.SubmitWal();
  }
}

BTreePessimisticSharedIterator BTreeGeneric::GetIterator() {
  return BTreePessimisticSharedIterator(*this);
}

BTreePessimisticExclusiveIterator BTreeGeneric::GetExclusiveIterator() {
  return BTreePessimisticExclusiveIterator(*this);
}

void BTreeGeneric::TrySplitMayJump(BufferFrame& toSplit,
                                   int16_t favoredSplitPos) {
  auto parentHandler = findParentEager(*this, toSplit);
  GuardedBufferFrame<BTreeNode> guardedParent(
      mStore->mBufferManager.get(), std::move(parentHandler.mParentGuard),
      parentHandler.mParentBf);
  auto guardedChild = GuardedBufferFrame<BTreeNode>(
      mStore->mBufferManager.get(), guardedParent, parentHandler.mChildSwip);
  if (guardedChild->mNumSeps <= 1) {
    DLOG(WARNING) << "Split failed, not enough separators in node"
                  << ", toSplit.mHeader.mPageId=" << toSplit.mHeader.mPageId
                  << ", favoredSplitPos=" << favoredSplitPos
                  << ", guardedChild->mNumSeps=" << guardedChild->mNumSeps;
    return;
  }

  // init the separator info
  BTreeNode::SeparatorInfo sepInfo;
  if (favoredSplitPos < 0 || favoredSplitPos >= guardedChild->mNumSeps - 1) {
    if (mConfig.mUseBulkInsert) {
      favoredSplitPos = guardedChild->mNumSeps - 2;
      sepInfo = BTreeNode::SeparatorInfo{
          guardedChild->getFullKeyLen(favoredSplitPos),
          static_cast<uint16_t>(favoredSplitPos), false};
    } else {
      sepInfo = guardedChild->findSep();
    }
  } else {
    // Split on a specified position, used by contention management
    sepInfo =
        BTreeNode::SeparatorInfo{guardedChild->getFullKeyLen(favoredSplitPos),
                                 static_cast<uint16_t>(favoredSplitPos), false};
  }

  // split the root node
  if (isMetaNode(guardedParent)) {
    splitRootMayJump(guardedParent, guardedChild, sepInfo);
    return;
  }

  // calculate space needed for separator in parent node
  const uint16_t spaceNeededForSeparator =
      guardedParent->spaceNeeded(sepInfo.mSize, sizeof(Swip));

  // split the parent node to make zoom for separator
  if (!guardedParent->hasEnoughSpaceFor(spaceNeededForSeparator)) {
    guardedParent.unlock();
    guardedChild.unlock();
    TrySplitMayJump(*guardedParent.mBf);
    return;
  }

  // split the non-root node
  splitNonRootMayJump(guardedParent, guardedChild, sepInfo,
                      spaceNeededForSeparator);
}

/// Split the root node, 4 nodes are involved in the split:
///
///   meta(oldRoot) -> meta(newRoot(newLeft, oldRoot)).
///
/// meta         meta
///   |            |
/// oldRoot      newRoot
///              |     |
///           newLeft oldRoot
///
/// 3 WALs are generated, redo process:
/// - Redo(newLeft, WalInitPage)
///   - create new left
/// - Redo(newRoot, WalInitPage)
///   - create new root
/// - Redo(oldRoot, WalSplitRoot)
///   - move half of the old root to the new left
///   - insert separator key into new root
///   - update meta node to point to new root
///
void BTreeGeneric::splitRootMayJump(
    GuardedBufferFrame<BTreeNode>& guardedMeta,
    GuardedBufferFrame<BTreeNode>& guardedOldRoot,
    const BTreeNode::SeparatorInfo& sepInfo) {
  auto xGuardedMeta = ExclusiveGuardedBufferFrame(std::move(guardedMeta));
  auto xGuardedOldRoot = ExclusiveGuardedBufferFrame(std::move(guardedOldRoot));
  auto* bm = mStore->mBufferManager.get();

  DCHECK(isMetaNode(guardedMeta)) << "Parent should be meta node";
  DCHECK(mHeight == 1 || !xGuardedOldRoot->mIsLeaf);

  // 1. create new left, lock it exclusively, write wal on demand
  auto* newLeftBf = &bm->AllocNewPage(mTreeId);
  auto guardedNewLeft = GuardedBufferFrame<BTreeNode>(bm, newLeftBf);
  auto xGuardedNewLeft =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNewLeft));
  if (mConfig.mEnableWal) {
    xGuardedNewLeft.WriteWal<WalInitPage>(0, mTreeId, xGuardedOldRoot->mIsLeaf);
  }
  xGuardedNewLeft.InitPayload(xGuardedOldRoot->mIsLeaf);

  // 2. create new root, lock it exclusively, write wal on demand
  auto* newRootBf = &bm->AllocNewPage(mTreeId);
  auto guardedNewRoot = GuardedBufferFrame<BTreeNode>(bm, newRootBf);
  auto xGuardedNewRoot =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNewRoot));
  if (mConfig.mEnableWal) {
    xGuardedNewRoot.WriteWal<WalInitPage>(0, mTreeId, false);
  }
  xGuardedNewRoot.InitPayload(false);

  // 3.1. write wal on demand
  if (mConfig.mEnableWal) {
    xGuardedMeta.SyncGSNBeforeWrite();
    xGuardedMeta.MarkAsDirty();
    xGuardedOldRoot.WriteWal<WalSplitRoot>(
        0, xGuardedNewLeft.bf()->mHeader.mPageId,
        xGuardedNewRoot.bf()->mHeader.mPageId,
        xGuardedMeta.bf()->mHeader.mPageId, sepInfo);
  }

  // 3.2. move half of the old root to the new left,
  // 3.3. insert separator key into new root,
  xGuardedNewRoot->mRightMostChildSwip = xGuardedOldRoot.bf();
  xGuardedOldRoot->Split(xGuardedNewRoot, xGuardedNewLeft, sepInfo);

  // 3.4. update meta node to point to new root
  xGuardedMeta->mRightMostChildSwip = xGuardedNewRoot.bf();
  mHeight++;
}

/// Split a non-root node, 3 nodes are involved in the split:
/// parent(child) -> parent(newLeft, child)
///
/// parent         parent
///   |            |   |
/// child     newLeft child
///
void BTreeGeneric::splitNonRootMayJump(
    GuardedBufferFrame<BTreeNode>& guardedParent,
    GuardedBufferFrame<BTreeNode>& guardedChild,
    const BTreeNode::SeparatorInfo& sepInfo, uint16_t spaceNeededForSeparator) {
  auto xGuardedParent = ExclusiveGuardedBufferFrame(std::move(guardedParent));
  auto xGuardedChild = ExclusiveGuardedBufferFrame(std::move(guardedChild));

  DCHECK(!isMetaNode(guardedParent)) << "Parent should not be meta node";
  DCHECK(!xGuardedParent->mIsLeaf) << "Parent should not be leaf node";

  // 1. create new left, lock it exclusively, write wal on demand
  auto* newLeftBf = &mStore->mBufferManager->AllocNewPage(mTreeId);
  auto guardedNewLeft =
      GuardedBufferFrame<BTreeNode>(mStore->mBufferManager.get(), newLeftBf);
  auto xGuardedNewLeft =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNewLeft));
  if (mConfig.mEnableWal) {
    xGuardedNewLeft.WriteWal<WalInitPage>(0, mTreeId, xGuardedChild->mIsLeaf);
  }
  xGuardedNewLeft.InitPayload(xGuardedChild->mIsLeaf);

  // 2.1. write wal on demand or simply mark as dirty
  if (mConfig.mEnableWal) {
    xGuardedParent.SyncGSNBeforeWrite();
    xGuardedParent.MarkAsDirty();
    xGuardedChild.WriteWal<WalSplitNonRoot>(
        0, xGuardedParent.bf()->mHeader.mPageId,
        xGuardedNewLeft.bf()->mHeader.mPageId, sepInfo);
  }

  // 2.2. make room for separator key in parent node
  // 2.3. move half of the old root to the new left
  // 2.4. insert separator key into parent node
  xGuardedParent->requestSpaceFor(spaceNeededForSeparator);
  xGuardedChild->Split(xGuardedParent, xGuardedNewLeft, sepInfo);
}

bool BTreeGeneric::TryMergeMayJump(BufferFrame& toMerge, bool swizzleSibling) {
  // pos == guardedParent->mNumSeps means that the current node is the upper
  // swip in parent
  auto parentHandler = findParentEager(*this, toMerge);
  GuardedBufferFrame<BTreeNode> guardedParent(
      mStore->mBufferManager.get(), std::move(parentHandler.mParentGuard),
      parentHandler.mParentBf);
  GuardedBufferFrame<BTreeNode> guardedChild(
      mStore->mBufferManager.get(), guardedParent, parentHandler.mChildSwip);
  auto posInParent = parentHandler.mPosInParent;
  if (isMetaNode(guardedParent) ||
      guardedChild->FreeSpaceAfterCompaction() < BTreeNode::UnderFullSize()) {
    guardedParent.unlock();
    guardedChild.unlock();
    return false;
  }

  bool succeed = false;
  if (guardedParent->mNumSeps > 1) {
    DCHECK(posInParent <= guardedParent->mNumSeps)
        << "Invalid position in parent"
        << ", posInParent=" << posInParent
        << ", childSizeOfParent=" << guardedParent->mNumSeps;
    guardedParent.JumpIfModifiedByOthers();
    guardedChild.JumpIfModifiedByOthers();

    // TODO: write WALs
    auto mergeAndReclaimLeft = [&]() {
      auto* leftSwip = guardedParent->ChildSwip(posInParent - 1);
      if (!swizzleSibling && leftSwip->IsEvicted()) {
        return false;
      }
      auto guardedLeft = GuardedBufferFrame<BTreeNode>(
          mStore->mBufferManager.get(), guardedParent, *leftSwip);
      auto xGuardedParent =
          ExclusiveGuardedBufferFrame(std::move(guardedParent));
      auto xGuardedChild = ExclusiveGuardedBufferFrame(std::move(guardedChild));
      auto xGuardedLeft = ExclusiveGuardedBufferFrame(std::move(guardedLeft));

      DCHECK(xGuardedChild->mIsLeaf == xGuardedLeft->mIsLeaf);

      if (!xGuardedLeft->merge(posInParent - 1, xGuardedParent,
                               xGuardedChild)) {
        guardedParent = std::move(xGuardedParent);
        guardedChild = std::move(xGuardedChild);
        guardedLeft = std::move(xGuardedLeft);
        return false;
      }

      if (mConfig.mEnableWal) {
        guardedParent.SyncGSNBeforeWrite();
        guardedChild.SyncGSNBeforeWrite();
        guardedLeft.SyncGSNBeforeWrite();
      } else {
        guardedParent.MarkAsDirty();
        guardedChild.MarkAsDirty();
        guardedLeft.MarkAsDirty();
      }

      xGuardedLeft.Reclaim();
      guardedParent = std::move(xGuardedParent);
      guardedChild = std::move(xGuardedChild);
      return true;
    };
    auto mergeAndReclaimRight = [&]() {
      auto& rightSwip = ((posInParent + 1) == guardedParent->mNumSeps)
                            ? guardedParent->mRightMostChildSwip
                            : *guardedParent->ChildSwip(posInParent + 1);
      if (!swizzleSibling && rightSwip.IsEvicted()) {
        return false;
      }
      auto guardedRight = GuardedBufferFrame<BTreeNode>(
          mStore->mBufferManager.get(), guardedParent, rightSwip);
      auto xGuardedParent =
          ExclusiveGuardedBufferFrame(std::move(guardedParent));
      auto xGuardedChild = ExclusiveGuardedBufferFrame(std::move(guardedChild));
      auto xGuardedRight = ExclusiveGuardedBufferFrame(std::move(guardedRight));

      DCHECK(xGuardedChild->mIsLeaf == xGuardedRight->mIsLeaf);

      if (!xGuardedChild->merge(posInParent, xGuardedParent, xGuardedRight)) {
        guardedParent = std::move(xGuardedParent);
        guardedChild = std::move(xGuardedChild);
        guardedRight = std::move(xGuardedRight);
        return false;
      }

      if (mConfig.mEnableWal) {
        guardedParent.SyncGSNBeforeWrite();
        guardedChild.SyncGSNBeforeWrite();
        guardedRight.SyncGSNBeforeWrite();
      } else {
        guardedParent.MarkAsDirty();
        guardedChild.MarkAsDirty();
        guardedRight.MarkAsDirty();
      }

      xGuardedChild.Reclaim();
      guardedParent = std::move(xGuardedParent);
      guardedRight = std::move(xGuardedRight);
      return true;
    };

    // ATTENTION: don't use guardedChild without making sure it was not
    // reclaimed
    if (posInParent > 0) {
      succeed = succeed | mergeAndReclaimLeft();
    }

    if (!succeed && posInParent < guardedParent->mNumSeps) {
      succeed = succeed | mergeAndReclaimRight();
    }
  }

  JUMPMU_TRY() {
    GuardedBufferFrame<BTreeNode> guardedMeta(mStore->mBufferManager.get(),
                                              mMetaNodeSwip);
    if (!isMetaNode(guardedParent) &&
        guardedParent->FreeSpaceAfterCompaction() >=
            BTreeNode::UnderFullSize()) {
      if (TryMergeMayJump(*guardedParent.mBf, true)) {
        WorkerCounters::MyCounters().dt_merge_parent_succ[mTreeId]++;
      } else {
        WorkerCounters::MyCounters().dt_merge_parent_fail[mTreeId]++;
      }
    }
  }
  JUMPMU_CATCH() {
    WorkerCounters::MyCounters().dt_merge_fail[mTreeId]++;
  }

  COUNTERS_BLOCK() {
    if (succeed) {
      WorkerCounters::MyCounters().dt_merge_succ[mTreeId]++;
    } else {
      WorkerCounters::MyCounters().dt_merge_fail[mTreeId]++;
    }
  }
  return succeed;
}

// ret: 0 did nothing, 1 full, 2 partial
int16_t BTreeGeneric::mergeLeftIntoRight(
    ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedParent, int16_t lhsSlotId,
    ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedLeft,
    ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedRight,
    bool fullMergeOrNothing) {
  // TODO: corner cases: new upper fence is larger than the older one.
  uint32_t spaceUpperBound = xGuardedLeft->mergeSpaceUpperBound(xGuardedRight);
  if (spaceUpperBound <= BTreeNode::Size()) {
    // Do a full merge TODO: threshold
    bool succ = xGuardedLeft->merge(lhsSlotId, xGuardedParent, xGuardedRight);
    static_cast<void>(succ);
    assert(succ);
    xGuardedLeft.Reclaim();
    return 1;
  }

  if (fullMergeOrNothing)
    return 0;

  // Do a partial merge
  // Remove a key at a time from the merge and check if now it fits
  int16_t tillSlotId = -1;
  for (int16_t i = 0; i < xGuardedLeft->mNumSeps; i++) {
    spaceUpperBound -= sizeof(BTreeNode::Slot) +
                       xGuardedLeft->KeySizeWithoutPrefix(i) +
                       xGuardedLeft->ValSize(i);
    if (spaceUpperBound + (xGuardedLeft->getFullKeyLen(i) -
                           xGuardedRight->mLowerFence.length) <
        BTreeNode::Size() * 1.0) {
      tillSlotId = i + 1;
      break;
    }
  }
  if (!(tillSlotId != -1 && tillSlotId < (xGuardedLeft->mNumSeps - 1))) {
    return 0; // false
  }

  assert((spaceUpperBound + (xGuardedLeft->getFullKeyLen(tillSlotId - 1) -
                             xGuardedRight->mLowerFence.length)) <
         BTreeNode::Size() * 1.0);
  assert(tillSlotId > 0);

  uint16_t copyFromCount = xGuardedLeft->mNumSeps - tillSlotId;

  uint16_t newLeftUpperFenceSize = xGuardedLeft->getFullKeyLen(tillSlotId - 1);
  ENSURE(newLeftUpperFenceSize > 0);
  auto newLeftUpperFenceBuf =
      utils::JumpScopedArray<uint8_t>(newLeftUpperFenceSize);
  auto* newLeftUpperFence = newLeftUpperFenceBuf->get();
  xGuardedLeft->copyFullKey(tillSlotId - 1, newLeftUpperFence);

  if (!xGuardedParent->prepareInsert(newLeftUpperFenceSize, 0)) {
    return 0; // false
  }

  auto nodeBuf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
  {
    auto* tmp = BTreeNode::Init(nodeBuf->get(), true);

    tmp->setFences(Slice(newLeftUpperFence, newLeftUpperFenceSize),
                   xGuardedRight->GetUpperFence());

    xGuardedLeft->copyKeyValueRange(tmp, 0, tillSlotId, copyFromCount);
    xGuardedRight->copyKeyValueRange(tmp, copyFromCount, 0,
                                     xGuardedRight->mNumSeps);
    memcpy(xGuardedRight.GetPagePayloadPtr(), tmp, BTreeNode::Size());
    xGuardedRight->makeHint();

    // Nothing to do for the right node's separator
    assert(xGuardedRight->compareKeyWithBoundaries(
               Slice(newLeftUpperFence, newLeftUpperFenceSize)) == 1);
  }
  {
    auto* tmp = BTreeNode::Init(nodeBuf->get(), true);

    tmp->setFences(xGuardedLeft->GetLowerFence(),
                   Slice(newLeftUpperFence, newLeftUpperFenceSize));
    // -------------------------------------------------------------------------------------
    xGuardedLeft->copyKeyValueRange(tmp, 0, 0,
                                    xGuardedLeft->mNumSeps - copyFromCount);
    memcpy(xGuardedLeft.GetPagePayloadPtr(), tmp, BTreeNode::Size());
    xGuardedLeft->makeHint();
    // -------------------------------------------------------------------------------------
    assert(xGuardedLeft->compareKeyWithBoundaries(
               Slice(newLeftUpperFence, newLeftUpperFenceSize)) == 0);
    // -------------------------------------------------------------------------------------
    xGuardedParent->removeSlot(lhsSlotId);
    ENSURE(xGuardedParent->prepareInsert(xGuardedLeft->mUpperFence.length,
                                         sizeof(Swip)));
    auto swip = xGuardedLeft.swip();
    Slice key(xGuardedLeft->getUpperFenceKey(),
              xGuardedLeft->mUpperFence.length);
    Slice val(reinterpret_cast<uint8_t*>(&swip), sizeof(Swip));
    xGuardedParent->Insert(key, val);
  }
  return 2;
}

// returns true if it has exclusively locked anything
BTreeGeneric::XMergeReturnCode BTreeGeneric::XMerge(
    GuardedBufferFrame<BTreeNode>& guardedParent,
    GuardedBufferFrame<BTreeNode>& guardedChild,
    ParentSwipHandler& parentHandler) {
  WorkerCounters::MyCounters().dt_researchy[0][1]++;
  if (guardedChild->fillFactorAfterCompaction() >= 0.9) {
    return XMergeReturnCode::kNothing;
  }

  int64_t maxMergePages = FLAGS_xmerge_k;
  GuardedBufferFrame<BTreeNode> guardedNodes[maxMergePages];
  bool fullyMerged[maxMergePages];

  int64_t pos = parentHandler.mPosInParent;
  int64_t pageCount = 1;
  int64_t maxRight;

  guardedNodes[0] = std::move(guardedChild);
  fullyMerged[0] = false;
  double totalFillFactor = guardedNodes[0]->fillFactorAfterCompaction();

  // Handle upper swip instead of avoiding guardedParent->mNumSeps -1 swip
  if (isMetaNode(guardedParent) || !guardedNodes[0]->mIsLeaf) {
    guardedChild = std::move(guardedNodes[0]);
    return XMergeReturnCode::kNothing;
  }
  for (maxRight = pos + 1; (maxRight - pos) < maxMergePages &&
                           (maxRight + 1) < guardedParent->mNumSeps;
       maxRight++) {
    if (!guardedParent->ChildSwip(maxRight)->IsHot()) {
      guardedChild = std::move(guardedNodes[0]);
      return XMergeReturnCode::kNothing;
    }

    guardedNodes[maxRight - pos] = GuardedBufferFrame<BTreeNode>(
        mStore->mBufferManager.get(), guardedParent,
        *guardedParent->ChildSwip(maxRight));
    fullyMerged[maxRight - pos] = false;
    totalFillFactor +=
        guardedNodes[maxRight - pos]->fillFactorAfterCompaction();
    pageCount++;
    if ((pageCount - std::ceil(totalFillFactor)) >= (1)) {
      // we can probably save a page by merging all together so there is no need
      // to look furhter
      break;
    }
  }
  if (((pageCount - std::ceil(totalFillFactor))) < (1)) {
    guardedChild = std::move(guardedNodes[0]);
    return XMergeReturnCode::kNothing;
  }

  ExclusiveGuardedBufferFrame<BTreeNode> xGuardedParent =
      std::move(guardedParent);
  xGuardedParent.SyncGSNBeforeWrite();

  XMergeReturnCode retCode = XMergeReturnCode::kPartialMerge;
  int16_t leftHand, rightHand, ret;
  while (true) {
    for (rightHand = maxRight; rightHand > pos; rightHand--) {
      if (fullyMerged[rightHand - pos]) {
        continue;
      }
      break;
    }
    if (rightHand == pos)
      break;

    leftHand = rightHand - 1;

    {
      ExclusiveGuardedBufferFrame<BTreeNode> xGuardedRight(
          std::move(guardedNodes[rightHand - pos]));
      ExclusiveGuardedBufferFrame<BTreeNode> xGuardedLeft(
          std::move(guardedNodes[leftHand - pos]));
      xGuardedRight.SyncGSNBeforeWrite();
      xGuardedLeft.SyncGSNBeforeWrite();
      maxRight = leftHand;
      ret = mergeLeftIntoRight(xGuardedParent, leftHand, xGuardedLeft,
                               xGuardedRight, leftHand == pos);
      // we unlock only the left page, the right one should not be touched again
      if (ret == 1) {
        fullyMerged[leftHand - pos] = true;
        WorkerCounters::MyCounters().xmerge_full_counter[mTreeId]++;
        retCode = XMergeReturnCode::kFullMerge;
      } else if (ret == 2) {
        guardedNodes[leftHand - pos] = std::move(xGuardedLeft);
        WorkerCounters::MyCounters().xmerge_partial_counter[mTreeId]++;
      } else if (ret == 0) {
        break;
      } else {
        DLOG(FATAL) << "Invalid return code from mergeLeftIntoRight";
      }
    }
  }
  if (guardedChild.mGuard.mState == GuardState::kMoved) {
    guardedChild = std::move(guardedNodes[0]);
  }
  guardedParent = std::move(xGuardedParent);
  return retCode;
}

// -------------------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------------------
int64_t BTreeGeneric::iterateAllPages(BTreeNodeCallback inner,
                                      BTreeNodeCallback leaf) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> guardedParent(mStore->mBufferManager.get(),
                                                  mMetaNodeSwip);
      GuardedBufferFrame<BTreeNode> guardedChild(
          mStore->mBufferManager.get(), guardedParent,
          guardedParent->mRightMostChildSwip);
      int64_t result = iterateAllPagesRecursive(guardedChild, inner, leaf);
      JUMPMU_RETURN result;
    }
    JUMPMU_CATCH() {
    }
  }
}

int64_t BTreeGeneric::iterateAllPagesRecursive(
    GuardedBufferFrame<BTreeNode>& guardedNode, BTreeNodeCallback inner,
    BTreeNodeCallback leaf) {
  if (guardedNode->mIsLeaf) {
    return leaf(guardedNode.ref());
  }
  int64_t res = inner(guardedNode.ref());
  for (uint16_t i = 0; i < guardedNode->mNumSeps; i++) {
    auto* childSwip = guardedNode->ChildSwip(i);
    auto guardedChild = GuardedBufferFrame<BTreeNode>(
        mStore->mBufferManager.get(), guardedNode, *childSwip);
    guardedChild.JumpIfModifiedByOthers();
    res += iterateAllPagesRecursive(guardedChild, inner, leaf);
  }

  Swip& childSwip = guardedNode->mRightMostChildSwip;
  auto guardedChild = GuardedBufferFrame<BTreeNode>(
      mStore->mBufferManager.get(), guardedNode, childSwip);
  guardedChild.JumpIfModifiedByOthers();
  res += iterateAllPagesRecursive(guardedChild, inner, leaf);

  return res;
}

uint64_t BTreeGeneric::GetHeight() {
  return mHeight.load();
}

uint64_t BTreeGeneric::CountEntries() {
  return iterateAllPages([](BTreeNode&) { return 0; },
                         [](BTreeNode& node) { return node.mNumSeps; });
}

uint64_t BTreeGeneric::CountAllPages() {
  return iterateAllPages([](BTreeNode&) { return 1; },
                         [](BTreeNode&) { return 1; });
}

uint64_t BTreeGeneric::CountInnerPages() {
  return iterateAllPages([](BTreeNode&) { return 1; },
                         [](BTreeNode&) { return 0; });
}

uint32_t BTreeGeneric::FreeSpaceAfterCompaction() {
  return iterateAllPages(
      [](BTreeNode& inner) { return inner.FreeSpaceAfterCompaction(); },
      [](BTreeNode& leaf) { return leaf.FreeSpaceAfterCompaction(); });
}

void BTreeGeneric::PrintInfo(uint64_t totalSize) {
  GuardedBufferFrame<BTreeNode> guardedMeta(mStore->mBufferManager.get(),
                                            mMetaNodeSwip);
  GuardedBufferFrame<BTreeNode> guardedRoot(mStore->mBufferManager.get(),
                                            guardedMeta,
                                            guardedMeta->mRightMostChildSwip);
  uint64_t numAllPages = CountAllPages();
  cout << "nodes:" << numAllPages << ", innerNodes:" << CountInnerPages()
       << ", space:" << (numAllPages * BTreeNode::Size()) / (float)totalSize
       << ", height:" << mHeight << ", rootCnt:" << guardedRoot->mNumSeps
       << ", freeSpaceAfterCompaction:" << FreeSpaceAfterCompaction() << endl;
}

StringMap BTreeGeneric::Serialize() {
  DCHECK(mMetaNodeSwip.AsBufferFrame().mPage.mBTreeId == mTreeId);
  auto& metaBf = mMetaNodeSwip.AsBufferFrame();
  auto metaPageId = metaBf.mHeader.mPageId;
  auto res = mStore->mBufferManager->CheckpointBufferFrame(metaBf);
  DLOG_IF(FATAL, !res) << "Failed to checkpoint meta node: "
                       << res.error().ToString();
  return {{kTreeId, std::to_string(mTreeId)},
          {kHeight, std::to_string(mHeight.load())},
          {kMetaPageId, std::to_string(metaPageId)}};
}

void BTreeGeneric::Deserialize(StringMap map) {
  mTreeId = std::stoull(map[kTreeId]);
  mHeight = std::stoull(map[kHeight]);
  mMetaNodeSwip.Evict(std::stoull(map[kMetaPageId]));

  // load meta node to memory
  HybridLatch dummyLatch;
  HybridGuard dummyGuard(&dummyLatch);
  dummyGuard.ToOptimisticSpin();

  uint16_t failcounter = 0;
  while (true) {
    JUMPMU_TRY() {
      mMetaNodeSwip =
          mStore->mBufferManager->ResolveSwipMayJump(dummyGuard, mMetaNodeSwip);
      JUMPMU_BREAK;
    }
    JUMPMU_CATCH() {
      failcounter++;
      LOG_IF(FATAL, failcounter >= 100) << "Failed to load MetaNode";
    }
  }
  mMetaNodeSwip.AsBufferFrame().mHeader.mKeepInMemory = true;
  DCHECK(mMetaNodeSwip.AsBufferFrame().mPage.mBTreeId == mTreeId);
}

} // namespace leanstore::storage::btree