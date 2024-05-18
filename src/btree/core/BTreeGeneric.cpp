#include "BTreeGeneric.hpp"

#include "btree/core/BTreeNode.hpp"
#include "btree/core/BTreePessimisticExclusiveIterator.hpp"
#include "btree/core/BTreePessimisticSharedIterator.hpp"
#include "btree/core/BTreeWalPayload.hpp"
#include "buffer-manager/BufferFrame.hpp"
#include "buffer-manager/BufferManager.hpp"
#include "buffer-manager/GuardedBufferFrame.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "utils/Defer.hpp"
#include "utils/Log.hpp"
#include "utils/Misc.hpp"

using namespace leanstore::storage;

namespace leanstore::storage::btree {

void BTreeGeneric::Init(leanstore::LeanStore* store, TREEID btreeId,
                        BTreeConfig config) {
  this->mStore = store;
  this->mTreeId = btreeId;
  this->mConfig = std::move(config);

  mMetaNodeSwip = &mStore->mBufferManager->AllocNewPageMayJump(btreeId);
  mMetaNodeSwip.AsBufferFrame().mHeader.mKeepInMemory = true;
  LS_DCHECK(
      mMetaNodeSwip.AsBufferFrame().mHeader.mLatch.GetOptimisticVersion() == 0);

  auto guardedRoot = GuardedBufferFrame<BTreeNode>(
      mStore->mBufferManager.get(),
      &mStore->mBufferManager->AllocNewPageMayJump(btreeId));
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
    Log::Warn("Split failed, not enough separators in node, "
              "toSplit.mHeader.mPageId={}, favoredSplitPos={}, "
              "guardedChild->mNumSeps={}",
              toSplit.mHeader.mPageId, favoredSplitPos, guardedChild->mNumSeps);
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
  if (!guardedParent->HasEnoughSpaceFor(spaceNeededForSeparator)) {
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

  LS_DCHECK(isMetaNode(guardedMeta), "Parent should be meta node");
  LS_DCHECK(mHeight == 1 || !xGuardedOldRoot->mIsLeaf);

  // 1. create new left, lock it exclusively, write wal on demand
  auto* newLeftBf = &bm->AllocNewPageMayJump(mTreeId);
  auto guardedNewLeft = GuardedBufferFrame<BTreeNode>(bm, newLeftBf);
  auto xGuardedNewLeft =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNewLeft));
  if (mConfig.mEnableWal) {
    xGuardedNewLeft.WriteWal<WalInitPage>(0, mTreeId, xGuardedOldRoot->mIsLeaf);
  }
  xGuardedNewLeft.InitPayload(xGuardedOldRoot->mIsLeaf);

  // 2. create new root, lock it exclusively, write wal on demand
  auto* newRootBf = &bm->AllocNewPageMayJump(mTreeId);
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

  LS_DCHECK(!isMetaNode(guardedParent), "Parent should not be meta node");
  LS_DCHECK(!xGuardedParent->mIsLeaf, "Parent should not be leaf node");

  // 1. create new left, lock it exclusively, write wal on demand
  auto* newLeftBf = &mStore->mBufferManager->AllocNewPageMayJump(mTreeId);
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
    xGuardedChild.WriteWal<WalSplitNonRoot>(
        0, xGuardedParent.bf()->mHeader.mPageId,
        xGuardedNewLeft.bf()->mHeader.mPageId, sepInfo);
  }

  // 2.2. make room for separator key in parent node
  // 2.3. move half of the old root to the new left
  // 2.4. insert separator key into parent node
  xGuardedParent->RequestSpaceFor(spaceNeededForSeparator);
  xGuardedChild->Split(xGuardedParent, xGuardedNewLeft, sepInfo);
}

bool BTreeGeneric::TryMergeMayJump(BufferFrame& toMerge, bool swizzleSibling) {
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

  if (guardedParent->mNumSeps <= 1) {
    return false;
  }

  LS_DCHECK(posInParent <= guardedParent->mNumSeps,
            "Invalid position in parent, posInParent={}, childSizeOfParent={}",
            posInParent, guardedParent->mNumSeps);
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
    auto xGuardedParent = ExclusiveGuardedBufferFrame(std::move(guardedParent));
    auto xGuardedChild = ExclusiveGuardedBufferFrame(std::move(guardedChild));
    auto xGuardedLeft = ExclusiveGuardedBufferFrame(std::move(guardedLeft));

    LS_DCHECK(xGuardedChild->mIsLeaf == xGuardedLeft->mIsLeaf);

    if (!xGuardedLeft->merge(posInParent - 1, xGuardedParent, xGuardedChild)) {
      guardedParent = std::move(xGuardedParent);
      guardedChild = std::move(xGuardedChild);
      guardedLeft = std::move(xGuardedLeft);
      return false;
    }

    if (mConfig.mEnableWal) {
      guardedParent.SyncGSNBeforeWrite();
      guardedChild.SyncGSNBeforeWrite();
      guardedLeft.SyncGSNBeforeWrite();
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
    auto xGuardedParent = ExclusiveGuardedBufferFrame(std::move(guardedParent));
    auto xGuardedChild = ExclusiveGuardedBufferFrame(std::move(guardedChild));
    auto xGuardedRight = ExclusiveGuardedBufferFrame(std::move(guardedRight));

    LS_DCHECK(xGuardedChild->mIsLeaf == xGuardedRight->mIsLeaf);

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
    }

    xGuardedChild.Reclaim();
    guardedParent = std::move(xGuardedParent);
    guardedRight = std::move(xGuardedRight);
    return true;
  };

  SCOPED_DEFER({
    if (!isMetaNode(guardedParent) &&
        guardedParent->FreeSpaceAfterCompaction() >=
            BTreeNode::UnderFullSize()) {
      JUMPMU_TRY() {
        TryMergeMayJump(*guardedParent.mBf, true);
      }
      JUMPMU_CATCH() {
        COUNTERS_BLOCK() {
          WorkerCounters::MyCounters().dt_merge_fail[mTreeId]++;
        }
      }
    }
  });

  bool succeed = false;
  if (posInParent > 0) {
    succeed = mergeAndReclaimLeft();
  }
  if (!succeed && posInParent < guardedParent->mNumSeps) {
    succeed = mergeAndReclaimRight();
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
                           xGuardedRight->mLowerFence.mLength) <
        BTreeNode::Size() * 1.0) {
      tillSlotId = i + 1;
      break;
    }
  }
  if (!(tillSlotId != -1 && tillSlotId < (xGuardedLeft->mNumSeps - 1))) {
    return 0; // false
  }

  assert((spaceUpperBound + (xGuardedLeft->getFullKeyLen(tillSlotId - 1) -
                             xGuardedRight->mLowerFence.mLength)) <
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
    ENSURE(xGuardedParent->prepareInsert(xGuardedLeft->mUpperFence.mLength,
                                         sizeof(Swip)));
    auto swip = xGuardedLeft.swip();
    Slice key(xGuardedLeft->GetUpperFenceKey(),
              xGuardedLeft->mUpperFence.mLength);
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
  if (guardedChild->FillFactorAfterCompaction() >= 0.9) {
    return XMergeReturnCode::kNothing;
  }

  int64_t maxMergePages = mStore->mStoreOption.mXMergeK;
  GuardedBufferFrame<BTreeNode> guardedNodes[maxMergePages];
  bool fullyMerged[maxMergePages];

  int64_t pos = parentHandler.mPosInParent;
  int64_t pageCount = 1;
  int64_t maxRight;

  guardedNodes[0] = std::move(guardedChild);
  fullyMerged[0] = false;
  double totalFillFactor = guardedNodes[0]->FillFactorAfterCompaction();

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
        guardedNodes[maxRight - pos]->FillFactorAfterCompaction();
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
        Log::Fatal("Invalid return code from mergeLeftIntoRight");
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
  LS_DCHECK(mMetaNodeSwip.AsBufferFrame().mPage.mBTreeId == mTreeId);
  auto& metaBf = mMetaNodeSwip.AsBufferFrame();
  auto metaPageId = metaBf.mHeader.mPageId;
  auto res = mStore->mBufferManager->CheckpointBufferFrame(metaBf);
  if (!res) {
    Log::Fatal("Failed to checkpoint meta node: {}", res.error().ToString());
  }
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
  while (true) {
    JUMPMU_TRY() {
      mMetaNodeSwip =
          mStore->mBufferManager->ResolveSwipMayJump(dummyGuard, mMetaNodeSwip);
      JUMPMU_BREAK;
    }
    JUMPMU_CATCH() {
    }
  }
  mMetaNodeSwip.AsBufferFrame().mHeader.mKeepInMemory = true;
  LS_DCHECK(mMetaNodeSwip.AsBufferFrame().mPage.mBTreeId == mTreeId,
            "MetaNode has wrong BTreeId, pageId={}, expected={}, actual={}",
            mMetaNodeSwip.AsBufferFrame().mHeader.mPageId, mTreeId,
            mMetaNodeSwip.AsBufferFrame().mPage.mBTreeId);
}

} // namespace leanstore::storage::btree
