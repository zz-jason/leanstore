#include "BTreeGeneric.hpp"

#include "Config.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "storage/btree/core/BTreeWALPayload.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "storage/buffer-manager/GuardedBufferFrame.hpp"
#include "utils/Misc.hpp"

#include <glog/logging.h>

using namespace leanstore::storage;

namespace leanstore::storage::btree {

void BTreeGeneric::Init(TREEID btreeId, Config config) {
  this->mTreeId = btreeId;
  this->config = config;
  mMetaNodeSwip = &BufferManager::sInstance->AllocNewPage();

  HybridGuard guard(mMetaNodeSwip.AsBufferFrame().header.mLatch,
                    GuardState::kExclusive);
  mMetaNodeSwip.AsBufferFrame().header.mKeepInMemory = true;
  mMetaNodeSwip.AsBufferFrame().page.mBTreeId = btreeId;
  guard.unlock();

  auto guardedRoot = GuardedBufferFrame<BTreeNode>(btreeId);
  auto xGuardedRoot =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedRoot));
  xGuardedRoot.InitPayload(true);

  GuardedBufferFrame<BTreeNode> guardedMeta(mMetaNodeSwip);
  ExclusiveGuardedBufferFrame xGuardedMeta(std::move(guardedMeta));
  xGuardedMeta->mIsLeaf = false;
  xGuardedMeta->mRightMostChildSwip = xGuardedRoot.bf();

  // Record WAL
  if (config.mEnableWal) {
    auto rootWalHandler = xGuardedRoot.ReserveWALPayload<WALInitPage>(
        0, mTreeId, xGuardedRoot->mIsLeaf);
    rootWalHandler.SubmitWal();

    auto metaWalHandler = xGuardedMeta.ReserveWALPayload<WALInitPage>(
        0, mTreeId, xGuardedMeta->mIsLeaf);
    metaWalHandler.SubmitWal();
  }

  xGuardedRoot.SyncGSNBeforeWrite();
  xGuardedMeta.SyncGSNBeforeWrite();
}

void BTreeGeneric::TrySplitMayJump(BufferFrame& toSplit, s16 favoredSplitPos) {
  cr::Worker::my().mLogging.WalEnsureEnoughSpace(FLAGS_page_size);
  auto parentHandler = findParentEager(*this, toSplit);
  auto guardedParent = parentHandler.GetGuardedParent<BTreeNode>();
  auto guardedChild = GuardedBufferFrame(
      guardedParent, parentHandler.mChildSwip.CastTo<BTreeNode>());
  if (guardedChild->mNumSeps <= 1) {
    DLOG(WARNING) << "Split failed, not enough separators in node"
                  << ", toSplit.header.mPageId=" << toSplit.header.mPageId
                  << ", favoredSplitPos=" << favoredSplitPos
                  << ", guardedChild->mNumSeps=" << guardedChild->mNumSeps;
    return;
  }

  BTreeNode::SeparatorInfo sepInfo;
  if (favoredSplitPos < 0 || favoredSplitPos >= guardedChild->mNumSeps - 1) {
    if (config.mUseBulkInsert) {
      favoredSplitPos = guardedChild->mNumSeps - 2;
      sepInfo =
          BTreeNode::SeparatorInfo{guardedChild->getFullKeyLen(favoredSplitPos),
                                   static_cast<u16>(favoredSplitPos), false};
    } else {
      sepInfo = guardedChild->findSep();
    }
  } else {
    // Split on a specified position, used by contention management
    sepInfo =
        BTreeNode::SeparatorInfo{guardedChild->getFullKeyLen(favoredSplitPos),
                                 static_cast<u16>(favoredSplitPos), false};
  }
  auto sepKeyBuf = utils::JumpScopedArray<u8>(sepInfo.length);
  auto* sepKey = sepKeyBuf->get();
  if (isMetaNode(guardedParent)) {
    // split the root node
    auto xGuardedParent = ExclusiveGuardedBufferFrame(std::move(guardedParent));
    auto xGuardedChild = ExclusiveGuardedBufferFrame(std::move(guardedChild));
    DCHECK(mHeight == 1 || !xGuardedChild->mIsLeaf);

    // create new root
    auto guardedNewRoot = GuardedBufferFrame<BTreeNode>(mTreeId, false);
    auto xGuardedNewRoot =
        ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNewRoot));
    auto guardedNewLeft = GuardedBufferFrame<BTreeNode>(mTreeId);
    auto xGuardedNewLeft =
        ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNewLeft));

    if (config.mEnableWal) {
      // TODO: System transactions
      xGuardedNewRoot.SyncGSNBeforeWrite();
      xGuardedNewLeft.SyncGSNBeforeWrite();
      xGuardedChild.SyncGSNBeforeWrite();
    } else {
      xGuardedNewRoot.MarkAsDirty();
      xGuardedNewLeft.MarkAsDirty();
      xGuardedChild.MarkAsDirty();
    }

    if (config.mEnableWal) {
      auto newRootWalHandler =
          xGuardedNewRoot.ReserveWALPayload<WALInitPage>(0, mTreeId, false);
      newRootWalHandler.SubmitWal();

      auto newLeftWalHandler = xGuardedNewLeft.ReserveWALPayload<WALInitPage>(
          0, mTreeId, xGuardedChild->mIsLeaf);
      newLeftWalHandler.SubmitWal();

      auto parentPageId(xGuardedNewRoot.bf()->header.mPageId);
      auto lhsPageId(xGuardedNewLeft.bf()->header.mPageId);
      auto rhsPageId(xGuardedChild.bf()->header.mPageId);

      auto curRightWalHandler =
          xGuardedChild.ReserveWALPayload<WALLogicalSplit>(
              0, parentPageId, lhsPageId, rhsPageId);
      curRightWalHandler.SubmitWal();

      auto rootWalHandler = xGuardedNewRoot.ReserveWALPayload<WALLogicalSplit>(
          0, parentPageId, lhsPageId, rhsPageId);
      rootWalHandler.SubmitWal();

      auto leftWalHandler = xGuardedNewLeft.ReserveWALPayload<WALLogicalSplit>(
          0, parentPageId, lhsPageId, rhsPageId);
      leftWalHandler.SubmitWal();
    }

    xGuardedNewRoot.keepAlive();
    xGuardedNewRoot.InitPayload(false);
    xGuardedNewRoot->mRightMostChildSwip = xGuardedChild.bf();
    xGuardedParent->mRightMostChildSwip = xGuardedNewRoot.bf();

    xGuardedNewLeft.InitPayload(xGuardedChild->mIsLeaf);
    xGuardedChild->getSep(sepKey, sepInfo);
    xGuardedChild->split(xGuardedNewRoot, xGuardedNewLeft, sepInfo.slot, sepKey,
                         sepInfo.length);
    mHeight++;
    COUNTERS_BLOCK() {
      WorkerCounters::MyCounters().dt_split[mTreeId]++;
    }
    return;
  }

  // Parent is not root
  const u16 spaceNeededForSeparator =
      guardedParent->spaceNeeded(sepInfo.length, sizeof(SwipType));
  if (guardedParent->hasEnoughSpaceFor(spaceNeededForSeparator)) {
    // Is there enough space in the parent for the separator?
    auto xGuardedParent = ExclusiveGuardedBufferFrame(std::move(guardedParent));
    auto xGuardedChild = ExclusiveGuardedBufferFrame(std::move(guardedChild));

    xGuardedParent->requestSpaceFor(spaceNeededForSeparator);
    DCHECK(&mMetaNodeSwip.AsBufferFrame() != xGuardedParent.bf());
    DCHECK(!xGuardedParent->mIsLeaf);

    auto guardedNewLeft = GuardedBufferFrame<BTreeNode>(mTreeId);
    auto xGuardedNewLeft =
        ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNewLeft));

    // Increment GSNs before writing WAL to make sure that these pages marked
    // as dirty regardless of the FLAGS_wal
    if (config.mEnableWal) {
      xGuardedParent.SyncGSNBeforeWrite();
      xGuardedNewLeft.SyncGSNBeforeWrite();
      xGuardedChild.SyncGSNBeforeWrite();
    } else {
      xGuardedParent.MarkAsDirty();
      xGuardedNewLeft.MarkAsDirty();
      xGuardedChild.MarkAsDirty();
    }

    if (config.mEnableWal) {
      auto newLeftWalHandler = xGuardedNewLeft.ReserveWALPayload<WALInitPage>(
          0, mTreeId, xGuardedNewLeft->mIsLeaf);
      newLeftWalHandler.SubmitWal();

      auto parentPageId = xGuardedParent.bf()->header.mPageId;
      auto lhsPageId = xGuardedNewLeft.bf()->header.mPageId;
      auto rhsPageId = xGuardedChild.bf()->header.mPageId;

      auto curRightWalHandler =
          xGuardedChild.ReserveWALPayload<WALLogicalSplit>(
              0, parentPageId, lhsPageId, rhsPageId);
      curRightWalHandler.SubmitWal();

      auto parentWalHandler = xGuardedParent.ReserveWALPayload<WALLogicalSplit>(
          0, parentPageId, lhsPageId, rhsPageId);
      parentWalHandler.SubmitWal();

      newLeftWalHandler = xGuardedNewLeft.ReserveWALPayload<WALInitPage>(
          0, mTreeId, xGuardedNewLeft->mIsLeaf);
      newLeftWalHandler.SubmitWal();

      auto leftWalHandler = xGuardedNewLeft.ReserveWALPayload<WALLogicalSplit>(
          0, parentPageId, lhsPageId, rhsPageId);
      leftWalHandler.SubmitWal();
    }

    xGuardedNewLeft.InitPayload(xGuardedChild->mIsLeaf);
    xGuardedChild->getSep(sepKey, sepInfo);
    xGuardedChild->split(xGuardedParent, xGuardedNewLeft, sepInfo.slot, sepKey,
                         sepInfo.length);
    COUNTERS_BLOCK() {
      WorkerCounters::MyCounters().dt_split[mTreeId]++;
    }
  } else {
    guardedParent.unlock();
    guardedChild.unlock();
    // Must split parent head to make space for separator
    TrySplitMayJump(*guardedParent.mBf);
  }
}

bool BTreeGeneric::TryMergeMayJump(BufferFrame& toMerge, bool swizzleSibling) {
  // pos == guardedParent->mNumSeps means that the current node is the upper
  // swip in parent
  auto parentHandler = findParentEager(*this, toMerge);
  auto guardedParent = parentHandler.GetGuardedParent<BTreeNode>();
  GuardedBufferFrame guardedChild(guardedParent,
                                  parentHandler.mChildSwip.CastTo<BTreeNode>());
  auto posInParent = parentHandler.mPosInParent;
  if (isMetaNode(guardedParent) ||
      guardedChild->freeSpaceAfterCompaction() < BTreeNode::UnderFullSize()) {
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
      Swip<BTreeNode>& leftSwip = guardedParent->getChild(posInParent - 1);
      if (!swizzleSibling && leftSwip.isEVICTED()) {
        return false;
      }
      auto guardedLeft = GuardedBufferFrame(guardedParent, leftSwip);
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

      if (config.mEnableWal) {
        guardedParent.SyncGSNBeforeWrite();
        guardedChild.SyncGSNBeforeWrite();
        guardedLeft.SyncGSNBeforeWrite();
      } else {
        guardedParent.MarkAsDirty();
        guardedChild.MarkAsDirty();
        guardedLeft.MarkAsDirty();
      }

      xGuardedLeft.reclaim();
      guardedParent = std::move(xGuardedParent);
      guardedChild = std::move(xGuardedChild);
      return true;
    };
    auto mergeAndReclaimRight = [&]() {
      Swip<BTreeNode>& rightSwip =
          ((posInParent + 1) == guardedParent->mNumSeps)
              ? guardedParent->mRightMostChildSwip
              : guardedParent->getChild(posInParent + 1);
      if (!swizzleSibling && rightSwip.isEVICTED()) {
        return false;
      }
      auto guardedRight = GuardedBufferFrame(guardedParent, rightSwip);
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

      if (config.mEnableWal) {
        guardedParent.SyncGSNBeforeWrite();
        guardedChild.SyncGSNBeforeWrite();
        guardedRight.SyncGSNBeforeWrite();
      } else {
        guardedParent.MarkAsDirty();
        guardedChild.MarkAsDirty();
        guardedRight.MarkAsDirty();
      }

      xGuardedChild.reclaim();
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
    GuardedBufferFrame<BTreeNode> guardedMeta(mMetaNodeSwip);
    if (!isMetaNode(guardedParent) &&
        guardedParent->freeSpaceAfterCompaction() >=
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
s16 BTreeGeneric::mergeLeftIntoRight(
    ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedParent, s16 lhsSlotId,
    ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedLeft,
    ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedRight,
    bool full_merge_or_nothing) {
  // TODO: corner cases: new upper fence is larger than the older one.
  u32 space_upper_bound = xGuardedLeft->mergeSpaceUpperBound(xGuardedRight);
  if (space_upper_bound <= BTreeNode::Size()) {
    // Do a full merge TODO: threshold
    bool succ = xGuardedLeft->merge(lhsSlotId, xGuardedParent, xGuardedRight);
    static_cast<void>(succ);
    assert(succ);
    xGuardedLeft.reclaim();
    return 1;
  }

  if (full_merge_or_nothing)
    return 0;

  // Do a partial merge
  // Remove a key at a time from the merge and check if now it fits
  s16 till_slot_id = -1;
  for (s16 s_i = 0; s_i < xGuardedLeft->mNumSeps; s_i++) {
    space_upper_bound -= sizeof(BTreeNode::Slot) +
                         xGuardedLeft->KeySizeWithoutPrefix(s_i) +
                         xGuardedLeft->ValSize(s_i);
    if (space_upper_bound + (xGuardedLeft->getFullKeyLen(s_i) -
                             xGuardedRight->mLowerFence.length) <
        BTreeNode::Size() * 1.0) {
      till_slot_id = s_i + 1;
      break;
    }
  }
  if (!(till_slot_id != -1 && till_slot_id < (xGuardedLeft->mNumSeps - 1)))
    return 0; // false

  assert((space_upper_bound + (xGuardedLeft->getFullKeyLen(till_slot_id - 1) -
                               xGuardedRight->mLowerFence.length)) <
         BTreeNode::Size() * 1.0);
  assert(till_slot_id > 0);

  u16 copy_from_count = xGuardedLeft->mNumSeps - till_slot_id;

  u16 newLeftUpperFenceSize = xGuardedLeft->getFullKeyLen(till_slot_id - 1);
  ENSURE(newLeftUpperFenceSize > 0);
  auto newLeftUpperFenceBuf = utils::JumpScopedArray<u8>(newLeftUpperFenceSize);
  auto newLeftUpperFence = newLeftUpperFenceBuf->get();
  xGuardedLeft->copyFullKey(till_slot_id - 1, newLeftUpperFence);

  if (!xGuardedParent->prepareInsert(newLeftUpperFenceSize, 0)) {
    return 0; // false
  }

  auto nodeBuf = utils::JumpScopedArray<u8>(BTreeNode::Size());
  {
    auto tmp = BTreeNode::Init(nodeBuf->get(), true);

    tmp->setFences(Slice(newLeftUpperFence, newLeftUpperFenceSize),
                   xGuardedRight->GetUpperFence());

    xGuardedLeft->copyKeyValueRange(tmp, 0, till_slot_id, copy_from_count);
    xGuardedRight->copyKeyValueRange(tmp, copy_from_count, 0,
                                     xGuardedRight->mNumSeps);
    memcpy(xGuardedRight.GetPagePayloadPtr(), tmp, BTreeNode::Size());
    xGuardedRight->makeHint();

    // Nothing to do for the right node's separator
    assert(xGuardedRight->compareKeyWithBoundaries(
               Slice(newLeftUpperFence, newLeftUpperFenceSize)) == 1);
  }
  {
    auto tmp = BTreeNode::Init(nodeBuf->get(), true);

    tmp->setFences(xGuardedLeft->GetLowerFence(),
                   Slice(newLeftUpperFence, newLeftUpperFenceSize));
    // -------------------------------------------------------------------------------------
    xGuardedLeft->copyKeyValueRange(tmp, 0, 0,
                                    xGuardedLeft->mNumSeps - copy_from_count);
    memcpy(xGuardedLeft.GetPagePayloadPtr(), tmp, BTreeNode::Size());
    xGuardedLeft->makeHint();
    // -------------------------------------------------------------------------------------
    assert(xGuardedLeft->compareKeyWithBoundaries(
               Slice(newLeftUpperFence, newLeftUpperFenceSize)) == 0);
    // -------------------------------------------------------------------------------------
    xGuardedParent->removeSlot(lhsSlotId);
    ENSURE(xGuardedParent->prepareInsert(xGuardedLeft->mUpperFence.length,
                                         sizeof(SwipType)));
    auto swip = xGuardedLeft.swip();
    Slice key(xGuardedLeft->getUpperFenceKey(),
              xGuardedLeft->mUpperFence.length);
    Slice val(reinterpret_cast<u8*>(&swip), sizeof(SwipType));
    xGuardedParent->insert(key, val);
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

  s64 maxMergePages = FLAGS_xmerge_k;
  GuardedBufferFrame<BTreeNode> guardedNodes[maxMergePages];
  bool fullyMerged[maxMergePages];

  s64 pos = parentHandler.mPosInParent;
  s64 pageCount = 1;
  s64 maxRight;

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
    if (!guardedParent->getChild(maxRight).isHOT()) {
      guardedChild = std::move(guardedNodes[0]);
      return XMergeReturnCode::kNothing;
    }

    guardedNodes[maxRight - pos] = GuardedBufferFrame<BTreeNode>(
        guardedParent, guardedParent->getChild(maxRight));
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

  XMergeReturnCode ret_code = XMergeReturnCode::kPartialMerge;
  s16 left_hand, right_hand, ret;
  while (true) {
    for (right_hand = maxRight; right_hand > pos; right_hand--) {
      if (fullyMerged[right_hand - pos]) {
        continue;
      } else {
        break;
      }
    }
    if (right_hand == pos)
      break;

    left_hand = right_hand - 1;

    {
      ExclusiveGuardedBufferFrame<BTreeNode> xGuardedRight(
          std::move(guardedNodes[right_hand - pos]));
      ExclusiveGuardedBufferFrame<BTreeNode> xGuardedLeft(
          std::move(guardedNodes[left_hand - pos]));
      xGuardedRight.SyncGSNBeforeWrite();
      xGuardedLeft.SyncGSNBeforeWrite();
      maxRight = left_hand;
      ret = mergeLeftIntoRight(xGuardedParent, left_hand, xGuardedLeft,
                               xGuardedRight, left_hand == pos);
      // we unlock only the left page, the right one should not be touched again
      if (ret == 1) {
        fullyMerged[left_hand - pos] = true;
        WorkerCounters::MyCounters().xmerge_full_counter[mTreeId]++;
        ret_code = XMergeReturnCode::kFullMerge;
      } else if (ret == 2) {
        guardedNodes[left_hand - pos] = std::move(xGuardedLeft);
        WorkerCounters::MyCounters().xmerge_partial_counter[mTreeId]++;
      } else if (ret == 0) {
        break;
      } else {
        ENSURE(false);
      }
    }
  }
  if (guardedChild.mGuard.mState == GuardState::kMoved) {
    guardedChild = std::move(guardedNodes[0]);
  }
  guardedParent = std::move(xGuardedParent);
  return ret_code;
}

// -------------------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------------------
s64 BTreeGeneric::iterateAllPagesRec(GuardedBufferFrame<BTreeNode>& guardedNode,
                                     BTreeNodeCallback inner,
                                     BTreeNodeCallback leaf) {
  if (guardedNode->mIsLeaf) {
    return leaf(guardedNode.ref());
  }
  s64 res = inner(guardedNode.ref());
  for (u16 i = 0; i < guardedNode->mNumSeps; i++) {
    Swip<BTreeNode>& c_swip = guardedNode->getChild(i);
    auto guardedChild = GuardedBufferFrame(guardedNode, c_swip);
    guardedChild.JumpIfModifiedByOthers();
    res += iterateAllPagesRec(guardedChild, inner, leaf);
  }

  Swip<BTreeNode>& c_swip = guardedNode->mRightMostChildSwip;
  auto guardedChild = GuardedBufferFrame(guardedNode, c_swip);
  guardedChild.JumpIfModifiedByOthers();
  res += iterateAllPagesRec(guardedChild, inner, leaf);

  return res;
}

s64 BTreeGeneric::iterateAllPages(BTreeNodeCallback inner,
                                  BTreeNodeCallback leaf) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> guardedParent(mMetaNodeSwip);
      GuardedBufferFrame<BTreeNode> guardedChild(
          guardedParent, guardedParent->mRightMostChildSwip);
      s64 result = iterateAllPagesRec(guardedChild, inner, leaf);
      JUMPMU_RETURN result;
    }
    JUMPMU_CATCH() {
    }
  }
}

u64 BTreeGeneric::getHeight() {
  return mHeight.load();
}

u64 BTreeGeneric::CountEntries() {
  return iterateAllPages([](BTreeNode&) { return 0; },
                         [](BTreeNode& node) { return node.mNumSeps; });
}

u64 BTreeGeneric::countPages() {
  return iterateAllPages([](BTreeNode&) { return 1; },
                         [](BTreeNode&) { return 1; });
}

u64 BTreeGeneric::countInner() {
  return iterateAllPages([](BTreeNode&) { return 1; },
                         [](BTreeNode&) { return 0; });
}

u32 BTreeGeneric::bytesFree() {
  return iterateAllPages(
      [](BTreeNode& inner) { return inner.freeSpaceAfterCompaction(); },
      [](BTreeNode& leaf) { return leaf.freeSpaceAfterCompaction(); });
}

void BTreeGeneric::printInfos(uint64_t totalSize) {
  GuardedBufferFrame<BTreeNode> guardedParent(mMetaNodeSwip);
  GuardedBufferFrame guardedRightMost(guardedParent,
                                      guardedParent->mRightMostChildSwip);
  uint64_t cnt = countPages();
  cout << "nodes:" << cnt << " innerNodes:" << countInner()
       << " space:" << (cnt * BTreeNode::Size()) / (float)totalSize
       << " height:" << mHeight << " rootCnt:" << guardedRightMost->mNumSeps
       << " bytesFree:" << bytesFree() << endl;
}

} // namespace leanstore::storage::btree
