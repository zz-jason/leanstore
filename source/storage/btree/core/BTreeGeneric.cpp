#include "BTreeGeneric.hpp"

#include "Config.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "storage/buffer-manager/GuardedBufferFrame.hpp"
#include "utils/RandomGenerator.hpp"

#include <glog/logging.h>

#include <alloca.h>

using namespace leanstore::storage;

namespace leanstore::storage::btree {

#define ARRAY_ON_STACK(varName, T, N) T* varName = (T*)alloca((N) * sizeof(T));

void BTreeGeneric::Init(TREEID btreeId, Config config) {
  this->mTreeId = btreeId;
  this->config = config;
  mMetaNodeSwip = &BufferManager::sInstance->AllocNewPage();

  HybridGuard guard(mMetaNodeSwip.AsBufferFrame().header.mLatch,
                    GUARD_STATE::EXCLUSIVE);
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

  xGuardedRoot.IncPageGSN();
  xGuardedMeta.IncPageGSN();
}

void BTreeGeneric::trySplit(BufferFrame& toSplit, s16 favoredSplitPos) {
  cr::Worker::my().mLogging.walEnsureEnoughSpace(PAGE_SIZE * 1);
  auto parentHandler = findParentEager(*this, toSplit);
  auto guardedParent = parentHandler.getParentReadPageGuard<BTreeNode>();
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
  // u8 sepKey[sepInfo.length];
  ARRAY_ON_STACK(sepKey, u8, sepInfo.length);
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
      xGuardedNewRoot.IncPageGSN();
      xGuardedNewLeft.IncPageGSN();
      xGuardedChild.IncPageGSN();
    } else {
      xGuardedNewRoot.MarkAsDirty();
      xGuardedNewLeft.MarkAsDirty();
      xGuardedChild.MarkAsDirty();
    }

    auto exec = [&]() {
      xGuardedNewRoot.keepAlive();
      xGuardedNewRoot.InitPayload(false);
      xGuardedNewRoot->mRightMostChildSwip = xGuardedChild.bf();
      xGuardedParent->mRightMostChildSwip = xGuardedNewRoot.bf();

      xGuardedNewLeft.InitPayload(xGuardedChild->mIsLeaf);
      xGuardedChild->getSep(sepKey, sepInfo);
      xGuardedChild->split(xGuardedNewRoot, xGuardedNewLeft, sepInfo.slot,
                           sepKey, sepInfo.length);
    };
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

      exec();

      auto rootWalHandler = xGuardedNewRoot.ReserveWALPayload<WALLogicalSplit>(
          0, parentPageId, lhsPageId, rhsPageId);
      rootWalHandler.SubmitWal();

      auto leftWalHandler = xGuardedNewLeft.ReserveWALPayload<WALLogicalSplit>(
          0, parentPageId, lhsPageId, rhsPageId);
      leftWalHandler.SubmitWal();
    } else {
      exec();
    }

    mHeight++;
    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().dt_split[mTreeId]++;
    }
    return;
  } else {
    // Parent is not root
    const u16 space_needed_for_separator =
        guardedParent->spaceNeeded(sepInfo.length, sizeof(SwipType));
    if (guardedParent->hasEnoughSpaceFor(space_needed_for_separator)) {
      // Is there enough space in the parent for the separator?
      auto xGuardedParent =
          ExclusiveGuardedBufferFrame(std::move(guardedParent));
      auto xGuardedChild = ExclusiveGuardedBufferFrame(std::move(guardedChild));

      xGuardedParent->requestSpaceFor(space_needed_for_separator);
      DCHECK(&mMetaNodeSwip.AsBufferFrame() != xGuardedParent.bf());
      DCHECK(!xGuardedParent->mIsLeaf);

      auto guardedNewLeft = GuardedBufferFrame<BTreeNode>(mTreeId);
      auto xGuardedNewLeft =
          ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNewLeft));

      // Increment GSNs before writing WAL to make sure that these pages marked
      // as dirty regardless of the FLAGS_wal
      if (config.mEnableWal) {
        xGuardedParent.IncPageGSN();
        xGuardedNewLeft.IncPageGSN();
        xGuardedChild.IncPageGSN();
      } else {
        xGuardedParent.MarkAsDirty();
        xGuardedNewLeft.MarkAsDirty();
        xGuardedChild.MarkAsDirty();
      }

      auto exec = [&]() {
        xGuardedNewLeft.InitPayload(xGuardedChild->mIsLeaf);
        xGuardedChild->getSep(sepKey, sepInfo);
        xGuardedChild->split(xGuardedParent, xGuardedNewLeft, sepInfo.slot,
                             sepKey, sepInfo.length);
      };

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

        exec();

        auto parentWalHandler =
            xGuardedParent.ReserveWALPayload<WALLogicalSplit>(
                0, parentPageId, lhsPageId, rhsPageId);
        parentWalHandler.SubmitWal();

        newLeftWalHandler = xGuardedNewLeft.ReserveWALPayload<WALInitPage>(
            0, mTreeId, xGuardedNewLeft->mIsLeaf);
        newLeftWalHandler.SubmitWal();

        auto leftWalHandler =
            xGuardedNewLeft.ReserveWALPayload<WALLogicalSplit>(
                0, parentPageId, lhsPageId, rhsPageId);
        leftWalHandler.SubmitWal();
      } else {
        exec();
      }
      COUNTERS_BLOCK() {
        WorkerCounters::myCounters().dt_split[mTreeId]++;
      }
    } else {
      guardedParent.unlock();
      guardedChild.unlock();
      // Must split parent head to make space for separator
      trySplit(*guardedParent.mBf);
    }
  }
}

bool BTreeGeneric::tryMerge(BufferFrame& to_merge, bool swizzle_sibling) {
  // pos == guardedParent->mNumSeps means that the current node is the upper
  // swip in parent
  auto parentHandler = findParentEager(*this, to_merge);
  GuardedBufferFrame<BTreeNode> guardedParent =
      parentHandler.getParentReadPageGuard<BTreeNode>();
  GuardedBufferFrame<BTreeNode> guardedChild = GuardedBufferFrame(
      guardedParent, parentHandler.mChildSwip.CastTo<BTreeNode>());
  int pos_in_parent = parentHandler.mPosInParent;
  if (isMetaNode(guardedParent) || guardedChild->freeSpaceAfterCompaction() <
                                       BTreeNodeHeader::sUnderFullSize) {
    guardedParent.unlock();
    guardedChild.unlock();
    return false;
  }
  // -------------------------------------------------------------------------------------
  volatile bool merged_successfully = false;
  if (guardedParent->mNumSeps > 1) {
    assert(pos_in_parent <= guardedParent->mNumSeps);
    // -------------------------------------------------------------------------------------
    guardedParent.JumpIfModifiedByOthers();
    guardedChild.JumpIfModifiedByOthers();
    // -------------------------------------------------------------------------------------
    // TODO: write WALs
    auto merge_left = [&]() {
      Swip<BTreeNode>& l_swip = guardedParent->getChild(pos_in_parent - 1);
      if (!swizzle_sibling && l_swip.isEVICTED()) {
        return false;
      }
      auto l_guard = GuardedBufferFrame(guardedParent, l_swip);
      auto xGuardedParent =
          ExclusiveGuardedBufferFrame(std::move(guardedParent));
      auto xGuardedChild = ExclusiveGuardedBufferFrame(std::move(guardedChild));
      auto l_x_guard = ExclusiveGuardedBufferFrame(std::move(l_guard));
      // -------------------------------------------------------------------------------------
      ENSURE(xGuardedChild->mIsLeaf == l_x_guard->mIsLeaf);
      // -------------------------------------------------------------------------------------
      if (!l_x_guard->merge(pos_in_parent - 1, xGuardedParent, xGuardedChild)) {
        guardedParent = std::move(xGuardedParent);
        guardedChild = std::move(xGuardedChild);
        l_guard = std::move(l_x_guard);
        return false;
      }
      // -------------------------------------------------------------------------------------
      if (config.mEnableWal) {
        guardedParent.IncPageGSN();
        guardedChild.IncPageGSN();
        l_guard.IncPageGSN();
      } else {
        guardedParent.MarkAsDirty();
        guardedChild.MarkAsDirty();
        l_guard.MarkAsDirty();
      }
      // -------------------------------------------------------------------------------------
      l_x_guard.reclaim();
      // -------------------------------------------------------------------------------------
      guardedParent = std::move(xGuardedParent);
      guardedChild = std::move(xGuardedChild);
      return true;
    };
    auto merge_right = [&]() {
      Swip<BTreeNode>& r_swip =
          ((pos_in_parent + 1) == guardedParent->mNumSeps)
              ? guardedParent->mRightMostChildSwip
              : guardedParent->getChild(pos_in_parent + 1);
      if (!swizzle_sibling && r_swip.isEVICTED()) {
        return false;
      }
      auto r_guard = GuardedBufferFrame(guardedParent, r_swip);
      auto xGuardedParent =
          ExclusiveGuardedBufferFrame(std::move(guardedParent));
      auto xGuardedChild = ExclusiveGuardedBufferFrame(std::move(guardedChild));
      auto r_x_guard = ExclusiveGuardedBufferFrame(std::move(r_guard));
      // -------------------------------------------------------------------------------------
      ENSURE(xGuardedChild->mIsLeaf == r_x_guard->mIsLeaf);
      // -------------------------------------------------------------------------------------
      if (!xGuardedChild->merge(pos_in_parent, xGuardedParent, r_x_guard)) {
        guardedParent = std::move(xGuardedParent);
        guardedChild = std::move(xGuardedChild);
        r_guard = std::move(r_x_guard);
        return false;
      }
      // -------------------------------------------------------------------------------------
      if (config.mEnableWal) {
        guardedParent.IncPageGSN();
        guardedChild.IncPageGSN();
        r_guard.IncPageGSN();
      } else {
        guardedParent.MarkAsDirty();
        guardedChild.MarkAsDirty();
        r_guard.MarkAsDirty();
      }
      // -------------------------------------------------------------------------------------
      xGuardedChild.reclaim();
      // -------------------------------------------------------------------------------------
      guardedParent = std::move(xGuardedParent);
      r_guard = std::move(r_x_guard);
      return true;
    };
    // ATTENTION: don't use guardedChild without making sure it was not
    // reclaimed
    // -------------------------------------------------------------------------------------
    if (pos_in_parent > 0) {
      merged_successfully = merged_successfully | merge_left();
    }
    if (!merged_successfully && pos_in_parent < guardedParent->mNumSeps) {
      merged_successfully = merged_successfully | merge_right();
    }
  }
  // -------------------------------------------------------------------------------------
  JUMPMU_TRY() {
    GuardedBufferFrame<BTreeNode> guardedMeta(mMetaNodeSwip);
    if (!isMetaNode(guardedParent) &&
        guardedParent->freeSpaceAfterCompaction() >=
            BTreeNode::sUnderFullSize) {
      if (tryMerge(*guardedParent.mBf, true)) {
        WorkerCounters::myCounters().dt_merge_parent_succ[mTreeId]++;
      } else {
        WorkerCounters::myCounters().dt_merge_parent_fail[mTreeId]++;
      }
    }
  }
  JUMPMU_CATCH() {
    WorkerCounters::myCounters().dt_merge_fail[mTreeId]++;
  }
  // -------------------------------------------------------------------------------------
  COUNTERS_BLOCK() {
    if (merged_successfully) {
      WorkerCounters::myCounters().dt_merge_succ[mTreeId]++;
    } else {
      WorkerCounters::myCounters().dt_merge_fail[mTreeId]++;
    }
  }
  return merged_successfully;
}

// ret: 0 did nothing, 1 full, 2 partial
s16 BTreeGeneric::mergeLeftIntoRight(
    ExclusiveGuardedBufferFrame<BTreeNode>& parent, s16 lhsSlotId,
    ExclusiveGuardedBufferFrame<BTreeNode>& lhs,
    ExclusiveGuardedBufferFrame<BTreeNode>& rhs, bool full_merge_or_nothing) {
  // TODO: corner cases: new upper fence is larger than the older one.
  u32 space_upper_bound = lhs->mergeSpaceUpperBound(rhs);
  if (space_upper_bound <= EFFECTIVE_PAGE_SIZE) {
    // Do a full merge TODO: threshold
    bool succ = lhs->merge(lhsSlotId, parent, rhs);
    static_cast<void>(succ);
    assert(succ);
    lhs.reclaim();
    return 1;
  }

  if (full_merge_or_nothing)
    return 0;

  // Do a partial merge
  // Remove a key at a time from the merge and check if now it fits
  s16 till_slot_id = -1;
  for (s16 s_i = 0; s_i < lhs->mNumSeps; s_i++) {
    space_upper_bound -= sizeof(BTreeNode::Slot) +
                         lhs->KeySizeWithoutPrefix(s_i) + lhs->ValSize(s_i);
    if (space_upper_bound +
            (lhs->getFullKeyLen(s_i) - rhs->mLowerFence.length) <
        EFFECTIVE_PAGE_SIZE * 1.0) {
      till_slot_id = s_i + 1;
      break;
    }
  }
  if (!(till_slot_id != -1 && till_slot_id < (lhs->mNumSeps - 1)))
    return 0; // false

  assert((space_upper_bound +
          (lhs->getFullKeyLen(till_slot_id - 1) - rhs->mLowerFence.length)) <
         EFFECTIVE_PAGE_SIZE * 1.0);
  assert(till_slot_id > 0);
  // -------------------------------------------------------------------------------------
  u16 copy_from_count = lhs->mNumSeps - till_slot_id;
  // -------------------------------------------------------------------------------------
  u16 new_left_uf_length = lhs->getFullKeyLen(till_slot_id - 1);
  ENSURE(new_left_uf_length > 0);
  ARRAY_ON_STACK(new_left_uf_key, u8, new_left_uf_length);
  lhs->copyFullKey(till_slot_id - 1, new_left_uf_key);
  // -------------------------------------------------------------------------------------
  if (!parent->prepareInsert(new_left_uf_length, 0))
    return 0; // false
  // -------------------------------------------------------------------------------------
  {
    BTreeNode tmp(true);
    tmp.setFences(Slice(new_left_uf_key, new_left_uf_length),
                  rhs->GetUpperFence());
    // -------------------------------------------------------------------------------------
    lhs->copyKeyValueRange(&tmp, 0, till_slot_id, copy_from_count);
    rhs->copyKeyValueRange(&tmp, copy_from_count, 0, rhs->mNumSeps);
    memcpy(rhs.GetPagePayloadPtr(), &tmp, sizeof(BTreeNode));
    rhs->makeHint();
    // -------------------------------------------------------------------------------------
    // Nothing to do for the right node's separator
    assert(rhs->compareKeyWithBoundaries(
               Slice(new_left_uf_key, new_left_uf_length)) == 1);
  }
  {
    BTreeNode tmp(true);
    tmp.setFences(lhs->GetLowerFence(),
                  Slice(new_left_uf_key, new_left_uf_length));
    // -------------------------------------------------------------------------------------
    lhs->copyKeyValueRange(&tmp, 0, 0, lhs->mNumSeps - copy_from_count);
    memcpy(lhs.GetPagePayloadPtr(), &tmp, sizeof(BTreeNode));
    lhs->makeHint();
    // -------------------------------------------------------------------------------------
    assert(lhs->compareKeyWithBoundaries(
               Slice(new_left_uf_key, new_left_uf_length)) == 0);
    // -------------------------------------------------------------------------------------
    parent->removeSlot(lhsSlotId);
    ENSURE(parent->prepareInsert(lhs->mUpperFence.length, sizeof(SwipType)));
    auto swip = lhs.swip();
    Slice key(lhs->getUpperFenceKey(), lhs->mUpperFence.length);
    Slice val(reinterpret_cast<u8*>(&swip), sizeof(SwipType));
    parent->insert(key, val);
  }
  return 2;
}

// returns true if it has exclusively locked anything

BTreeGeneric::XMergeReturnCode BTreeGeneric::XMerge(
    GuardedBufferFrame<BTreeNode>& guardedParent,
    GuardedBufferFrame<BTreeNode>& guardedChild,
    ParentSwipHandler& parentHandler) {
  WorkerCounters::myCounters().dt_researchy[0][1]++;
  if (guardedChild->fillFactorAfterCompaction() >= 0.9) {
    return XMergeReturnCode::NOTHING;
  }

  const u8 MAX_MERGE_PAGES = FLAGS_xmerge_k;
  s16 pos = parentHandler.mPosInParent;
  u8 pages_count = 1;
  s16 max_right;
  ARRAY_ON_STACK(guards, GuardedBufferFrame<BTreeNode>, MAX_MERGE_PAGES);
  ARRAY_ON_STACK(fully_merged, bool, MAX_MERGE_PAGES);

  guards[0] = std::move(guardedChild);
  fully_merged[0] = false;
  double total_fill_factor = guards[0]->fillFactorAfterCompaction();

  // Handle upper swip instead of avoiding guardedParent->mNumSeps -1 swip
  if (isMetaNode(guardedParent) || !guards[0]->mIsLeaf) {
    guardedChild = std::move(guards[0]);
    return XMergeReturnCode::NOTHING;
  }
  for (max_right = pos + 1; (max_right - pos) < MAX_MERGE_PAGES &&
                            (max_right + 1) < guardedParent->mNumSeps;
       max_right++) {
    if (!guardedParent->getChild(max_right).isHOT()) {
      guardedChild = std::move(guards[0]);
      return XMergeReturnCode::NOTHING;
    }

    guards[max_right - pos] = GuardedBufferFrame<BTreeNode>(
        guardedParent, guardedParent->getChild(max_right));
    fully_merged[max_right - pos] = false;
    total_fill_factor += guards[max_right - pos]->fillFactorAfterCompaction();
    pages_count++;
    if ((pages_count - std::ceil(total_fill_factor)) >= (1)) {
      // we can probably save a page by merging all together so there is no need
      // to look furhter
      break;
    }
  }
  if (((pages_count - std::ceil(total_fill_factor))) < (1)) {
    guardedChild = std::move(guards[0]);
    return XMergeReturnCode::NOTHING;
  }

  ExclusiveGuardedBufferFrame<BTreeNode> xGuardedParent =
      std::move(guardedParent);
  xGuardedParent.IncPageGSN();

  XMergeReturnCode ret_code = XMergeReturnCode::PARTIAL_MERGE;
  s16 left_hand, right_hand, ret;
  while (true) {
    for (right_hand = max_right; right_hand > pos; right_hand--) {
      if (fully_merged[right_hand - pos]) {
        continue;
      } else {
        break;
      }
    }
    if (right_hand == pos)
      break;

    left_hand = right_hand - 1;

    {
      ExclusiveGuardedBufferFrame<BTreeNode> right_x_guard(
          std::move(guards[right_hand - pos]));
      ExclusiveGuardedBufferFrame<BTreeNode> left_x_guard(
          std::move(guards[left_hand - pos]));
      right_x_guard.IncPageGSN();
      left_x_guard.IncPageGSN();
      max_right = left_hand;
      ret = mergeLeftIntoRight(xGuardedParent, left_hand, left_x_guard,
                               right_x_guard, left_hand == pos);
      // we unlock only the left page, the right one should not be touched again
      if (ret == 1) {
        fully_merged[left_hand - pos] = true;
        WorkerCounters::myCounters().xmerge_full_counter[mTreeId]++;
        ret_code = XMergeReturnCode::FULL_MERGE;
      } else if (ret == 2) {
        guards[left_hand - pos] = std::move(left_x_guard);
        WorkerCounters::myCounters().xmerge_partial_counter[mTreeId]++;
      } else if (ret == 0) {
        break;
      } else {
        ENSURE(false);
      }
    }
  }
  if (guardedChild.mGuard.mState == GUARD_STATE::MOVED) {
    guardedChild = std::move(guards[0]);
  }
  guardedParent = std::move(xGuardedParent);
  return ret_code;
}

// -------------------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------------------
s64 BTreeGeneric::iterateAllPagesRec(GuardedBufferFrame<BTreeNode>& node_guard,
                                     BTreeNodeCallback inner,
                                     BTreeNodeCallback leaf) {
  if (node_guard->mIsLeaf) {
    return leaf(node_guard.ref());
  }
  s64 res = inner(node_guard.ref());
  for (u16 i = 0; i < node_guard->mNumSeps; i++) {
    Swip<BTreeNode>& c_swip = node_guard->getChild(i);
    auto guardedChild = GuardedBufferFrame(node_guard, c_swip);
    guardedChild.JumpIfModifiedByOthers();
    res += iterateAllPagesRec(guardedChild, inner, leaf);
  }

  Swip<BTreeNode>& c_swip = node_guard->mRightMostChildSwip;
  auto guardedChild = GuardedBufferFrame(node_guard, c_swip);
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

u64 BTreeGeneric::countEntries() {
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

double BTreeGeneric::averageSpaceUsage() {
  ENSURE(false); // TODO
}

u32 BTreeGeneric::bytesFree() {
  return iterateAllPages(
      [](BTreeNode& inner) { return inner.freeSpaceAfterCompaction(); },
      [](BTreeNode& leaf) { return leaf.freeSpaceAfterCompaction(); });
}

void BTreeGeneric::printInfos(uint64_t totalSize) {
  GuardedBufferFrame<BTreeNode> guardedParent(mMetaNodeSwip);
  GuardedBufferFrame r_guard(guardedParent, guardedParent->mRightMostChildSwip);
  uint64_t cnt = countPages();
  cout << "nodes:" << cnt << " innerNodes:" << countInner()
       << " space:" << (cnt * EFFECTIVE_PAGE_SIZE) / (float)totalSize
       << " height:" << mHeight << " rootCnt:" << r_guard->mNumSeps
       << " bytesFree:" << bytesFree() << endl;
}

} // namespace leanstore::storage::btree
