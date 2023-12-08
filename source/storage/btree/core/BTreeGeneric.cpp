#include "BTreeGeneric.hpp"

#include "Config.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "sync-primitives/PageGuard.hpp"
#include "utils/RandomGenerator.hpp"

#include <glog/logging.h>

#include <alloca.h>

using namespace leanstore::storage;

namespace leanstore::storage::btree {

#define ARRAY_ON_STACK(varName, T, N) T* varName = (T*)alloca((N) * sizeof(T));

void BTreeGeneric::create(TREEID btreeId, Config config) {
  this->mTreeId = btreeId;
  this->config = config;
  mMetaNodeSwip = &BufferManager::sInstance->AllocNewPage();

  Guard guard(mMetaNodeSwip.AsBufferFrame().header.mLatch,
              GUARD_STATE::EXCLUSIVE);
  mMetaNodeSwip.AsBufferFrame().header.mKeepInMemory = true;
  mMetaNodeSwip.AsBufferFrame().page.mBTreeId = btreeId;
  guard.unlock();

  auto root_write_guard_h = HybridPageGuard<BTreeNode>(btreeId);
  auto root_write_guard =
      ExclusivePageGuard<BTreeNode>(std::move(root_write_guard_h));
  root_write_guard.init(true);

  HybridPageGuard<BTreeNode> meta_guard(mMetaNodeSwip);
  ExclusivePageGuard meta_page(std::move(meta_guard));
  meta_page->mIsLeaf = false;
  // HACK: use upper of meta node as a swip to the storage root
  meta_page->mRightMostChildSwip = root_write_guard.bf();

  // TODO: write WALs
  root_write_guard.incrementGSN();
  meta_page.incrementGSN();
}

void BTreeGeneric::trySplit(BufferFrame& to_split, s16 favored_split_pos) {
  cr::Worker::my().mLogging.walEnsureEnoughSpace(PAGE_SIZE * 1);
  auto parent_handler = findParentEager(*this, to_split);
  HybridPageGuard<BTreeNode> p_guard =
      parent_handler.getParentReadPageGuard<BTreeNode>();
  HybridPageGuard<BTreeNode> c_guard =
      HybridPageGuard(p_guard, parent_handler.mChildSwip.CastTo<BTreeNode>());
  if (c_guard->mNumSeps <= 1)
    return;

  BTreeNode::SeparatorInfo sep_info;
  if (favored_split_pos < 0 || favored_split_pos >= c_guard->mNumSeps - 1) {
    if (config.mUseBulkInsert) {
      favored_split_pos = c_guard->mNumSeps - 2;
      sep_info =
          BTreeNode::SeparatorInfo{c_guard->getFullKeyLen(favored_split_pos),
                                   static_cast<u16>(favored_split_pos), false};
    } else {
      sep_info = c_guard->findSep();
    }
  } else {
    // Split on a specified position, used by contention management
    sep_info =
        BTreeNode::SeparatorInfo{c_guard->getFullKeyLen(favored_split_pos),
                                 static_cast<u16>(favored_split_pos), false};
  }
  // u8 sep_key[sep_info.length];
  ARRAY_ON_STACK(sep_key, u8, sep_info.length);
  if (isMetaNode(p_guard)) { // root split
    auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
    auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
    assert(mHeight == 1 || !c_x_guard->mIsLeaf);
    // -------------------------------------------------------------------------------------
    // create new root
    auto new_root_h = HybridPageGuard<BTreeNode>(mTreeId, false);
    auto new_root = ExclusivePageGuard<BTreeNode>(std::move(new_root_h));
    auto new_left_node_h = HybridPageGuard<BTreeNode>(mTreeId);
    auto new_left_node =
        ExclusivePageGuard<BTreeNode>(std::move(new_left_node_h));
    // -------------------------------------------------------------------------------------
    if (config.mEnableWal) {
      // TODO: System transactions
      new_root.incrementGSN();
      new_left_node.incrementGSN();
      c_x_guard.incrementGSN();
    } else {
      new_root.markAsDirty();
      new_left_node.markAsDirty();
      c_x_guard.markAsDirty();
    }
    // -------------------------------------------------------------------------------------
    auto exec = [&]() {
      new_root.keepAlive();
      new_root.init(false);
      new_root->mRightMostChildSwip = c_x_guard.bf();
      p_x_guard->mRightMostChildSwip = new_root.bf();
      // -------------------------------------------------------------------------------------
      new_left_node.init(c_x_guard->mIsLeaf);
      c_x_guard->getSep(sep_key, sep_info);
      c_x_guard->split(new_root, new_left_node, sep_info.slot, sep_key,
                       sep_info.length);
    };
    if (config.mEnableWal) {
      auto newRootWalHandler =
          new_root.ReserveWALPayload<WALInitPage>(0, mTreeId);
      newRootWalHandler.SubmitWal();

      auto newLeftWalHandler =
          new_left_node.ReserveWALPayload<WALInitPage>(0, mTreeId);
      newLeftWalHandler.SubmitWal();

      auto parentPageId(new_root.bf()->header.pid);
      auto lhsPageId(new_left_node.bf()->header.pid);
      auto rhsPageId(c_x_guard.bf()->header.pid);

      auto curRightWalHandler = c_x_guard.ReserveWALPayload<WALLogicalSplit>(
          0, parentPageId, lhsPageId, rhsPageId);
      curRightWalHandler.SubmitWal();
      // -------------------------------------------------------------------------------------
      exec();
      // -------------------------------------------------------------------------------------
      auto rootWalHandler = new_root.ReserveWALPayload<WALLogicalSplit>(
          0, parentPageId, lhsPageId, rhsPageId);
      rootWalHandler.SubmitWal();
      // -------------------------------------------------------------------------------------
      auto leftWalHandler = new_left_node.ReserveWALPayload<WALLogicalSplit>(
          0, parentPageId, lhsPageId, rhsPageId);
      leftWalHandler.SubmitWal();
    } else {
      exec();
    }
    // -------------------------------------------------------------------------------------
    mHeight++;
    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().dt_split[mTreeId]++;
    }
    return;
  } else {
    // Parent is not root
    const u16 space_needed_for_separator =
        p_guard->spaceNeeded(sep_info.length, sizeof(SwipType));
    if (p_guard->hasEnoughSpaceFor(
            space_needed_for_separator)) { // Is there enough space in the
                                           // parent for the separator?
      auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
      auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
      // -------------------------------------------------------------------------------------
      p_x_guard->requestSpaceFor(space_needed_for_separator);
      assert(&mMetaNodeSwip.AsBufferFrame() != p_x_guard.bf());
      assert(!p_x_guard->mIsLeaf);
      // -------------------------------------------------------------------------------------
      auto new_left_node_h = HybridPageGuard<BTreeNode>(mTreeId);
      auto new_left_node =
          ExclusivePageGuard<BTreeNode>(std::move(new_left_node_h));
      // -------------------------------------------------------------------------------------
      // Increment GSNs before writing WAL to make sure that these pages marked
      // as dirty regardless of the FLAGS_wal
      if (config.mEnableWal) {
        p_x_guard.incrementGSN();
        new_left_node.incrementGSN();
        c_x_guard.incrementGSN();
      } else {
        p_x_guard.markAsDirty();
        new_left_node.markAsDirty();
        c_x_guard.markAsDirty();
      }
      // -------------------------------------------------------------------------------------
      auto exec = [&]() {
        new_left_node.init(c_x_guard->mIsLeaf);
        c_x_guard->getSep(sep_key, sep_info);
        c_x_guard->split(p_x_guard, new_left_node, sep_info.slot, sep_key,
                         sep_info.length);
      };
      // -------------------------------------------------------------------------------------
      if (config.mEnableWal) {
        auto newLeftWalHandler =
            new_left_node.ReserveWALPayload<WALInitPage>(0, mTreeId);
        newLeftWalHandler.SubmitWal();

        auto parentPageId = p_x_guard.bf()->header.pid;
        auto lhsPageId = new_left_node.bf()->header.pid;
        auto rhsPageId = c_x_guard.bf()->header.pid;

        auto curRightWalHandler = c_x_guard.ReserveWALPayload<WALLogicalSplit>(
            0, parentPageId, lhsPageId, rhsPageId);
        curRightWalHandler.SubmitWal();

        exec();

        auto parentWalHandler = p_x_guard.ReserveWALPayload<WALLogicalSplit>(
            0, parentPageId, lhsPageId, rhsPageId);
        parentWalHandler.SubmitWal();

        newLeftWalHandler =
            new_left_node.ReserveWALPayload<WALInitPage>(0, mTreeId);
        newLeftWalHandler.SubmitWal();

        auto leftWalHandler = new_left_node.ReserveWALPayload<WALLogicalSplit>(
            0, parentPageId, lhsPageId, rhsPageId);
        leftWalHandler.SubmitWal();
      } else {
        exec();
      }
      COUNTERS_BLOCK() {
        WorkerCounters::myCounters().dt_split[mTreeId]++;
      }
    } else {
      p_guard.unlock();
      c_guard.unlock();
      // Must split parent head to make space for separator
      trySplit(*p_guard.mBf);
    }
  }
}

bool BTreeGeneric::tryMerge(BufferFrame& to_merge, bool swizzle_sibling) {
  // pos == p_guard->mNumSeps means that the current node is the upper swip in
  // parent
  auto parent_handler = findParentEager(*this, to_merge);
  HybridPageGuard<BTreeNode> p_guard =
      parent_handler.getParentReadPageGuard<BTreeNode>();
  HybridPageGuard<BTreeNode> c_guard =
      HybridPageGuard(p_guard, parent_handler.mChildSwip.CastTo<BTreeNode>());
  int pos_in_parent = parent_handler.mPosInParent;
  if (isMetaNode(p_guard) ||
      c_guard->freeSpaceAfterCompaction() < BTreeNodeHeader::sUnderFullSize) {
    p_guard.unlock();
    c_guard.unlock();
    return false;
  }
  // -------------------------------------------------------------------------------------
  volatile bool merged_successfully = false;
  if (p_guard->mNumSeps > 1) {
    assert(pos_in_parent <= p_guard->mNumSeps);
    // -------------------------------------------------------------------------------------
    p_guard.JumpIfModifiedByOthers();
    c_guard.JumpIfModifiedByOthers();
    // -------------------------------------------------------------------------------------
    // TODO: write WALs
    auto merge_left = [&]() {
      Swip<BTreeNode>& l_swip = p_guard->getChild(pos_in_parent - 1);
      if (!swizzle_sibling && l_swip.isEVICTED()) {
        return false;
      }
      auto l_guard = HybridPageGuard(p_guard, l_swip);
      auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
      auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
      auto l_x_guard = ExclusivePageGuard(std::move(l_guard));
      // -------------------------------------------------------------------------------------
      ENSURE(c_x_guard->mIsLeaf == l_x_guard->mIsLeaf);
      // -------------------------------------------------------------------------------------
      if (!l_x_guard->merge(pos_in_parent - 1, p_x_guard, c_x_guard)) {
        p_guard = std::move(p_x_guard);
        c_guard = std::move(c_x_guard);
        l_guard = std::move(l_x_guard);
        return false;
      }
      // -------------------------------------------------------------------------------------
      if (config.mEnableWal) {
        p_guard.incrementGSN();
        c_guard.incrementGSN();
        l_guard.incrementGSN();
      } else {
        p_guard.markAsDirty();
        c_guard.markAsDirty();
        l_guard.markAsDirty();
      }
      // -------------------------------------------------------------------------------------
      l_x_guard.reclaim();
      // -------------------------------------------------------------------------------------
      p_guard = std::move(p_x_guard);
      c_guard = std::move(c_x_guard);
      return true;
    };
    auto merge_right = [&]() {
      Swip<BTreeNode>& r_swip = ((pos_in_parent + 1) == p_guard->mNumSeps)
                                    ? p_guard->mRightMostChildSwip
                                    : p_guard->getChild(pos_in_parent + 1);
      if (!swizzle_sibling && r_swip.isEVICTED()) {
        return false;
      }
      auto r_guard = HybridPageGuard(p_guard, r_swip);
      auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
      auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
      auto r_x_guard = ExclusivePageGuard(std::move(r_guard));
      // -------------------------------------------------------------------------------------
      ENSURE(c_x_guard->mIsLeaf == r_x_guard->mIsLeaf);
      // -------------------------------------------------------------------------------------
      if (!c_x_guard->merge(pos_in_parent, p_x_guard, r_x_guard)) {
        p_guard = std::move(p_x_guard);
        c_guard = std::move(c_x_guard);
        r_guard = std::move(r_x_guard);
        return false;
      }
      // -------------------------------------------------------------------------------------
      if (config.mEnableWal) {
        p_guard.incrementGSN();
        c_guard.incrementGSN();
        r_guard.incrementGSN();
      } else {
        p_guard.markAsDirty();
        c_guard.markAsDirty();
        r_guard.markAsDirty();
      }
      // -------------------------------------------------------------------------------------
      c_x_guard.reclaim();
      // -------------------------------------------------------------------------------------
      p_guard = std::move(p_x_guard);
      r_guard = std::move(r_x_guard);
      return true;
    };
    // ATTENTION: don't use c_guard without making sure it was not reclaimed
    // -------------------------------------------------------------------------------------
    if (pos_in_parent > 0) {
      merged_successfully = merged_successfully | merge_left();
    }
    if (!merged_successfully && pos_in_parent < p_guard->mNumSeps) {
      merged_successfully = merged_successfully | merge_right();
    }
  }
  // -------------------------------------------------------------------------------------
  JUMPMU_TRY() {
    HybridPageGuard<BTreeNode> meta_guard(mMetaNodeSwip);
    if (!isMetaNode(p_guard) &&
        p_guard->freeSpaceAfterCompaction() >= BTreeNode::sUnderFullSize) {
      if (tryMerge(*p_guard.mBf, true)) {
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
s16 BTreeGeneric::mergeLeftIntoRight(ExclusivePageGuard<BTreeNode>& parent,
                                     s16 lhsSlotId,
                                     ExclusivePageGuard<BTreeNode>& lhs,
                                     ExclusivePageGuard<BTreeNode>& rhs,
                                     bool full_merge_or_nothing) {
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
    memcpy(reinterpret_cast<u8*>(rhs.PageData()), &tmp, sizeof(BTreeNode));
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
    memcpy(reinterpret_cast<u8*>(lhs.PageData()), &tmp, sizeof(BTreeNode));
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
// -------------------------------------------------------------------------------------
// returns true if it has exclusively locked anything

BTreeGeneric::XMergeReturnCode BTreeGeneric::XMerge(
    HybridPageGuard<BTreeNode>& p_guard, HybridPageGuard<BTreeNode>& c_guard,
    ParentSwipHandler& parent_handler) {
  WorkerCounters::myCounters().dt_researchy[0][1]++;
  if (c_guard->fillFactorAfterCompaction() >= 0.9) {
    return XMergeReturnCode::NOTHING;
  }
  // -------------------------------------------------------------------------------------
  const u8 MAX_MERGE_PAGES = FLAGS_xmerge_k;
  s16 pos = parent_handler.mPosInParent;
  u8 pages_count = 1;
  s16 max_right;
  ARRAY_ON_STACK(guards, HybridPageGuard<BTreeNode>, MAX_MERGE_PAGES);
  ARRAY_ON_STACK(fully_merged, bool, MAX_MERGE_PAGES);
  // -------------------------------------------------------------------------------------
  guards[0] = std::move(c_guard);
  fully_merged[0] = false;
  double total_fill_factor = guards[0]->fillFactorAfterCompaction();
  // -------------------------------------------------------------------------------------
  // Handle upper swip instead of avoiding p_guard->mNumSeps -1 swip
  if (isMetaNode(p_guard) || !guards[0]->mIsLeaf) {
    c_guard = std::move(guards[0]);
    return XMergeReturnCode::NOTHING;
  }
  for (max_right = pos + 1; (max_right - pos) < MAX_MERGE_PAGES &&
                            (max_right + 1) < p_guard->mNumSeps;
       max_right++) {
    if (!p_guard->getChild(max_right).isHOT()) {
      c_guard = std::move(guards[0]);
      return XMergeReturnCode::NOTHING;
    }
    // -------------------------------------------------------------------------------------
    guards[max_right - pos] =
        HybridPageGuard<BTreeNode>(p_guard, p_guard->getChild(max_right));
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
    c_guard = std::move(guards[0]);
    return XMergeReturnCode::NOTHING;
  }
  // -------------------------------------------------------------------------------------
  ExclusivePageGuard<BTreeNode> p_x_guard = std::move(p_guard);
  p_x_guard.incrementGSN();
  // -------------------------------------------------------------------------------------
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
    // -------------------------------------------------------------------------------------
    left_hand = right_hand - 1;
    // -------------------------------------------------------------------------------------
    {
      ExclusivePageGuard<BTreeNode> right_x_guard(
          std::move(guards[right_hand - pos]));
      ExclusivePageGuard<BTreeNode> left_x_guard(
          std::move(guards[left_hand - pos]));
      right_x_guard.incrementGSN();
      left_x_guard.incrementGSN();
      max_right = left_hand;
      ret = mergeLeftIntoRight(p_x_guard, left_hand, left_x_guard,
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
    // -------------------------------------------------------------------------------------
  }
  if (c_guard.guard.state == GUARD_STATE::MOVED)
    c_guard = std::move(guards[0]);
  p_guard = std::move(p_x_guard);
  return ret_code;
}

// -------------------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------------------
s64 BTreeGeneric::iterateAllPagesRec(HybridPageGuard<BTreeNode>& node_guard,
                                     BTreeNodeCallback inner,
                                     BTreeNodeCallback leaf) {
  if (node_guard->mIsLeaf) {
    return leaf(node_guard.ref());
  }
  s64 res = inner(node_guard.ref());
  for (u16 i = 0; i < node_guard->mNumSeps; i++) {
    Swip<BTreeNode>& c_swip = node_guard->getChild(i);
    auto c_guard = HybridPageGuard(node_guard, c_swip);
    c_guard.JumpIfModifiedByOthers();
    res += iterateAllPagesRec(c_guard, inner, leaf);
  }
  // -------------------------------------------------------------------------------------
  Swip<BTreeNode>& c_swip = node_guard->mRightMostChildSwip;
  auto c_guard = HybridPageGuard(node_guard, c_swip);
  c_guard.JumpIfModifiedByOthers();
  res += iterateAllPagesRec(c_guard, inner, leaf);
  // -------------------------------------------------------------------------------------
  return res;
}
// -------------------------------------------------------------------------------------
s64 BTreeGeneric::iterateAllPages(BTreeNodeCallback inner,
                                  BTreeNodeCallback leaf) {
  while (true) {
    JUMPMU_TRY() {
      HybridPageGuard<BTreeNode> p_guard(mMetaNodeSwip);
      HybridPageGuard<BTreeNode> c_guard(p_guard, p_guard->mRightMostChildSwip);
      s64 result = iterateAllPagesRec(c_guard, inner, leaf);
      JUMPMU_RETURN result;
    }
    JUMPMU_CATCH() {
    }
  }
}
// -------------------------------------------------------------------------------------
u64 BTreeGeneric::getHeight() {
  return mHeight.load();
}
// -------------------------------------------------------------------------------------
u64 BTreeGeneric::countEntries() {
  return iterateAllPages([](BTreeNode&) { return 0; },
                         [](BTreeNode& node) { return node.mNumSeps; });
}
// -------------------------------------------------------------------------------------
u64 BTreeGeneric::countPages() {
  return iterateAllPages([](BTreeNode&) { return 1; },
                         [](BTreeNode&) { return 1; });
}
// -------------------------------------------------------------------------------------
u64 BTreeGeneric::countInner() {
  return iterateAllPages([](BTreeNode&) { return 1; },
                         [](BTreeNode&) { return 0; });
}
// -------------------------------------------------------------------------------------
double BTreeGeneric::averageSpaceUsage() {
  ENSURE(false); // TODO
}
// -------------------------------------------------------------------------------------
u32 BTreeGeneric::bytesFree() {
  return iterateAllPages(
      [](BTreeNode& inner) { return inner.freeSpaceAfterCompaction(); },
      [](BTreeNode& leaf) { return leaf.freeSpaceAfterCompaction(); });
}
// -------------------------------------------------------------------------------------
void BTreeGeneric::printInfos(uint64_t totalSize) {
  HybridPageGuard<BTreeNode> p_guard(mMetaNodeSwip);
  HybridPageGuard r_guard(p_guard, p_guard->mRightMostChildSwip);
  uint64_t cnt = countPages();
  cout << "nodes:" << cnt << " innerNodes:" << countInner()
       << " space:" << (cnt * EFFECTIVE_PAGE_SIZE) / (float)totalSize
       << " height:" << mHeight << " rootCnt:" << r_guard->mNumSeps
       << " bytesFree:" << bytesFree() << endl;
}
// -------------------------------------------------------------------------------------
} // namespace leanstore::storage::btree
