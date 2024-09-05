#pragma once

#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "leanstore/btree/core/BTreeNode.hpp"
#include "leanstore/buffer-manager/BufferManager.hpp"
#include "leanstore/buffer-manager/GuardedBufferFrame.hpp"
#include "leanstore/buffer-manager/TreeRegistry.hpp"
#include "leanstore/sync/HybridLatch.hpp"
#include "leanstore/utils/Log.hpp"

#include <rapidjson/document.h>

#include <atomic>
#include <limits>

namespace leanstore::storage::btree {

enum class BTreeType : uint8_t { kGeneric = 0, kBasicKV = 1, kTransactionKV = 2 };

class PessimisticSharedIterator;
class PessimisticExclusiveIterator;
using BTreeNodeCallback = std::function<int64_t(BTreeNode&)>;

class BTreeGeneric : public leanstore::storage::BufferManagedTree {
public:
  friend class PessimisticIterator;

  enum class XMergeReturnCode : uint8_t { kNothing, kFullMerge, kPartialMerge };

public:
  leanstore::LeanStore* mStore;

  TREEID mTreeId;

  BTreeType mTreeType = BTreeType::kGeneric;

  BTreeConfig mConfig;

  //! Owns the meta node of the tree. The right-most child of meta node is the root of the tree.
  Swip mMetaNodeSwip;

  std::atomic<uint64_t> mHeight = 1;

public:
  BTreeGeneric() = default;

  virtual ~BTreeGeneric() override = default;

public:
  void Init(leanstore::LeanStore* store, TREEID treeId, BTreeConfig config);

  PessimisticSharedIterator GetIterator();

  PessimisticExclusiveIterator GetExclusiveIterator();

  //! Try to merge the current node with its left or right sibling, reclaim the merged left or right
  //! sibling if successful.
  bool TryMergeMayJump(TXID sysTxId, BufferFrame& toMerge, bool swizzleSibling = true);

  void TrySplitMayJump(TXID sysTxId, BufferFrame& toSplit, int16_t pos = -1);

  XMergeReturnCode XMerge(GuardedBufferFrame<BTreeNode>& guardedParent,
                          GuardedBufferFrame<BTreeNode>& guardedChild,
                          ParentSwipHandler& parentSwipHandler);

  uint64_t CountInnerPages() {
    return iterateAllPages([](BTreeNode&) { return 1; }, [](BTreeNode&) { return 0; });
  }

  uint64_t CountAllPages() {
    return iterateAllPages([](BTreeNode&) { return 1; }, [](BTreeNode&) { return 1; });
  }

  uint64_t CountEntries() {
    return iterateAllPages([](BTreeNode&) { return 0; },
                           [](BTreeNode& node) { return node.mNumSlots; });
  }

  uint64_t GetHeight() {
    return mHeight.load();
  }

  uint32_t FreeSpaceAfterCompaction() {
    return iterateAllPages([](BTreeNode& inner) { return inner.FreeSpaceAfterCompaction(); },
                           [](BTreeNode& leaf) { return leaf.FreeSpaceAfterCompaction(); });
  }

  //! Get a summary of the BTree
  std::string Summary();

  // for buffer manager
  virtual void IterateChildSwips(BufferFrame& bf, std::function<bool(Swip&)> callback) override;

  virtual ParentSwipHandler FindParent(BufferFrame& childBf) override {
    return BTreeGeneric::findParentMayJump(*this, childBf);
  }

  //! Returns true if the buffer manager has to restart and pick another buffer
  //! frame for eviction Attention: the guards here down the stack are not
  //! synchronized with the ones in the buffer frame manager stack frame
  ///
  //! Called by buffer manager before eviction
  virtual SpaceCheckResult CheckSpaceUtilization(BufferFrame& bf) override;

  //! Flush the page content in the buffer frame to disk
  ///
  //! NOTE: The source buffer frame should be shared latched
  virtual void Checkpoint(BufferFrame& bf, void* dest) override;

  virtual void undo(const uint8_t*, const uint64_t) override {
    Log::Fatal("undo is unsupported");
  }

  virtual void GarbageCollect(const uint8_t*, WORKERID, TXID, bool) override {
    Log::Fatal("GarbageCollect is unsupported");
  }

  virtual void unlock(const uint8_t*) override {
    Log::Fatal("unlock is unsupported");
  }

  virtual StringMap Serialize() override;

  virtual void Deserialize(StringMap map) override;

private:
  inline bool isMetaNode(GuardedBufferFrame<BTreeNode>& guardedNode) {
    return mMetaNodeSwip == guardedNode.mBf;
  }

  inline bool isMetaNode(ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedNode) {
    return mMetaNodeSwip == xGuardedNode.bf();
  }

  //! Split the root node, 4 nodes are involved in the split:
  //! meta(oldRoot) -> meta(newRoot(newLeft, oldRoot))
  ///
  //! meta         meta
  //!   |            |
  //! toSplit      newRoot
  //!              |     |
  //!           newLeft toSplit
  ///
  void splitRootMayJump(TXID sysTxId, GuardedBufferFrame<BTreeNode>& guardedParent,
                        GuardedBufferFrame<BTreeNode>& guardedChild,
                        const BTreeNode::SeparatorInfo& sepInfo);

  //! Split a non-root node, 3 nodes are involved in the split:
  //! parent(toSplit) -> parent(newLeft, toSplit)
  ///
  //! parent         parent
  //!   |            |   |
  //! toSplit   newLeft toSplit
  ///
  void splitNonRootMayJump(TXID sysTxId, GuardedBufferFrame<BTreeNode>& guardedParent,
                           GuardedBufferFrame<BTreeNode>& guardedChild,
                           const BTreeNode::SeparatorInfo& sepInfo,
                           uint16_t spaceNeededForSeparator);

  int64_t iterateAllPages(BTreeNodeCallback inner, BTreeNodeCallback leaf);

  int64_t iterateAllPagesRecursive(GuardedBufferFrame<BTreeNode>& guardedNode,
                                   BTreeNodeCallback inner, BTreeNodeCallback leaf);

  int16_t mergeLeftIntoRight(ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedParent,
                             int16_t leftPos, ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedLeft,
                             ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedRight,
                             bool fullMergeOrNothing);

public:
  // Helpers
  inline void FindLeafCanJump(Slice key, GuardedBufferFrame<BTreeNode>& guardedTarget,
                              LatchMode mode = LatchMode::kPessimisticShared);

public:
  //! Note on Synchronization: it is called by the page provide thread which are not allowed to
  //! block. Therefore, we jump whenever we encounter a latched node on our way Moreover, we jump if
  //! any page on the path is already evicted or of the bf could not be found Pre: bfToFind is not
  //! exclusively latched
  //! @param jumpIfEvicted
  //! @param btree the target tree which the parent is on
  //! @param bfToFind the target node to find parent for
  template <bool jumpIfEvicted = true>
  static ParentSwipHandler FindParent(BTreeGeneric& btree, BufferFrame& bfToFind);

  //! Removes a btree from disk, reclaim all the buffer frames in memory and
  //! pages in disk used by it.
  ///
  //! @param btree The tree to free.
  static void FreeAndReclaim(BTreeGeneric& btree) {
    GuardedBufferFrame<BTreeNode> guardedMetaNode(btree.mStore->mBufferManager.get(),
                                                  btree.mMetaNodeSwip);
    GuardedBufferFrame<BTreeNode> guardedRootNode(
        btree.mStore->mBufferManager.get(), guardedMetaNode, guardedMetaNode->mRightMostChildSwip);
    BTreeGeneric::freeBTreeNodesRecursive(btree, guardedRootNode);

    auto xGuardedMeta = ExclusiveGuardedBufferFrame(std::move(guardedMetaNode));
    xGuardedMeta.Reclaim();
  }

  static void ToJson(BTreeGeneric& btree, rapidjson::Document* resultDoc);

private:
  static void freeBTreeNodesRecursive(BTreeGeneric& btree,
                                      GuardedBufferFrame<BTreeNode>& guardedNode);

  static void toJsonRecursive(BTreeGeneric& btree, GuardedBufferFrame<BTreeNode>& guardedNode,
                              rapidjson::Value* resultObj,
                              rapidjson::Value::AllocatorType& allocator);

  static ParentSwipHandler findParentMayJump(BTreeGeneric& btree, BufferFrame& bfToFind) {
    return FindParent<true>(btree, bfToFind);
  }

  static ParentSwipHandler findParentEager(BTreeGeneric& btree, BufferFrame& bfToFind) {
    return FindParent<false>(btree, bfToFind);
  }

public:
  static constexpr std::string kTreeId = "treeId";
  static constexpr std::string kHeight = "height";
  static constexpr std::string kMetaPageId = "metaPageId";
};

inline void BTreeGeneric::freeBTreeNodesRecursive(BTreeGeneric& btree,
                                                  GuardedBufferFrame<BTreeNode>& guardedNode) {
  if (!guardedNode->mIsLeaf) {
    for (auto i = 0u; i <= guardedNode->mNumSlots; ++i) {
      auto* childSwip = guardedNode->ChildSwipIncludingRightMost(i);
      GuardedBufferFrame<BTreeNode> guardedChild(btree.mStore->mBufferManager.get(), guardedNode,
                                                 *childSwip);
      freeBTreeNodesRecursive(btree, guardedChild);
    }
  }

  auto xGuardedNode = ExclusiveGuardedBufferFrame(std::move(guardedNode));
  xGuardedNode.Reclaim();
}

inline void BTreeGeneric::IterateChildSwips(BufferFrame& bf, std::function<bool(Swip&)> callback) {
  // Pre: bf is read locked
  auto& btreeNode = *reinterpret_cast<BTreeNode*>(bf.mPage.mPayload);
  if (btreeNode.mIsLeaf) {
    return;
  }
  for (uint16_t i = 0; i < btreeNode.mNumSlots; i++) {
    if (!callback(*btreeNode.ChildSwip(i))) {
      return;
    }
  }
  if (btreeNode.mRightMostChildSwip != nullptr) {
    callback(btreeNode.mRightMostChildSwip);
  }
}

inline SpaceCheckResult BTreeGeneric::CheckSpaceUtilization(BufferFrame& bf) {
  if (!mStore->mStoreOption->mEnableXMerge) {
    return SpaceCheckResult::kNothing;
  }

  ParentSwipHandler parentHandler = BTreeGeneric::findParentMayJump(*this, bf);
  GuardedBufferFrame<BTreeNode> guardedParent(
      mStore->mBufferManager.get(), std::move(parentHandler.mParentGuard), parentHandler.mParentBf);
  GuardedBufferFrame<BTreeNode> guardedChild(mStore->mBufferManager.get(), guardedParent,
                                             parentHandler.mChildSwip,
                                             LatchMode::kOptimisticOrJump);
  auto mergeResult = XMerge(guardedParent, guardedChild, parentHandler);
  guardedParent.unlock();
  guardedChild.unlock();

  if (mergeResult == XMergeReturnCode::kNothing) {
    return SpaceCheckResult::kNothing;
  }
  return SpaceCheckResult::kPickAnotherBf;
}

inline void BTreeGeneric::Checkpoint(BufferFrame& bf, void* dest) {
  std::memcpy(dest, &bf.mPage, mStore->mStoreOption->mPageSize);
  auto* destPage = reinterpret_cast<Page*>(dest);
  auto* destNode = reinterpret_cast<BTreeNode*>(destPage->mPayload);

  if (!destNode->mIsLeaf) {
    // Replace all child swip to their page ID
    for (uint64_t i = 0; i < destNode->mNumSlots; i++) {
      if (!destNode->ChildSwip(i)->IsEvicted()) {
        auto& childBf = destNode->ChildSwip(i)->AsBufferFrameMasked();
        destNode->ChildSwip(i)->Evict(childBf.mHeader.mPageId);
      }
    }
    // Replace right most child swip to page id
    if (destNode->mRightMostChildSwip != nullptr && !destNode->mRightMostChildSwip.IsEvicted()) {
      auto& childBf = destNode->mRightMostChildSwip.AsBufferFrameMasked();
      destNode->mRightMostChildSwip.Evict(childBf.mHeader.mPageId);
    }
  }
}

inline void BTreeGeneric::FindLeafCanJump(Slice key, GuardedBufferFrame<BTreeNode>& guardedTarget,
                                          LatchMode mode) {
  guardedTarget.unlock();
  auto* bufferManager = mStore->mBufferManager.get();

  // meta node
  GuardedBufferFrame<BTreeNode> guardedParent(bufferManager, mMetaNodeSwip,
                                              LatchMode::kOptimisticSpin);

  // root node
  guardedTarget = GuardedBufferFrame<BTreeNode>(
      bufferManager, guardedParent, guardedParent->mRightMostChildSwip, LatchMode::kOptimisticSpin);

  volatile uint16_t level = 0;
  while (!guardedTarget->mIsLeaf) {
    auto& childSwip = guardedTarget->LookupInner(key);
    LS_DCHECK(!childSwip.IsEmpty());
    guardedParent = std::move(guardedTarget);
    if (level == mHeight - 1) {
      guardedTarget = GuardedBufferFrame<BTreeNode>(bufferManager, guardedParent, childSwip, mode);
    } else {
      // middle node
      guardedTarget = GuardedBufferFrame<BTreeNode>(bufferManager, guardedParent, childSwip,
                                                    LatchMode::kOptimisticSpin);
    }
    level = level + 1;
  }

  guardedParent.unlock();
}

template <bool jumpIfEvicted>
inline ParentSwipHandler BTreeGeneric::FindParent(BTreeGeneric& btree, BufferFrame& bfToFind) {
  // Check whether search on the wrong tree or the root node is evicted
  GuardedBufferFrame<BTreeNode> guardedParent(btree.mStore->mBufferManager.get(),
                                              btree.mMetaNodeSwip);
  if (btree.mTreeId != bfToFind.mPage.mBTreeId || guardedParent->mRightMostChildSwip.IsEvicted()) {
    jumpmu::Jump();
  }

  // Check whether the parent buffer frame to find is root
  auto* childSwip = &guardedParent->mRightMostChildSwip;
  if (&childSwip->AsBufferFrameMasked() == &bfToFind) {
    guardedParent.JumpIfModifiedByOthers();
    return {.mParentGuard = std::move(guardedParent.mGuard),
            .mParentBf = &btree.mMetaNodeSwip.AsBufferFrame(),
            .mChildSwip = *childSwip};
  }

  // Check whether the root node is cool, all nodes below including the parent
  // of the buffer frame to find are evicted.
  if (guardedParent->mRightMostChildSwip.IsCool()) {
    jumpmu::Jump();
  }

  auto& nodeToFind = *reinterpret_cast<BTreeNode*>(bfToFind.mPage.mPayload);
  const auto isInfinity = nodeToFind.mUpperFence.IsInfinity();
  const auto keyToFind = nodeToFind.GetUpperFence();

  auto posInParent = std::numeric_limits<uint32_t>::max();
  auto searchCondition = [&](GuardedBufferFrame<BTreeNode>& guardedNode) {
    if (isInfinity) {
      childSwip = &(guardedNode->mRightMostChildSwip);
      posInParent = guardedNode->mNumSlots;
    } else {
      posInParent = guardedNode->LowerBound<false>(keyToFind);
      if (posInParent == guardedNode->mNumSlots) {
        childSwip = &(guardedNode->mRightMostChildSwip);
      } else {
        childSwip = guardedNode->ChildSwip(posInParent);
      }
    }
    return (&childSwip->AsBufferFrameMasked() != &bfToFind);
  };

  // LatchMode latchMode = (jumpIfEvicted) ?
  // LatchMode::kOptimisticOrJump : LatchMode::kPessimisticExclusive;
  LatchMode latchMode = LatchMode::kOptimisticOrJump;
  // The parent of the bf we are looking for (bfToFind)
  GuardedBufferFrame<BTreeNode> guardedChild(btree.mStore->mBufferManager.get(), guardedParent,
                                             guardedParent->mRightMostChildSwip, latchMode);
  uint16_t level = 0;
  while (!guardedChild->mIsLeaf && searchCondition(guardedChild)) {
    guardedParent = std::move(guardedChild);
    if constexpr (jumpIfEvicted) {
      if (childSwip->IsEvicted()) {
        jumpmu::Jump();
      }
    }
    guardedChild = GuardedBufferFrame<BTreeNode>(btree.mStore->mBufferManager.get(), guardedParent,
                                                 *childSwip, latchMode);
    level = level + 1;
  }
  guardedParent.unlock();

  const bool found = &childSwip->AsBufferFrameMasked() == &bfToFind;
  guardedChild.JumpIfModifiedByOthers();
  if (!found) {
    jumpmu::Jump();
  }

  LS_DCHECK(posInParent != std::numeric_limits<uint32_t>::max(), "Invalid posInParent={}",
            posInParent);
  ParentSwipHandler parentHandler = {.mParentGuard = std::move(guardedChild.mGuard),
                                     .mParentBf = guardedChild.mBf,
                                     .mChildSwip = *childSwip,
                                     .mPosInParent = posInParent};
  return parentHandler;
}

} // namespace leanstore::storage::btree
