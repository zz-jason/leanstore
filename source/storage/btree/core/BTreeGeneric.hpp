#pragma once

#include "BTreeIteratorInterface.hpp"
#include "BTreeNode.hpp"
#include "BTreeWALPayload.hpp"
#include "Config.hpp"
#include "KVInterface.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "sync-primitives/PageGuard.hpp"
#include "utils/Defer.hpp"
#include "utils/JsonUtil.hpp"
#include "utils/RandomGenerator.hpp"

using namespace leanstore::storage;

namespace leanstore {
namespace storage {
namespace btree {

enum class BTREE_TYPE : u8 { GENERIC = 0, LL = 1, VI = 2 };

using BTreeNodeCallback = std::function<s64(BTreeNode&)>;

class BTreeGeneric : public leanstore::storage::BufferManagedTree {
public:
  friend class BTreePessimisticIterator;

  struct Config {
    bool mEnableWal = true;
    bool mUseBulkInsert = false;
  };

  enum class XMergeReturnCode : u8 { NOTHING, FULL_MERGE, PARTIAL_MERGE };

public:
  //---------------------------------------------------------------------------
  // Member fields
  //---------------------------------------------------------------------------
  TREEID mTreeId;
  BTREE_TYPE mTreeType = BTREE_TYPE::GENERIC;
  Config config;

  /// @brief mMetaNodeSwip owns the meta node of the tree. The right-most child
  /// of meta node is the root of the tree.
  Swip<BufferFrame> mMetaNodeSwip;
  atomic<u64> mHeight = 1;

public:
  //---------------------------------------------------------------------------
  // Constructors and Destructors
  //---------------------------------------------------------------------------
  BTreeGeneric() = default;

  virtual ~BTreeGeneric() override = default;

public:
  //---------------------------------------------------------------------------
  // Object Utils
  //---------------------------------------------------------------------------
  void create(TREEID treeId, Config config);

  bool tryMerge(BufferFrame& to_split, bool swizzle_sibling = true);

  void trySplit(BufferFrame& to_split, s16 pos = -1);

  s16 mergeLeftIntoRight(ExclusivePageGuard<BTreeNode>& parent, s16 left_pos,
                         ExclusivePageGuard<BTreeNode>& from_left,
                         ExclusivePageGuard<BTreeNode>& to_right,
                         bool full_merge_or_nothing);

  XMergeReturnCode XMerge(HybridPageGuard<BTreeNode>& guardedParent,
                          HybridPageGuard<BTreeNode>& guardedChild,
                          ParentSwipHandler&);

  inline bool isMetaNode(HybridPageGuard<BTreeNode>& guard) {
    return mMetaNodeSwip == guard.mBf;
  }
  inline bool isMetaNode(ExclusivePageGuard<BTreeNode>& guard) {
    return mMetaNodeSwip == guard.bf();
  }
  s64 iterateAllPages(BTreeNodeCallback inner, BTreeNodeCallback leaf);
  s64 iterateAllPagesRec(HybridPageGuard<BTreeNode>& node_guard,
                         BTreeNodeCallback inner, BTreeNodeCallback leaf);
  u64 countInner();
  u64 countPages();
  u64 countEntries();
  u64 getHeight();
  double averageSpaceUsage();
  u32 bytesFree();
  void printInfos(uint64_t totalSize);

public:
  // for buffer manager
  virtual void IterateChildSwips(
      BufferFrame& bf,
      std::function<bool(Swip<BufferFrame>&)> callback) override;

  virtual struct ParentSwipHandler findParent(BufferFrame& childBf) override {
    return BTreeGeneric::findParentJump(*this, childBf);
  }

  /// Returns true if the buffer manager has to restart and pick another buffer
  /// frame for eviction Attention: the guards here down the stack are not
  /// synchronized with the ones in the buffer frame manager stack frame
  ///
  /// Called by buffer manager before eviction
  virtual SpaceCheckResult checkSpaceUtilization(BufferFrame& bf) override;

  // pre: source buffer frame is shared latched
  virtual void checkpoint(BufferFrame& bf, u8* dest) override;

  virtual void undo(const u8*, const u64) override {
    LOG(FATAL) << "undo is unsupported";
  }

  virtual void todo(const u8*, const u64, const u64, const bool) override {
    LOG(FATAL) << "todo is unsupported";
  }

  virtual void unlock(const u8*) override {
    LOG(FATAL) << "unlock is unsupported";
  }

  virtual StringMap serialize() override;

  virtual void deserialize(StringMap map) override;

public:
  // Helpers
  template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
  inline void FindLeafCanJump(Slice key,
                              HybridPageGuard<BTreeNode>& targetGuard);

  template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
  void findLeafAndLatch(HybridPageGuard<BTreeNode>& targetGuard, Slice key);

public:
  /// @brief
  /// @note Note on Synchronization: findParent is called by the page provide
  /// thread which are not allowed to block Therefore, we jump whenever we
  /// encounter a latched node on our way Moreover, we jump if any page on the
  /// path is already evicted or of the bf could not be found Pre: bfToFind is
  /// not exclusively latched
  /// @tparam jumpIfEvicted
  /// @param btree the target tree which the parent is on
  /// @param bfToFind the target node to find parent for
  template <bool jumpIfEvicted = true>
  static struct ParentSwipHandler findParent(BTreeGeneric& btree,
                                             BufferFrame& bfToFind) {
    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().dt_find_parent[btree.mTreeId]++;
    }

    // Check whether search on the wrong tree or the root node is evicted
    HybridPageGuard<BTreeNode> parentGuard(btree.mMetaNodeSwip);
    if (btree.mTreeId != bfToFind.page.mBTreeId ||
        parentGuard->mRightMostChildSwip.isEVICTED()) {
      jumpmu::jump();
    }

    // Check whether the parent buffer frame to find is root
    Swip<BTreeNode>* childSwip = &parentGuard->mRightMostChildSwip;
    if (&childSwip->asBufferFrameMasked() == &bfToFind) {
      parentGuard.JumpIfModifiedByOthers();
      COUNTERS_BLOCK() {
        WorkerCounters::myCounters().dt_find_parent_root[btree.mTreeId]++;
      }
      return {.mParentGuard = std::move(parentGuard.guard),
              .mParentBf = &btree.mMetaNodeSwip.AsBufferFrame(),
              .mChildSwip = childSwip->CastTo<BufferFrame>()};
    }

    // Check whether the root node is cool, all nodes below including the parent
    // of the buffer frame to find are evicted.
    if (parentGuard->mRightMostChildSwip.isCOOL()) {
      jumpmu::jump();
    }

    auto& nodeToFind = *reinterpret_cast<BTreeNode*>(bfToFind.page.mPayload);
    const auto isInfinity = nodeToFind.mUpperFence.offset == 0;
    const auto keyToFind = nodeToFind.GetUpperFence();

    s16 posInParent = -1;
    auto search_condition = [&](HybridPageGuard<BTreeNode>& guardedNode) {
      if (isInfinity) {
        childSwip = &(guardedNode->mRightMostChildSwip);
        posInParent = guardedNode->mNumSeps;
      } else {
        posInParent = guardedNode->lowerBound<false>(keyToFind);
        if (posInParent == guardedNode->mNumSeps) {
          childSwip = &(guardedNode->mRightMostChildSwip);
        } else {
          childSwip = &(guardedNode->getChild(posInParent));
        }
      }
      return (&childSwip->asBufferFrameMasked() != &bfToFind);
    };

    // LATCH_FALLBACK_MODE latch_mode = (jumpIfEvicted) ?
    // LATCH_FALLBACK_MODE::JUMP : LATCH_FALLBACK_MODE::EXCLUSIVE;
    LATCH_FALLBACK_MODE latch_mode = LATCH_FALLBACK_MODE::JUMP;
    // The parent of the bf we are looking for (bfToFind)
    HybridPageGuard childGuard(parentGuard, parentGuard->mRightMostChildSwip,
                               latch_mode);
    u16 level = 0;
    while (!childGuard->mIsLeaf && search_condition(childGuard)) {
      parentGuard = std::move(childGuard);
      if constexpr (jumpIfEvicted) {
        if (childSwip->isEVICTED()) {
          jumpmu::jump();
        }
      }
      childGuard = HybridPageGuard(parentGuard, childSwip->CastTo<BTreeNode>(),
                                   latch_mode);
      level = level + 1;
    }
    parentGuard.unlock();

    const bool found = &childSwip->asBufferFrameMasked() == &bfToFind;
    childGuard.JumpIfModifiedByOthers();
    if (!found) {
      jumpmu::jump();
    }

    ParentSwipHandler parentHandler = {
        .mParentGuard = std::move(childGuard.guard),
        .mParentBf = childGuard.mBf,
        .mChildSwip = childSwip->CastTo<BufferFrame>(),
        .mPosInParent = posInParent};
    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().dt_find_parent_slow[btree.mTreeId]++;
    }
    return parentHandler;
  }

  static struct ParentSwipHandler findParentJump(BTreeGeneric& btree,
                                                 BufferFrame& bfToFind) {
    return findParent<true>(btree, bfToFind);
  }

  static struct ParentSwipHandler findParentEager(BTreeGeneric& btree,
                                                  BufferFrame& bfToFind) {
    return findParent<false>(btree, bfToFind);
  }

  /// Removes a btree from disk, reclaim all the buffer frames in memory and
  /// pages in disk used by it.
  ///
  /// @param btree The tree to free.
  static void FreeAndReclaim(BTreeGeneric& btree) {
    HybridPageGuard<BTreeNode> guardedMetaNode(btree.mMetaNodeSwip);
    HybridPageGuard<BTreeNode> guardedRootNode(
        guardedMetaNode, guardedMetaNode->mRightMostChildSwip);
    BTreeGeneric::freeBTreeNodesRecursive(guardedRootNode);

    auto exclusiveGuardedMetaNode =
        ExclusivePageGuard(std::move(guardedMetaNode));
    exclusiveGuardedMetaNode.reclaim();
  }

  static void ToJSON(BTreeGeneric& btree, rapidjson::Document* resultDoc) {
    DCHECK(resultDoc->IsObject());
    auto& allocator = resultDoc->GetAllocator();

    // meta node
    HybridPageGuard<BTreeNode> guardedMetaNode(btree.mMetaNodeSwip);
    rapidjson::Value metaJson(rapidjson::kObjectType);
    guardedMetaNode.mBf->ToJSON(&metaJson, allocator);
    resultDoc->AddMember("metaNode", metaJson, allocator);

    // root node
    HybridPageGuard<BTreeNode> guardedRootNode(
        guardedMetaNode, guardedMetaNode->mRightMostChildSwip);
    rapidjson::Value rootJson(rapidjson::kObjectType);
    ToJSONRecursive(guardedRootNode, &rootJson, allocator);
    resultDoc->AddMember("rootNode", rootJson, allocator);
  }

private:
  static void freeBTreeNodesRecursive(HybridPageGuard<BTreeNode>& guardedNode);

  static void ToJSONRecursive(HybridPageGuard<BTreeNode>& guardedNode,
                              rapidjson::Value* resultObj,
                              rapidjson::Value::AllocatorType& allocator);

public:
  static constexpr std::string TREE_ID = "treeId";
  static constexpr std::string HEIGHT = "height";
  static constexpr std::string META_PAGE_ID = "metaPageId";
};

inline void BTreeGeneric::freeBTreeNodesRecursive(
    HybridPageGuard<BTreeNode>& guardedNode) {
  if (!guardedNode->mIsLeaf) {
    for (auto i = 0u; i <= guardedNode->mNumSeps; ++i) {
      auto childSwip = guardedNode->GetChildIncludingRightMost(i);
      HybridPageGuard<BTreeNode> guardedChild(guardedNode, childSwip);
      freeBTreeNodesRecursive(guardedChild);
    }
  }

  auto exclusiveGuardedNode = ExclusivePageGuard(std::move(guardedNode));
  exclusiveGuardedNode.reclaim();
}

inline void BTreeGeneric::ToJSONRecursive(
    HybridPageGuard<BTreeNode>& guardedNode, rapidjson::Value* resultObj,
    rapidjson::Value::AllocatorType& allocator) {

  DCHECK(resultObj->IsObject());
  guardedNode.mBf->ToJSON(resultObj, allocator);

  // btree node
  {
    rapidjson::Value nodeObj(rapidjson::kObjectType);
    guardedNode->ToJSON(&nodeObj, allocator);
    resultObj->AddMember("btreeNode", nodeObj, allocator);
  }

  if (guardedNode->mIsLeaf) {
    return;
  }

  rapidjson::Value childrenJson(rapidjson::kArrayType);
  for (auto i = 0u; i < guardedNode->mNumSeps; ++i) {
    auto childSwip = guardedNode->getChild(i);
    HybridPageGuard<BTreeNode> guardedChild(guardedNode, childSwip);

    rapidjson::Value childObj(rapidjson::kObjectType);
    ToJSONRecursive(guardedChild, &childObj, allocator);
    guardedChild.unlock();

    childrenJson.PushBack(childObj, allocator);
  }

  if (guardedNode->mRightMostChildSwip != nullptr) {
    HybridPageGuard<BTreeNode> guardedChild(guardedNode,
                                            guardedNode->mRightMostChildSwip);
    rapidjson::Value childObj(rapidjson::kObjectType);
    ToJSONRecursive(guardedChild, &childObj, allocator);
    guardedChild.unlock();

    childrenJson.PushBack(childObj, allocator);
  }

  // children
  resultObj->AddMember("mChildren", childrenJson, allocator);
}

inline void BTreeGeneric::IterateChildSwips(
    BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback) {
  // Pre: bf is read locked
  auto& childNode = *reinterpret_cast<BTreeNode*>(bf.page.mPayload);
  if (childNode.mIsLeaf) {
    return;
  }
  for (u16 i = 0; i < childNode.mNumSeps; i++) {
    if (!callback(childNode.getChild(i).CastTo<BufferFrame>())) {
      return;
    }
  }
  callback(childNode.mRightMostChildSwip.CastTo<BufferFrame>());
}

inline SpaceCheckResult BTreeGeneric::checkSpaceUtilization(BufferFrame& bf) {
  if (!FLAGS_xmerge) {
    return SpaceCheckResult::NOTHING;
  }

  ParentSwipHandler parentHandler = BTreeGeneric::findParentJump(*this, bf);
  HybridPageGuard<BTreeNode> guardedParent =
      parentHandler.getParentReadPageGuard<BTreeNode>();
  HybridPageGuard<BTreeNode> guardedChild(
      guardedParent, parentHandler.mChildSwip.CastTo<BTreeNode>(),
      LATCH_FALLBACK_MODE::JUMP);
  auto mergeResult = XMerge(guardedParent, guardedChild, parentHandler);
  guardedParent.unlock();
  guardedChild.unlock();

  if (mergeResult == XMergeReturnCode::NOTHING) {
    return SpaceCheckResult::NOTHING;
  } else {
    return SpaceCheckResult::PICK_ANOTHER_BF;
  }
}

inline void BTreeGeneric::checkpoint(BufferFrame& bf, u8* dest) {
  std::memcpy(dest, bf.page.mPayload, EFFECTIVE_PAGE_SIZE);
  auto& destNode = *reinterpret_cast<BTreeNode*>(dest);

  if (!destNode.mIsLeaf) {
    // Replace all child swip to their page ID
    for (u64 i = 0; i < destNode.mNumSeps; i++) {
      if (!destNode.getChild(i).isEVICTED()) {
        auto& childBf = destNode.getChild(i).asBufferFrameMasked();
        destNode.getChild(i).evict(childBf.header.mPageId);
      }
    }
    // Replace right most child swip to page id
    if (!destNode.mRightMostChildSwip.isEVICTED()) {
      auto& childBf = destNode.mRightMostChildSwip.asBufferFrameMasked();
      destNode.mRightMostChildSwip.evict(childBf.header.mPageId);
    }
  }
}

inline StringMap BTreeGeneric::serialize() {
  DCHECK(mMetaNodeSwip.AsBufferFrame().page.mBTreeId == mTreeId);
  BufferManager::sInstance->WriteBufferFrame(mMetaNodeSwip.AsBufferFrame());
  return {{TREE_ID, std::to_string(mTreeId)},
          {HEIGHT, std::to_string(mHeight.load())},
          {META_PAGE_ID,
           std::to_string(mMetaNodeSwip.AsBufferFrame().header.mPageId)}};
}

inline void BTreeGeneric::deserialize(StringMap map) {
  mTreeId = std::stoull(map[TREE_ID]);
  mHeight = std::stoull(map[HEIGHT]);
  mMetaNodeSwip.evict(std::stoull(map[META_PAGE_ID]));

  // load meta node to memory
  HybridLatch dummyLatch;
  Guard dummyGuard(&dummyLatch);
  dummyGuard.toOptimisticSpin();

  u16 failcounter = 0;
  while (true) {
    JUMPMU_TRY() {
      mMetaNodeSwip = BufferManager::sInstance->ResolveSwipMayJump(
          dummyGuard, mMetaNodeSwip);
      JUMPMU_BREAK;
    }
    JUMPMU_CATCH() {
      failcounter++;
      LOG_IF(FATAL, failcounter >= 100) << "Failed to load MetaNode";
    }
  }
  mMetaNodeSwip.AsBufferFrame().header.mKeepInMemory = true;
  DCHECK(mMetaNodeSwip.AsBufferFrame().page.mBTreeId == mTreeId);
}

template <LATCH_FALLBACK_MODE mode>
inline void BTreeGeneric::FindLeafCanJump(
    Slice key, HybridPageGuard<BTreeNode>& targetGuard) {
  targetGuard.unlock();
  HybridPageGuard<BTreeNode> guardedParent(mMetaNodeSwip);
  targetGuard = HybridPageGuard<BTreeNode>(guardedParent,
                                           guardedParent->mRightMostChildSwip);

  u16 volatile level = 0;

  while (!targetGuard->mIsLeaf) {
    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().dt_inner_page[mTreeId]++;
    }

    Swip<BTreeNode>& c_swip = targetGuard->lookupInner(key);
    guardedParent = std::move(targetGuard);
    if (level == mHeight - 1) {
      targetGuard = HybridPageGuard(guardedParent, c_swip, mode);
    } else {
      targetGuard = HybridPageGuard(guardedParent, c_swip);
    }
    level = level + 1;
  }

  guardedParent.unlock();
}

template <LATCH_FALLBACK_MODE mode>
inline void BTreeGeneric::findLeafAndLatch(
    HybridPageGuard<BTreeNode>& targetGuard, Slice key) {
  while (true) {
    JUMPMU_TRY() {
      FindLeafCanJump<mode>(key, targetGuard);
      if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
        targetGuard.toExclusive();
      } else {
        targetGuard.toShared();
      }
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
    }
  }
}

} // namespace btree
} // namespace storage
} // namespace leanstore
