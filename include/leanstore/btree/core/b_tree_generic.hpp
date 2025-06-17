#pragma once

#include "leanstore/btree/core/b_tree_node.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/buffer-manager/guarded_buffer_frame.hpp"
#include "leanstore/buffer-manager/tree_registry.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/sync/hybrid_latch.hpp"
#include "leanstore/units.hpp"
#include "leanstore/utils/log.hpp"
#include "utils/json.hpp"

#include <atomic>
#include <cassert>
#include <limits>

namespace leanstore::storage::btree {

enum class BTreeType : uint8_t { kGeneric = 0, kBasicKV = 1, kTransactionKV = 2 };

class BTreeIter;
class BTreeIterMut;
using BTreeNodeCallback = std::function<int64_t(BTreeNode&)>;

class BTreeGeneric : public leanstore::storage::BufferManagedTree {
public:
  friend class BTreeIterPessistic;

  enum class XMergeReturnCode : uint8_t { kNothing, kFullMerge, kPartialMerge };

  leanstore::LeanStore* store_;

  TREEID tree_id_;

  BTreeType tree_type_ = BTreeType::kGeneric;

  BTreeConfig config_;

  /// Owns the meta node of the tree. The right-most child of meta node is the root of the tree.
  Swip meta_node_swip_;

  std::atomic<uint64_t> height_ = 1;

  BTreeGeneric() = default;

  virtual ~BTreeGeneric() override = default;

  void Init(leanstore::LeanStore* store, TREEID tree_id, BTreeConfig config);

  /// Create an immutable iterator for the BTree.
  std::unique_ptr<BTreeIter> NewBTreeIter();

  /// Create an mutable iterator for the BTree.
  std::unique_ptr<BTreeIterMut> NewBTreeIterMut();

  /// Try to merge the current node with its left or right sibling, reclaim the merged left or right
  /// sibling if successful.
  bool TryMergeMayJump(TXID sys_tx_id, BufferFrame& to_merge, bool swizzle_sibling = true);

  void TrySplitMayJump(TXID sys_tx_id, BufferFrame& to_split, int16_t pos = -1);

  XMergeReturnCode XMerge(GuardedBufferFrame<BTreeNode>& guarded_parent,
                          GuardedBufferFrame<BTreeNode>& guarded_child,
                          ParentSwipHandler& parent_swip_handler);

  uint64_t CountInnerPages() {
    return iterate_all_pages([](BTreeNode&) { return 1; }, [](BTreeNode&) { return 0; });
  }

  uint64_t CountAllPages() {
    return iterate_all_pages([](BTreeNode&) { return 1; }, [](BTreeNode&) { return 1; });
  }

  uint64_t CountEntries() {
    return iterate_all_pages([](BTreeNode&) { return 0; },
                             [](BTreeNode& node) { return node.num_slots_; });
  }

  uint64_t GetHeight() {
    return height_.load();
  }

  uint32_t FreeSpaceAfterCompaction() {
    return iterate_all_pages([](BTreeNode& inner) { return inner.FreeSpaceAfterCompaction(); },
                             [](BTreeNode& leaf) { return leaf.FreeSpaceAfterCompaction(); });
  }

  /// Get a summary of the BTree
  std::string Summary();

  // for buffer manager
  virtual void IterateChildSwips(BufferFrame& bf, std::function<bool(Swip&)> callback) override;

  virtual ParentSwipHandler FindParent(BufferFrame& child_bf) override {
    return BTreeGeneric::find_parent_may_jump(*this, child_bf);
  }

  /// Returns true if the buffer manager has to restart and pick another buffer
  /// frame for eviction Attention: the guards here down the stack are not
  /// synchronized with the ones in the buffer frame manager stack frame
  ///
  /// Called by buffer manager before eviction
  virtual SpaceCheckResult CheckSpaceUtilization(BufferFrame& bf) override;

  /// Flush the page content in the buffer frame to disk
  ///
  /// NOTE: The source buffer frame should be shared latched
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
  inline bool is_meta_node(GuardedBufferFrame<BTreeNode>& guarded_node) {
    return meta_node_swip_ == guarded_node.bf_;
  }

  inline bool is_meta_node(ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_node) {
    return meta_node_swip_ == x_guarded_node.bf();
  }

  /// Split the root node, 4 nodes are involved in the split:
  /// meta(oldRoot) -> meta(newRoot(newLeft, oldRoot))
  ///
  /// meta         meta
  ///   |            |
  /// toSplit      newRoot
  ///              |     |
  ///           newLeft toSplit
  ///
  void split_root_may_jump(TXID sys_tx_id, GuardedBufferFrame<BTreeNode>& guarded_parent,
                           GuardedBufferFrame<BTreeNode>& guarded_child,
                           const BTreeNode::SeparatorInfo& sep_info);

  /// Split a non-root node, 3 nodes are involved in the split:
  /// parent(toSplit) -> parent(newLeft, toSplit)
  ///
  /// parent         parent
  ///   |            |   |
  /// toSplit   newLeft toSplit
  ///
  void split_non_root_may_jump(TXID sys_tx_id, GuardedBufferFrame<BTreeNode>& guarded_parent,
                               GuardedBufferFrame<BTreeNode>& guarded_child,
                               const BTreeNode::SeparatorInfo& sep_info,
                               uint16_t space_needed_for_separator);

  int64_t iterate_all_pages(BTreeNodeCallback inner, BTreeNodeCallback leaf);

  int64_t iterate_all_pages_recursive(GuardedBufferFrame<BTreeNode>& guarded_node,
                                      BTreeNodeCallback inner, BTreeNodeCallback leaf);

  int16_t merge_left_into_right(ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_parent,
                                int16_t left_pos,
                                ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_left,
                                ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_right,
                                bool full_merge_or_nothing);

public:
  // Helpers
  void FindLeafCanJump(Slice key, GuardedBufferFrame<BTreeNode>& guarded_target,
                       LatchMode mode = LatchMode::kSharedPessimistic);

  // void CoroFindLeaf(Slice key, CoroLockedBufferFrame& guarded_target, LockMode mode);

  /// Note on Synchronization: it is called by the page provide thread which are not allowed to
  /// block. Therefore, we jump whenever we encounter a latched node on our way Moreover, we jump if
  /// any page on the path is already evicted or of the bf could not be found Pre: bfToFind is not
  /// exclusively latched
  /// @param jumpIfEvicted
  /// @param btree the target tree which the parent is on
  /// @param bfToFind the target node to find parent for
  template <bool jump_if_evicted = true>
  static ParentSwipHandler FindParent(BTreeGeneric& btree, BufferFrame& bf_to_find);

  /// Removes a btree from disk, reclaim all the buffer frames in memory and
  /// pages in disk used by it.
  ///
  /// @param btree The tree to free.
  static void FreeAndReclaim(BTreeGeneric& btree) {
    GuardedBufferFrame<BTreeNode> guarded_meta_node(btree.store_->buffer_manager_.get(),
                                                    btree.meta_node_swip_);
    GuardedBufferFrame<BTreeNode> guarded_root_node(btree.store_->buffer_manager_.get(),
                                                    guarded_meta_node,
                                                    guarded_meta_node->right_most_child_swip_);
    BTreeGeneric::free_b_tree_nodes_recursive(btree, guarded_root_node);

    auto x_guarded_meta = ExclusiveGuardedBufferFrame(std::move(guarded_meta_node));
    x_guarded_meta.Reclaim();
  }

  static void ToJson(BTreeGeneric& btree, utils::JsonObj* btree_json_obj);

private:
  static void free_b_tree_nodes_recursive(BTreeGeneric& btree,
                                          GuardedBufferFrame<BTreeNode>& guarded_node);

  static void to_json_recursive(BTreeGeneric& btree, GuardedBufferFrame<BTreeNode>& guarded_node,
                                utils::JsonObj* node_json_obj);

  static ParentSwipHandler find_parent_may_jump(BTreeGeneric& btree, BufferFrame& bf_to_find) {
    return FindParent<true>(btree, bf_to_find);
  }

  static ParentSwipHandler find_parent_eager(BTreeGeneric& btree, BufferFrame& bf_to_find) {
    return FindParent<false>(btree, bf_to_find);
  }

public:
  static constexpr std::string kTreeId = "treeId";
  static constexpr std::string kHeight = "height";
  static constexpr std::string kMetaPageId = "metaPageId";
};

inline void BTreeGeneric::free_b_tree_nodes_recursive(BTreeGeneric& btree,
                                                      GuardedBufferFrame<BTreeNode>& guarded_node) {
  if (!guarded_node->is_leaf_) {
    for (auto i = 0u; i <= guarded_node->num_slots_; ++i) {
      auto* child_swip = guarded_node->ChildSwipIncludingRightMost(i);
      GuardedBufferFrame<BTreeNode> guarded_child(btree.store_->buffer_manager_.get(), guarded_node,
                                                  *child_swip);
      free_b_tree_nodes_recursive(btree, guarded_child);
    }
  }

  auto x_guarded_node = ExclusiveGuardedBufferFrame(std::move(guarded_node));
  x_guarded_node.Reclaim();
}

inline void BTreeGeneric::IterateChildSwips(BufferFrame& bf, std::function<bool(Swip&)> callback) {
  // Pre: bf is read locked
  auto& btree_node = *reinterpret_cast<BTreeNode*>(bf.page_.payload_);
  if (btree_node.is_leaf_) {
    return;
  }
  for (uint16_t i = 0; i < btree_node.num_slots_; i++) {
    if (!callback(*btree_node.ChildSwip(i))) {
      return;
    }
  }
  if (btree_node.right_most_child_swip_ != nullptr) {
    callback(btree_node.right_most_child_swip_);
  }
}

inline SpaceCheckResult BTreeGeneric::CheckSpaceUtilization(BufferFrame& bf) {
  if (!store_->store_option_->enable_xmerge_) {
    return SpaceCheckResult::kNothing;
  }

  ParentSwipHandler parent_handler = BTreeGeneric::find_parent_may_jump(*this, bf);
  GuardedBufferFrame<BTreeNode> guarded_parent(store_->buffer_manager_.get(),
                                               std::move(parent_handler.parent_guard_),
                                               parent_handler.parent_bf_);
  GuardedBufferFrame<BTreeNode> guarded_child(store_->buffer_manager_.get(), guarded_parent,
                                              parent_handler.child_swip_,
                                              LatchMode::kOptimisticOrJump);
  auto merge_result = XMerge(guarded_parent, guarded_child, parent_handler);
  guarded_parent.unlock();
  guarded_child.unlock();

  if (merge_result == XMergeReturnCode::kNothing) {
    return SpaceCheckResult::kNothing;
  }
  return SpaceCheckResult::kPickAnotherBf;
}

inline void BTreeGeneric::Checkpoint(BufferFrame& bf, void* dest) {
  std::memcpy(dest, &bf.page_, store_->store_option_->page_size_);
  auto* dest_page = reinterpret_cast<Page*>(dest);
  auto* dest_node = reinterpret_cast<BTreeNode*>(dest_page->payload_);

  if (!dest_node->is_leaf_) {
    // Replace all child swip to their page ID
    for (uint64_t i = 0; i < dest_node->num_slots_; i++) {
      if (!dest_node->ChildSwip(i)->IsEvicted()) {
        auto& child_bf = dest_node->ChildSwip(i)->AsBufferFrameMasked();
        dest_node->ChildSwip(i)->Evict(child_bf.header_.page_id_);
      }
    }
    // Replace right most child swip to page id
    if (dest_node->right_most_child_swip_ != nullptr &&
        !dest_node->right_most_child_swip_.IsEvicted()) {
      auto& child_bf = dest_node->right_most_child_swip_.AsBufferFrameMasked();
      dest_node->right_most_child_swip_.Evict(child_bf.header_.page_id_);
    }
  }
}

inline void BTreeGeneric::FindLeafCanJump(Slice key, GuardedBufferFrame<BTreeNode>& guarded_target,
                                          LatchMode mode) {
  guarded_target.unlock();
  auto* buffer_manager = store_->buffer_manager_.get();

  // meta node
  GuardedBufferFrame<BTreeNode> guarded_parent(buffer_manager, meta_node_swip_,
                                               LatchMode::kOptimisticSpin);

  // root node
  guarded_target = GuardedBufferFrame<BTreeNode>(buffer_manager, guarded_parent,
                                                 guarded_parent->right_most_child_swip_,
                                                 LatchMode::kOptimisticSpin);

  volatile uint16_t level = 0;
  while (!guarded_target->is_leaf_) {
    auto& child_swip = guarded_target->LookupInner(key);
    LS_DCHECK(!child_swip.IsEmpty());
    guarded_parent = std::move(guarded_target);
    if (level == height_ - 1) {
      guarded_target =
          GuardedBufferFrame<BTreeNode>(buffer_manager, guarded_parent, child_swip, mode);
    } else {
      // middle node
      guarded_target = GuardedBufferFrame<BTreeNode>(buffer_manager, guarded_parent, child_swip,
                                                     LatchMode::kOptimisticSpin);
    }
    level = level + 1;
  }

  guarded_parent.unlock();
}

// inline void BTreeGeneric::CoroFindLeaf(Slice key, CoroLockedBufferFrame& guarded_target,
//                                        LockMode mode) {
//
//   auto non_leaf_lock_mode = LockMode::kSharedOptimistic;
//   auto* buffer_manager = store_->buffer_manager_.get();
//
//   // lock meta buffer frame
//   auto* parent_bf = &meta_node_swip_.AsBufferFrame();
//   auto* parent_node = BTreeNode::From(parent_bf);
//   CoroLockedBufferFrame locked_parent(parent_bf, non_leaf_lock_mode);
//
//   // lock root buffer frame
//   auto* child_bf = &parent_node->right_most_child_swip_.AsBufferFrame();
//   auto* child_node = BTreeNode::From(&parent_node->right_most_child_swip_.AsBufferFrame());
//   CoroLockedBufferFrame locked_child(locked_parent.LockChild(child_bf, non_leaf_lock_mode));
//
//   // search for the leaf node
//   auto level = 0u;
//   while (!child_node->is_leaf_) {
//     auto& child_swip = child_node->LookupInner(key);
//     LS_DCHECK(!child_swip.IsEmpty());
//
//     // TODO: yield and retry from the begining
//     if (locked_parent.IsConflicted()) {
//     }
//
//     locked_parent = std::move(locked_child);
//     if (level == height_ - 1) {
//       locked_child = locked_parent.LockChild(&child_swip.AsBufferFrame(), mode);
//     } else {
//       locked_child = locked_parent.LockChild(&child_swip.AsBufferFrame(), non_leaf_lock_mode);
//     }
//     child_bf = &locked_child.BufferFrame();
//     child_node = BTreeNode::From(child_bf);
//     level++;
//   }
// }

template <bool jump_if_evicted>
inline ParentSwipHandler BTreeGeneric::FindParent(BTreeGeneric& btree, BufferFrame& bf_to_find) {
  // Check whether search on the wrong tree or the root node is evicted
  GuardedBufferFrame<BTreeNode> guarded_parent(btree.store_->buffer_manager_.get(),
                                               btree.meta_node_swip_);
  if (btree.tree_id_ != bf_to_find.page_.btree_id_ ||
      guarded_parent->right_most_child_swip_.IsEvicted()) {
    leanstore::JumpContext::Jump();
  }

  // Check whether the parent buffer frame to find is root
  auto* child_swip = &guarded_parent->right_most_child_swip_;
  if (&child_swip->AsBufferFrameMasked() == &bf_to_find) {
    guarded_parent.JumpIfModifiedByOthers();
    return {.parent_guard_ = std::move(guarded_parent.guard_),
            .parent_bf_ = &btree.meta_node_swip_.AsBufferFrame(),
            .child_swip_ = *child_swip};
  }

  // Check whether the root node is cool, all nodes below including the parent
  // of the buffer frame to find are evicted.
  if (guarded_parent->right_most_child_swip_.IsCool()) {
    leanstore::JumpContext::Jump();
  }

  auto& node_to_find = *reinterpret_cast<BTreeNode*>(bf_to_find.page_.payload_);
  const auto is_infinity = node_to_find.upper_fence_.IsInfinity();
  const auto key_to_find = node_to_find.GetUpperFence();

  auto pos_in_parent = std::numeric_limits<uint32_t>::max();
  auto search_condition = [&](GuardedBufferFrame<BTreeNode>& guarded_node) {
    if (is_infinity) {
      child_swip = &(guarded_node->right_most_child_swip_);
      pos_in_parent = guarded_node->num_slots_;
    } else {
      pos_in_parent = guarded_node->LowerBound<false>(key_to_find);
      if (pos_in_parent == guarded_node->num_slots_) {
        child_swip = &(guarded_node->right_most_child_swip_);
      } else {
        child_swip = guarded_node->ChildSwip(pos_in_parent);
      }
    }
    return (&child_swip->AsBufferFrameMasked() != &bf_to_find);
  };

  // LatchMode latchMode = (jumpIfEvicted) ?
  // LatchMode::kOptimisticOrJump : LatchMode::kExclusivePessimistic;
  LatchMode latch_mode = LatchMode::kOptimisticOrJump;
  // The parent of the bf we are looking for (bfToFind)
  GuardedBufferFrame<BTreeNode> guarded_child(btree.store_->buffer_manager_.get(), guarded_parent,
                                              guarded_parent->right_most_child_swip_, latch_mode);
  uint16_t level = 0;
  while (!guarded_child->is_leaf_ && search_condition(guarded_child)) {
    guarded_parent = std::move(guarded_child);
    if constexpr (jump_if_evicted) {
      if (child_swip->IsEvicted()) {
        leanstore::JumpContext::Jump();
      }
    }
    guarded_child = GuardedBufferFrame<BTreeNode>(btree.store_->buffer_manager_.get(),
                                                  guarded_parent, *child_swip, latch_mode);
    level = level + 1;
  }
  guarded_parent.unlock();

  const bool found = &child_swip->AsBufferFrameMasked() == &bf_to_find;
  guarded_child.JumpIfModifiedByOthers();
  if (!found) {
    leanstore::JumpContext::Jump();
  }

  LS_DCHECK(pos_in_parent != std::numeric_limits<uint32_t>::max(), "Invalid posInParent={}",
            pos_in_parent);
  ParentSwipHandler parent_handler = {.parent_guard_ = std::move(guarded_child.guard_),
                                      .parent_bf_ = guarded_child.bf_,
                                      .child_swip_ = *child_swip,
                                      .pos_in_parent_ = pos_in_parent};
  return parent_handler;
}

} // namespace leanstore::storage::btree
