#include "leanstore/btree/core/b_tree_generic.hpp"

#include "btree/core/b_tree_wal_payload.hpp"
#include "leanstore/btree/core/b_tree_node.hpp"
#include "leanstore/btree/core/btree_iter.hpp"
#include "leanstore/btree/core/btree_iter_mut.hpp"
#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/buffer-manager/guarded_buffer_frame.hpp"
#include "leanstore/concurrency/wal_builder.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/units.hpp"
#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/managed_thread.hpp"
#include "leanstore/utils/misc.hpp"
#include "utils/coroutine/coro_env.hpp"
#include "utils/coroutine/mvcc_manager.hpp"
#include "utils/to_json.hpp"

#include <cstdint>
#include <format>
#include <memory>

using namespace leanstore::storage;

namespace leanstore::storage::btree {

void BTreeGeneric::Init(leanstore::LeanStore* store, TREEID btree_id, lean_btree_config config) {
  this->store_ = store;
  this->tree_id_ = btree_id;
  this->config_ = std::move(config);

  meta_node_swip_ = &store_->buffer_manager_->AllocNewPage(btree_id);
  meta_node_swip_.AsBufferFrame().header_.keep_in_memory_ = true;
  LEAN_DCHECK(meta_node_swip_.AsBufferFrame().header_.latch_.GetVersion() == 0);

  auto guarded_root = GuardedBufferFrame<BTreeNode>(
      store_->buffer_manager_.get(), &store_->buffer_manager_->AllocNewPage(btree_id));
  auto x_guarded_root = ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guarded_root));
  x_guarded_root.InitPayload(true);

  auto guarded_meta = GuardedBufferFrame<BTreeNode>(store_->buffer_manager_.get(), meta_node_swip_);
  auto x_guarded_meta = ExclusiveGuardedBufferFrame(std::move(guarded_meta));
  x_guarded_meta->is_leaf_ = false;
  x_guarded_meta->right_most_child_swip_ = x_guarded_root.bf();

  // Record WAL
  if (config_.enable_wal_) {
    TXID sys_tx_id = store_->MvccManager()->AllocSysTxTs();

    // wal for meta node
    auto* meta_bf = x_guarded_meta.bf();
    WalBuilder<WalInitPage>{}
        .InitHeader(0, 0, 0, meta_bf->page_.psn_, meta_bf->header_.page_id_, tree_id_)
        .InitData(sys_tx_id, tree_id_, x_guarded_meta->is_leaf_)
        .Submit();

    // wal for root node
    auto* root_bf = x_guarded_root.bf();
    WalBuilder<WalInitPage>{}
        .InitHeader(0, 0, 0, root_bf->page_.psn_, root_bf->header_.page_id_, tree_id_)
        .InitData(sys_tx_id, tree_id_, x_guarded_root->is_leaf_)
        .Submit();

    x_guarded_meta.SyncSystemTxId(sys_tx_id);
    x_guarded_root.SyncSystemTxId(sys_tx_id);
  }
}

std::unique_ptr<BTreeIter> BTreeGeneric::NewBTreeIter() {
  return std::make_unique<BTreeIter>(*this);
}

std::unique_ptr<BTreeIterMut> BTreeGeneric::NewBTreeIterMut() {
  return std::make_unique<BTreeIterMut>(*this);
}

void BTreeGeneric::TrySplitMayJump(TXID sys_tx_id, BufferFrame& to_split,
                                   int16_t favored_split_pos) {
  auto parent_handler = find_parent_eager(*this, to_split);
  GuardedBufferFrame<BTreeNode> guarded_parent(store_->buffer_manager_.get(),
                                               std::move(parent_handler.parent_guard_),
                                               parent_handler.parent_bf_);
  auto guarded_child = GuardedBufferFrame<BTreeNode>(store_->buffer_manager_.get(), guarded_parent,
                                                     parent_handler.child_swip_);
  if (guarded_child->num_slots_ <= 1) {
    Log::Warn(
        "Split failed, slots too less: sysTxId={}, pageId={}, favoredSplitPos={}, numSlots={}",
        sys_tx_id, to_split.header_.page_id_, favored_split_pos, guarded_child->num_slots_);
    return;
  }

  // init the separator info
  BTreeNode::SeparatorInfo sep_info;
  if (favored_split_pos < 0 || favored_split_pos >= guarded_child->num_slots_ - 1) {
    if (config_.use_bulk_insert_) {
      favored_split_pos = guarded_child->num_slots_ - 2;
      sep_info = BTreeNode::SeparatorInfo{guarded_child->GetFullKeyLen(favored_split_pos),
                                          static_cast<uint16_t>(favored_split_pos), false};
    } else {
      sep_info = guarded_child->FindSep();
    }
  } else {
    // Split on a specified position, used by contention management
    sep_info = BTreeNode::SeparatorInfo{guarded_child->GetFullKeyLen(favored_split_pos),
                                        static_cast<uint16_t>(favored_split_pos), false};
  }

  // split the root node
  if (is_meta_node(guarded_parent)) {
    split_root_may_jump(sys_tx_id, guarded_parent, guarded_child, sep_info);
    return;
  }

  // calculate space needed for separator in parent node
  const uint16_t space_needed_for_separator =
      guarded_parent->SpaceNeeded(sep_info.size_, sizeof(Swip));

  // split the parent node to make zoom for separator
  if (!guarded_parent->HasEnoughSpaceFor(space_needed_for_separator)) {
    guarded_parent.unlock();
    guarded_child.unlock();
    TrySplitMayJump(sys_tx_id, *guarded_parent.bf_);
    return;
  }

  // split the non-root node
  split_non_root_may_jump(sys_tx_id, guarded_parent, guarded_child, sep_info,
                          space_needed_for_separator);
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
void BTreeGeneric::split_root_may_jump(TXID sys_tx_id, GuardedBufferFrame<BTreeNode>& guarded_meta,
                                       GuardedBufferFrame<BTreeNode>& guarded_old_root,
                                       const BTreeNode::SeparatorInfo& sep_info) {
  auto x_guarded_meta = ExclusiveGuardedBufferFrame(std::move(guarded_meta));
  auto x_guarded_old_root = ExclusiveGuardedBufferFrame(std::move(guarded_old_root));
  auto* bm = store_->buffer_manager_.get();

  LEAN_DCHECK(is_meta_node(guarded_meta), "Parent should be meta node");
  LEAN_DCHECK(height_ == 1 || !x_guarded_old_root->is_leaf_);

  // 1. create new left, lock it exclusively, write wal on demand
  auto* new_left_bf = &bm->AllocNewPageMayJump(tree_id_);
  auto guarded_new_left = GuardedBufferFrame<BTreeNode>(bm, new_left_bf);
  auto x_guarded_new_left = ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guarded_new_left));
  if (config_.enable_wal_) {
    auto* new_left_bf = x_guarded_new_left.bf();
    WalBuilder<WalInitPage>{}
        .InitHeader(0, 0, 0, new_left_bf->page_.psn_, new_left_bf->header_.page_id_, tree_id_)
        .InitData(sys_tx_id, tree_id_, x_guarded_old_root->is_leaf_)
        .Submit();
    x_guarded_new_left.SyncSystemTxId(sys_tx_id);
  }
  x_guarded_new_left.InitPayload(x_guarded_old_root->is_leaf_);

  // 2. create new root, lock it exclusively, write wal on demand
  auto* new_root_bf = &bm->AllocNewPageMayJump(tree_id_);
  auto guarded_new_root = GuardedBufferFrame<BTreeNode>(bm, new_root_bf);
  auto x_guarded_new_root = ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guarded_new_root));
  if (config_.enable_wal_) {
    auto* new_root_bf = x_guarded_new_root.bf();
    WalBuilder<WalInitPage>{}
        .InitHeader(0, 0, 0, new_root_bf->page_.psn_, new_root_bf->header_.page_id_, tree_id_)
        .InitData(sys_tx_id, tree_id_, false)
        .Submit();
    x_guarded_new_root.SyncSystemTxId(sys_tx_id);
  }
  x_guarded_new_root.InitPayload(false);

  // 3.1. write wal on demand
  if (config_.enable_wal_) {
    auto* old_root_bf = x_guarded_old_root.bf();
    WalBuilder<WalSplitRoot>{}
        .InitHeader(0, 0, 0, old_root_bf->page_.psn_, old_root_bf->header_.page_id_, tree_id_)
        .InitData(sys_tx_id, x_guarded_new_left.bf()->header_.page_id_,
                  x_guarded_new_root.bf()->header_.page_id_, x_guarded_meta.bf()->header_.page_id_,
                  sep_info)
        .Submit();
    x_guarded_old_root.SyncSystemTxId(sys_tx_id);
  }

  // 3.2. move half of the old root to the new left,
  // 3.3. insert separator key into new root,
  x_guarded_new_root->right_most_child_swip_ = x_guarded_old_root.bf();
  x_guarded_old_root->Split(x_guarded_new_root, x_guarded_new_left, sep_info);

  // 3.4. update meta node to point to new root
  x_guarded_meta->right_most_child_swip_ = x_guarded_new_root.bf();
  height_++;
}

/// Split a non-root node, 3 nodes are involved in the split:
/// parent(child) -> parent(newLeft, child)
///
/// parent         parent
///   |            |   |
/// child     newLeft child
///
void BTreeGeneric::split_non_root_may_jump(TXID sys_tx_id,
                                           GuardedBufferFrame<BTreeNode>& guarded_parent,
                                           GuardedBufferFrame<BTreeNode>& guarded_child,
                                           const BTreeNode::SeparatorInfo& sep_info,
                                           uint16_t space_needed_for_separator) {
  auto x_guarded_parent = ExclusiveGuardedBufferFrame(std::move(guarded_parent));
  auto x_guarded_child = ExclusiveGuardedBufferFrame(std::move(guarded_child));

  LEAN_DCHECK(!is_meta_node(guarded_parent), "Parent should not be meta node");
  LEAN_DCHECK(!x_guarded_parent->is_leaf_, "Parent should not be leaf node");

  // 1. create new left, lock it exclusively, write wal on demand
  auto* new_left_bf = &store_->buffer_manager_->AllocNewPageMayJump(tree_id_);
  auto guarded_new_left = GuardedBufferFrame<BTreeNode>(store_->buffer_manager_.get(), new_left_bf);
  auto x_guarded_new_left = ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guarded_new_left));
  if (config_.enable_wal_) {
    auto* new_left_bf = x_guarded_new_left.bf();
    WalBuilder<WalInitPage>{}
        .InitHeader(0, 0, 0, new_left_bf->page_.psn_, new_left_bf->header_.page_id_, tree_id_)
        .InitData(sys_tx_id, tree_id_, x_guarded_child->is_leaf_)
        .Submit();
    x_guarded_new_left.SyncSystemTxId(sys_tx_id);
  }
  x_guarded_new_left.InitPayload(x_guarded_child->is_leaf_);

  // 2.1. write wal on demand or simply mark as dirty
  if (config_.enable_wal_) {
    auto* child_bf = x_guarded_child.bf();
    WalBuilder<WalSplitNonRoot>{}
        .InitHeader(0, 0, 0, child_bf->page_.psn_, child_bf->header_.page_id_, tree_id_)
        .InitData(sys_tx_id, x_guarded_parent.bf()->header_.page_id_,
                  x_guarded_new_left.bf()->header_.page_id_, sep_info)
        .Submit();
    x_guarded_parent.SyncSystemTxId(sys_tx_id);
    x_guarded_child.SyncSystemTxId(sys_tx_id);
  }

  // 2.2. make room for separator key in parent node
  // 2.3. move half of the old root to the new left
  // 2.4. insert separator key into parent node
  x_guarded_parent->RequestSpaceFor(space_needed_for_separator);
  x_guarded_child->Split(x_guarded_parent, x_guarded_new_left, sep_info);
}

bool BTreeGeneric::TryMergeMayJump(TXID sys_tx_id, BufferFrame& to_merge, bool swizzle_sibling) {
  auto parent_handler = find_parent_eager(*this, to_merge);
  GuardedBufferFrame<BTreeNode> guarded_parent(store_->buffer_manager_.get(),
                                               std::move(parent_handler.parent_guard_),
                                               parent_handler.parent_bf_);
  GuardedBufferFrame<BTreeNode> guarded_child(store_->buffer_manager_.get(), guarded_parent,
                                              parent_handler.child_swip_);
  auto pos_in_parent = parent_handler.pos_in_parent_;
  if (is_meta_node(guarded_parent) ||
      guarded_child->FreeSpaceAfterCompaction() < BTreeNode::UnderFullSize()) {
    guarded_parent.unlock();
    guarded_child.unlock();
    return false;
  }

  if (guarded_parent->num_slots_ <= 1) {
    return false;
  }

  LEAN_DCHECK(pos_in_parent <= guarded_parent->num_slots_,
              "Invalid position in parent, posInParent={}, childSizeOfParent={}", pos_in_parent,
              guarded_parent->num_slots_);
  guarded_parent.JumpIfModifiedByOthers();
  guarded_child.JumpIfModifiedByOthers();

  // TODO: write WALs
  auto merge_and_reclaim_left = [&]() {
    auto* left_swip = guarded_parent->ChildSwip(pos_in_parent - 1);
    if (!swizzle_sibling && left_swip->IsEvicted()) {
      return false;
    }
    auto guarded_left =
        GuardedBufferFrame<BTreeNode>(store_->buffer_manager_.get(), guarded_parent, *left_swip);
    auto x_guarded_parent = ExclusiveGuardedBufferFrame(std::move(guarded_parent));
    auto x_guarded_child = ExclusiveGuardedBufferFrame(std::move(guarded_child));
    auto x_guarded_left = ExclusiveGuardedBufferFrame(std::move(guarded_left));

    LEAN_DCHECK(x_guarded_child->is_leaf_ == x_guarded_left->is_leaf_);

    if (!x_guarded_left->merge(pos_in_parent - 1, x_guarded_parent, x_guarded_child)) {
      guarded_parent = std::move(x_guarded_parent);
      guarded_child = std::move(x_guarded_child);
      guarded_left = std::move(x_guarded_left);
      return false;
    }

    if (config_.enable_wal_) {
      guarded_parent.SyncSystemTxId(sys_tx_id);
      guarded_child.SyncSystemTxId(sys_tx_id);
      guarded_left.SyncSystemTxId(sys_tx_id);
    }

    x_guarded_left.Reclaim();
    guarded_parent = std::move(x_guarded_parent);
    guarded_child = std::move(x_guarded_child);
    return true;
  };
  auto merge_and_reclaim_right = [&]() {
    auto& right_swip = ((pos_in_parent + 1) == guarded_parent->num_slots_)
                           ? guarded_parent->right_most_child_swip_
                           : *guarded_parent->ChildSwip(pos_in_parent + 1);
    if (!swizzle_sibling && right_swip.IsEvicted()) {
      return false;
    }
    auto guarded_right =
        GuardedBufferFrame<BTreeNode>(store_->buffer_manager_.get(), guarded_parent, right_swip);
    auto x_guarded_parent = ExclusiveGuardedBufferFrame(std::move(guarded_parent));
    auto x_guarded_child = ExclusiveGuardedBufferFrame(std::move(guarded_child));
    auto x_guarded_right = ExclusiveGuardedBufferFrame(std::move(guarded_right));

    LEAN_DCHECK(x_guarded_child->is_leaf_ == x_guarded_right->is_leaf_);

    if (!x_guarded_child->merge(pos_in_parent, x_guarded_parent, x_guarded_right)) {
      guarded_parent = std::move(x_guarded_parent);
      guarded_child = std::move(x_guarded_child);
      guarded_right = std::move(x_guarded_right);
      return false;
    }

    if (config_.enable_wal_) {
      guarded_parent.SyncSystemTxId(sys_tx_id);
      guarded_child.SyncSystemTxId(sys_tx_id);
      guarded_right.SyncSystemTxId(sys_tx_id);
    }

    x_guarded_child.Reclaim();
    guarded_parent = std::move(x_guarded_parent);
    guarded_right = std::move(x_guarded_right);
    return true;
  };

  SCOPED_DEFER({
    if (!is_meta_node(guarded_parent) &&
        guarded_parent->FreeSpaceAfterCompaction() >= BTreeNode::UnderFullSize()) {
      JUMPMU_TRY() {
        TryMergeMayJump(sys_tx_id, *guarded_parent.bf_, true);
      }
      JUMPMU_CATCH() {
      }
    }
  });

  bool succeed = false;
  if (pos_in_parent > 0) {
    succeed = merge_and_reclaim_left();
  }
  if (!succeed && pos_in_parent < guarded_parent->num_slots_) {
    succeed = merge_and_reclaim_right();
  }

  return succeed;
}

// ret: 0 did nothing, 1 full, 2 partial
int16_t BTreeGeneric::merge_left_into_right(
    ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_parent, int16_t lhs_slot_id,
    ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_left,
    ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_right, bool full_merge_or_nothing) {
  // TODO: corner cases: new upper fence is larger than the older one.
  uint32_t space_upper_bound = x_guarded_left->MergeSpaceUpperBound(x_guarded_right);
  if (space_upper_bound <= BTreeNode::Size()) {
    // Do a full merge TODO: threshold
    bool succ = x_guarded_left->merge(lhs_slot_id, x_guarded_parent, x_guarded_right);
    static_cast<void>(succ);
    assert(succ);
    x_guarded_left.Reclaim();
    return 1;
  }

  if (full_merge_or_nothing)
    return 0;

  // Do a partial merge
  // Remove a key at a time from the merge and check if now it fits
  int16_t till_slot_id = -1;
  for (int16_t i = 0; i < x_guarded_left->num_slots_; i++) {
    space_upper_bound -= sizeof(BTreeNodeSlot) + x_guarded_left->KeySizeWithoutPrefix(i) +
                         x_guarded_left->ValSize(i);
    if (space_upper_bound +
            (x_guarded_left->GetFullKeyLen(i) - x_guarded_right->lower_fence_.size_) <
        BTreeNode::Size() * 1.0) {
      till_slot_id = i + 1;
      break;
    }
  }
  if (!(till_slot_id != -1 && till_slot_id < (x_guarded_left->num_slots_ - 1))) {
    return 0; // false
  }

  assert((space_upper_bound + (x_guarded_left->GetFullKeyLen(till_slot_id - 1) -
                               x_guarded_right->lower_fence_.size_)) < BTreeNode::Size() * 1.0);
  assert(till_slot_id > 0);

  uint16_t copy_from_count = x_guarded_left->num_slots_ - till_slot_id;

  uint16_t new_left_upper_fence_size = x_guarded_left->GetFullKeyLen(till_slot_id - 1);
  ENSURE(new_left_upper_fence_size > 0);
  auto new_left_upper_fence_buf = utils::JumpScopedArray<uint8_t>(new_left_upper_fence_size);
  auto* new_left_upper_fence = new_left_upper_fence_buf->get();
  x_guarded_left->CopyFullKey(till_slot_id - 1, new_left_upper_fence);

  if (!x_guarded_parent->PrepareInsert(new_left_upper_fence_size, 0)) {
    return 0; // false
  }

  auto node_buf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
  {
    Slice new_lower_fence{new_left_upper_fence, new_left_upper_fence_size};
    Slice new_upper_fence{x_guarded_right->GetUpperFence()};
    auto* tmp = BTreeNode::New(node_buf->get(), true, new_lower_fence, new_upper_fence);

    x_guarded_left->CopyKeyValueRange(tmp, 0, till_slot_id, copy_from_count);
    x_guarded_right->CopyKeyValueRange(tmp, copy_from_count, 0, x_guarded_right->num_slots_);
    memcpy(x_guarded_right.GetPagePayloadPtr(), tmp, BTreeNode::Size());
    x_guarded_right->MakeHint();

    // Nothing to do for the right node's separator
    assert(x_guarded_right->CompareKeyWithBoundaries(new_lower_fence) == 1);
  }

  {
    Slice new_lower_fence{x_guarded_left->GetLowerFence()};
    Slice new_upper_fence{new_left_upper_fence, new_left_upper_fence_size};
    auto* tmp = BTreeNode::New(node_buf->get(), true, new_lower_fence, new_upper_fence);

    x_guarded_left->CopyKeyValueRange(tmp, 0, 0, x_guarded_left->num_slots_ - copy_from_count);
    memcpy(x_guarded_left.GetPagePayloadPtr(), tmp, BTreeNode::Size());
    x_guarded_left->MakeHint();

    assert(x_guarded_left->CompareKeyWithBoundaries(new_upper_fence) == 0);

    x_guarded_parent->RemoveSlot(lhs_slot_id);
    ENSURE(x_guarded_parent->PrepareInsert(x_guarded_left->upper_fence_.size_, sizeof(Swip)));
    auto swip = x_guarded_left.swip();
    Slice key = x_guarded_left->GetUpperFence();
    Slice val(reinterpret_cast<uint8_t*>(&swip), sizeof(Swip));
    x_guarded_parent->Insert(key, val);
  }
  return 2;
}

// returns true if it has exclusively locked anything
BTreeGeneric::XMergeReturnCode BTreeGeneric::XMerge(GuardedBufferFrame<BTreeNode>& guarded_parent,
                                                    GuardedBufferFrame<BTreeNode>& guarded_child,
                                                    ParentSwipHandler& parent_handler) {
  if (guarded_child->FillFactorAfterCompaction() >= 0.9) {
    return XMergeReturnCode::kNothing;
  }

  const int64_t max_merge_pages = store_->store_option_->xmerge_k_;
  GuardedBufferFrame<BTreeNode> guarded_nodes[max_merge_pages];
  bool fully_merged[max_merge_pages];

  int64_t pos = parent_handler.pos_in_parent_;
  int64_t page_count = 1;
  int64_t max_right;

  guarded_nodes[0] = std::move(guarded_child);
  fully_merged[0] = false;
  double total_fill_factor = guarded_nodes[0]->FillFactorAfterCompaction();

  // Handle upper swip instead of avoiding guardedParent->num_slots_ -1 swip
  if (is_meta_node(guarded_parent) || !guarded_nodes[0]->is_leaf_) {
    guarded_child = std::move(guarded_nodes[0]);
    return XMergeReturnCode::kNothing;
  }
  for (max_right = pos + 1;
       (max_right - pos) < max_merge_pages && (max_right + 1) < guarded_parent->num_slots_;
       max_right++) {
    if (!guarded_parent->ChildSwip(max_right)->IsHot()) {
      guarded_child = std::move(guarded_nodes[0]);
      return XMergeReturnCode::kNothing;
    }

    guarded_nodes[max_right - pos] = GuardedBufferFrame<BTreeNode>(
        store_->buffer_manager_.get(), guarded_parent, *guarded_parent->ChildSwip(max_right));
    fully_merged[max_right - pos] = false;
    total_fill_factor += guarded_nodes[max_right - pos]->FillFactorAfterCompaction();
    page_count++;
    if ((page_count - std::ceil(total_fill_factor)) >= (1)) {
      // we can probably save a page by merging all together so there is no need
      // to look furhter
      break;
    }
  }
  if (((page_count - std::ceil(total_fill_factor))) < (1)) {
    guarded_child = std::move(guarded_nodes[0]);
    return XMergeReturnCode::kNothing;
  }

  ExclusiveGuardedBufferFrame<BTreeNode> x_guarded_parent = std::move(guarded_parent);
  // TODO(zz-jason): support wal and sync system tx id
  // TXID sysTxId = utils::tlsStore->MvccManager()->AllocSysTxTs();
  // xGuardedParent.SyncSystemTxId(sysTxId);

  XMergeReturnCode ret_code = XMergeReturnCode::kPartialMerge;
  int16_t left_hand, right_hand, ret;
  while (true) {
    for (right_hand = max_right; right_hand > pos; right_hand--) {
      if (fully_merged[right_hand - pos]) {
        continue;
      }
      break;
    }
    if (right_hand == pos)
      break;

    left_hand = right_hand - 1;

    {
      ExclusiveGuardedBufferFrame<BTreeNode> x_guarded_right(
          std::move(guarded_nodes[right_hand - pos]));
      ExclusiveGuardedBufferFrame<BTreeNode> x_guarded_left(
          std::move(guarded_nodes[left_hand - pos]));
      // TODO(zz-jason): support wal and sync system tx id
      // xGuardedRight.SyncSystemTxId(sysTxId);
      // xGuardedLeft.SyncSystemTxId(sysTxId);
      max_right = left_hand;
      ret = merge_left_into_right(x_guarded_parent, left_hand, x_guarded_left, x_guarded_right,
                                  left_hand == pos);
      // we unlock only the left page, the right one should not be touched again
      if (ret == 1) {
        fully_merged[left_hand - pos] = true;
        ret_code = XMergeReturnCode::kFullMerge;
      } else if (ret == 2) {
        guarded_nodes[left_hand - pos] = std::move(x_guarded_left);
      } else if (ret == 0) {
        break;
      } else {
        Log::Fatal("Invalid return code from mergeLeftIntoRight");
      }
    }
  }
  if (guarded_child.guard_.state_ == GuardState::kMoved) {
    guarded_child = std::move(guarded_nodes[0]);
  }
  guarded_parent = std::move(x_guarded_parent);
  return ret_code;
}

// -------------------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------------------
int64_t BTreeGeneric::iterate_all_pages(BTreeNodeCallback inner, BTreeNodeCallback leaf) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> guarded_parent(store_->buffer_manager_.get(), meta_node_swip_);
      GuardedBufferFrame<BTreeNode> guarded_child(store_->buffer_manager_.get(), guarded_parent,
                                                  guarded_parent->right_most_child_swip_);
      int64_t result = iterate_all_pages_recursive(guarded_child, inner, leaf);
      JUMPMU_RETURN result;
    }
    JUMPMU_CATCH() {
    }
  }
}

int64_t BTreeGeneric::iterate_all_pages_recursive(GuardedBufferFrame<BTreeNode>& guarded_node,
                                                  BTreeNodeCallback inner, BTreeNodeCallback leaf) {
  if (guarded_node->is_leaf_) {
    return leaf(guarded_node.ref());
  }
  int64_t res = inner(guarded_node.ref());
  for (uint16_t i = 0; i < guarded_node->num_slots_; i++) {
    auto* child_swip = guarded_node->ChildSwip(i);
    auto guarded_child =
        GuardedBufferFrame<BTreeNode>(store_->buffer_manager_.get(), guarded_node, *child_swip);
    guarded_child.JumpIfModifiedByOthers();
    res += iterate_all_pages_recursive(guarded_child, inner, leaf);
  }

  Swip& child_swip = guarded_node->right_most_child_swip_;
  auto guarded_child =
      GuardedBufferFrame<BTreeNode>(store_->buffer_manager_.get(), guarded_node, child_swip);
  guarded_child.JumpIfModifiedByOthers();
  res += iterate_all_pages_recursive(guarded_child, inner, leaf);

  return res;
}

std::string BTreeGeneric::Summary() {
  GuardedBufferFrame<BTreeNode> guarded_meta(store_->buffer_manager_.get(), meta_node_swip_);
  GuardedBufferFrame<BTreeNode> guarded_root(store_->buffer_manager_.get(), guarded_meta,
                                             guarded_meta->right_most_child_swip_);
  uint64_t num_all_pages = CountAllPages();
  return std::format("entries={}, nodes={}, innerNodes={}, spacePct={:.2f}, height={}"
                     ", rootSlots={}, freeSpaceAfterCompaction={}",
                     CountEntries(), num_all_pages, CountInnerPages(),
                     (num_all_pages * BTreeNode::Size()) /
                         (double)store_->store_option_->buffer_pool_size_,
                     GetHeight(), guarded_root->num_slots_, FreeSpaceAfterCompaction());
}

StringMap BTreeGeneric::Serialize() {
  LEAN_DCHECK(meta_node_swip_.AsBufferFrame().page_.btree_id_ == tree_id_);
  auto& meta_bf = meta_node_swip_.AsBufferFrame();
  auto meta_page_id = meta_bf.header_.page_id_;
  auto res = store_->buffer_manager_->CheckpointBufferFrame(meta_bf);
  if (!res) {
    Log::Fatal("Failed to checkpoint meta node: {}", res.error().ToString());
  }
  return {{kTreeId, std::to_string(tree_id_)},
          {kHeight, std::to_string(GetHeight())},
          {kMetaPageId, std::to_string(meta_page_id)}};
}

void BTreeGeneric::Deserialize(StringMap map) {
  tree_id_ = std::stoull(map[kTreeId]);
  height_ = std::stoull(map[kHeight]);
  meta_node_swip_.Evict(std::stoull(map[kMetaPageId]));

  // load meta node to memory
  HybridMutex dummy_latch;
  HybridGuard dummy_guard(&dummy_latch);
  dummy_guard.ToOptimisticSpin();
  while (true) {
    JUMPMU_TRY() {
      meta_node_swip_ = store_->buffer_manager_->ResolveSwipMayJump(dummy_guard, meta_node_swip_);
      JUMPMU_BREAK;
    }
    JUMPMU_CATCH() {
    }
  }
  meta_node_swip_.AsBufferFrame().header_.keep_in_memory_ = true;
  LEAN_DCHECK(meta_node_swip_.AsBufferFrame().page_.btree_id_ == tree_id_,
              "MetaNode has wrong BTreeId, pageId={}, expected={}, actual={}",
              meta_node_swip_.AsBufferFrame().header_.page_id_, tree_id_,
              meta_node_swip_.AsBufferFrame().page_.btree_id_);
}

void BTreeGeneric::ToJson(BTreeGeneric& btree, utils::JsonObj* btree_json_obj) {
  constexpr char kMetaNode[] = "meta_node";
  constexpr char kRootNode[] = "root_node";

  // meta node
  GuardedBufferFrame<BTreeNode> guarded_meta_node(btree.store_->buffer_manager_.get(),
                                                  btree.meta_node_swip_);
  utils::JsonObj meta_json_obj;
  utils::ToJson(guarded_meta_node.bf_, &meta_json_obj);
  btree_json_obj->AddJsonObj(kMetaNode, meta_json_obj);

  // root node
  GuardedBufferFrame<BTreeNode> guarded_root_node(btree.store_->buffer_manager_.get(),
                                                  guarded_meta_node,
                                                  guarded_meta_node->right_most_child_swip_);
  utils::JsonObj root_json_obj;
  to_json_recursive(btree, guarded_root_node, &root_json_obj);
  btree_json_obj->AddJsonObj(kRootNode, root_json_obj);
}

void BTreeGeneric::to_json_recursive(BTreeGeneric& btree,
                                     GuardedBufferFrame<BTreeNode>& guarded_node,
                                     utils::JsonObj* node_json_obj) {

  constexpr char kBtreeNode[] = "btree_node";
  constexpr char kChildren[] = "children";

  // buffer frame header
  utils::ToJson(guarded_node.bf_, node_json_obj);

  // btree node
  {
    utils::JsonObj btree_node_json_obj;
    utils::ToJson(guarded_node.ptr(), &btree_node_json_obj);
    node_json_obj->AddJsonObj(kBtreeNode, btree_node_json_obj);
  }

  if (guarded_node->is_leaf_) {
    return;
  }

  utils::JsonArray children_json_array;
  for (auto i = 0u; i < guarded_node->num_slots_; ++i) {
    auto* child_swip = guarded_node->ChildSwip(i);
    GuardedBufferFrame<BTreeNode> guarded_child(btree.store_->buffer_manager_.get(), guarded_node,
                                                *child_swip);
    utils::JsonObj child_json_obj;
    to_json_recursive(btree, guarded_child, &child_json_obj);
    guarded_child.unlock();

    children_json_array.AppendJsonObj(child_json_obj);
  }

  if (guarded_node->right_most_child_swip_ != nullptr) {
    GuardedBufferFrame<BTreeNode> guarded_child(btree.store_->buffer_manager_.get(), guarded_node,
                                                guarded_node->right_most_child_swip_);
    utils::JsonObj child_json_obj;
    to_json_recursive(btree, guarded_child, &child_json_obj);
    guarded_child.unlock();

    children_json_array.AppendJsonObj(child_json_obj);
  }

  node_json_obj->AddJsonArray(kChildren, children_json_array);
}

} // namespace leanstore::storage::btree
