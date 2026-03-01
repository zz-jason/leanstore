#include "leanstore/btree/b_tree_generic.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/column_store/column_block_builder.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/table/table.hpp"
#include "leanstore/tx/transaction_kv.hpp"

#include <algorithm>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

namespace leanstore {

namespace {

struct BlockRefEntry {
  column_store::ColumnBlockRef ref_;
  std::string max_key_;
};

struct FenceKeyBytes {
  bool is_infinity_ = false;
  std::string key_;
};

struct NodeRef {
  Swip swip_;
  std::string max_key_;
  FenceKeyBytes upper_fence_;
};

struct LeafGroup {
  std::vector<BlockRefEntry> refs_;
  FenceKeyBytes upper_fence_;
};

struct SubtreeCollect {
  std::vector<BufferFrame*> row_leaf_frames_;
  std::vector<BufferFrame*> subtree_frames_;
  bool any_column_leaf_ = false;
  bool any_row_leaf_ = false;
};

struct ColumnSubtreePlan {
  // Parent fences for the subtree being converted.
  FenceKeyBytes parent_lower_;
  FenceKeyBytes parent_upper_;
  // Collected leaf frames plus flags that describe row/column mix.
  SubtreeCollect collect_;
  // Subtree height and the number of inner levels to rebuild above column leaves.
  uint64_t subtree_height_ = 0;
  uint64_t levels_to_build_ = 0;
  // Column block payloads produced from row leaves and their grouped leaf batches.
  std::vector<column_store::ColumnBlockPayload> payloads_;
  std::vector<LeafGroup> groups_;
  // Indicates a no-op conversion when the subtree is already column-only.
  bool skip_ = false;
};

Slice FenceSlice(const FenceKeyBytes& fence) {
  if (fence.is_infinity_) {
    return Slice(static_cast<const uint8_t*>(nullptr), 0);
  }
  return Slice(reinterpret_cast<const uint8_t*>(fence.key_.data()), fence.key_.size());
}

FenceKeyBytes ExtractFence(BTreeNode& node, bool lower) {
  FenceKeyBytes fence;
  if (lower) {
    fence.is_infinity_ = node.lower_fence_.IsInfinity();
    if (!fence.is_infinity_) {
      auto slice = node.GetLowerFence();
      fence.key_.assign(reinterpret_cast<const char*>(slice.data()), slice.size());
    }
  } else {
    fence.is_infinity_ = node.upper_fence_.IsInfinity();
    if (!fence.is_infinity_) {
      auto slice = node.GetUpperFence();
      fence.key_.assign(reinterpret_cast<const char*>(slice.data()), slice.size());
    }
  }
  return fence;
}

bool CanFitRefs(const FenceKeyBytes& lower_fence, const FenceKeyBytes& upper_fence,
                const std::vector<BlockRefEntry>& refs, std::vector<uint8_t>& buffer) {
  auto* node =
      BTreeNode::New(buffer.data(), true, FenceSlice(lower_fence), FenceSlice(upper_fence));
  node->has_garbage_ = false;
  for (const auto& entry : refs) {
    const uint16_t key_size = static_cast<uint16_t>(entry.max_key_.size());
    const uint16_t val_size = sizeof(column_store::ColumnBlockRef);
    if (!node->PrepareInsert(key_size, val_size)) {
      return false;
    }
    int32_t slot = node->InsertDoNotCopyPayload(Slice(entry.max_key_), val_size);
    std::memcpy(node->ValData(slot), &entry.ref_, sizeof(entry.ref_));
  }
  return true;
}

bool CanFitInner(const FenceKeyBytes& lower_fence, const FenceKeyBytes& upper_fence,
                 const std::vector<NodeRef>& children, std::vector<uint8_t>& buffer) {
  if (children.empty()) {
    return false;
  }
  auto* node =
      BTreeNode::New(buffer.data(), false, FenceSlice(lower_fence), FenceSlice(upper_fence));
  node->has_garbage_ = false;
  for (size_t i = 0; i + 1 < children.size(); ++i) {
    const auto& key = children[i].max_key_;
    const uint16_t key_size = static_cast<uint16_t>(key.size());
    if (!node->PrepareInsert(key_size, sizeof(Swip))) {
      return false;
    }
    int32_t slot = node->InsertDoNotCopyPayload(Slice(key), sizeof(Swip));
    std::memcpy(node->ValData(slot), &children[i].swip_, sizeof(Swip));
  }
  return true;
}

bool CanFitParentSeparators(const FenceKeyBytes& parent_lower, const FenceKeyBytes& parent_upper,
                            const std::vector<FenceKeyBytes>& leaf_upper_fences) {
  if (leaf_upper_fences.empty()) {
    return false;
  }
  std::vector<uint8_t> buffer(BTreeNode::Size());
  auto* node =
      BTreeNode::New(buffer.data(), false, FenceSlice(parent_lower), FenceSlice(parent_upper));
  node->has_garbage_ = false;
  Swip dummy;
  for (size_t i = 0; i + 1 < leaf_upper_fences.size(); ++i) {
    const auto& fence = leaf_upper_fences[i];
    if (fence.is_infinity_) {
      return false;
    }
    const uint16_t key_size = static_cast<uint16_t>(fence.key_.size());
    if (!node->PrepareInsert(key_size, sizeof(Swip))) {
      return false;
    }
    int32_t slot = node->InsertDoNotCopyPayload(
        Slice(reinterpret_cast<const uint8_t*>(fence.key_.data()), fence.key_.size()),
        sizeof(Swip));
    std::memcpy(node->ValData(slot), &dummy, sizeof(dummy));
  }
  return true;
}

Result<uint64_t> CollectSubtree(BasicKV& btree, GuardedBufferFrame<BTreeNode>& guarded_node,
                                SubtreeCollect& collect, bool include_self) {
  if (guarded_node->is_leaf_) {
    if (include_self) {
      collect.subtree_frames_.push_back(guarded_node.bf_);
    }
    if (column_store::IsColumnLeaf(*guarded_node.bf_)) {
      collect.any_column_leaf_ = true;
    } else {
      collect.any_row_leaf_ = true;
      collect.row_leaf_frames_.push_back(guarded_node.bf_);
    }
    return 1;
  }

  if (include_self) {
    collect.subtree_frames_.push_back(guarded_node.bf_);
  }

  uint64_t child_height = 0;
  for (uint16_t i = 0; i <= guarded_node->num_slots_; ++i) {
    GuardedBufferFrame<BTreeNode> guarded_child(btree.store_->buffer_manager_.get(), guarded_node,
                                                *guarded_node->ChildSwipIncludingRightMost(i),
                                                LatchMode::kOptimisticSpin);
    auto res = CollectSubtree(btree, guarded_child, collect, true);
    if (!res) {
      return std::move(res.error());
    }
    if (child_height == 0) {
      child_height = res.value();
    } else if (child_height != res.value()) {
      return Error::General("inconsistent subtree height");
    }
  }

  return child_height + 1;
}

Result<void> AppendLeafRows(const TableDefinition& def, TableCodec& codec,
                            column_store::ColumnBlockBuilder& builder,
                            GuardedBufferFrame<BTreeNode>& leaf,
                            column_store::ColumnStoreStats& stats,
                            std::vector<column_store::ColumnBlockPayload>& payloads,
                            std::string* out_last_key) {
  if (out_last_key == nullptr) {
    return Error::General("null last key output");
  }
  const auto col_count = def.schema_.Columns().size();
  std::vector<Datum> datums(col_count);
  auto nulls = std::make_unique<bool[]>(col_count);
  lean_row row{.columns = datums.data(),
               .nulls = nulls.get(),
               .num_columns = static_cast<uint32_t>(datums.size())};
  std::string key_bytes;

  for (uint16_t slot = 0; slot < leaf->num_slots_; ++slot) {
    auto value = leaf->Value(slot);

    if (auto res = codec.DecodeValue(value, &row); !res) {
      return std::move(res.error());
    }

    builder.AddRow(row);
    stats.row_count_++;
    if (builder.ShouldFlush()) {
      const auto key_len = leaf->GetFullKeyLen(slot);
      key_bytes.resize(key_len);
      leaf->CopyFullKey(slot, reinterpret_cast<uint8_t*>(key_bytes.data()));
      auto block_res = builder.Finalize(Slice(key_bytes));
      if (!block_res) {
        return std::move(block_res.error());
      }
      payloads.push_back(std::move(block_res.value()));
    }
  }
  if (leaf->num_slots_ > 0) {
    const uint16_t last_slot = static_cast<uint16_t>(leaf->num_slots_ - 1);
    const auto key_len = leaf->GetFullKeyLen(last_slot);
    out_last_key->resize(key_len);
    leaf->CopyFullKey(last_slot, reinterpret_cast<uint8_t*>(out_last_key->data()));
  }
  return {};
}

Result<std::vector<BlockRefEntry>> PersistBlocks(
    BasicKV& btree, const std::vector<column_store::ColumnBlockPayload>& payloads,
    column_store::ColumnStoreStats& stats) {
  std::vector<BlockRefEntry> block_refs;
  block_refs.reserve(payloads.size());
  for (const auto& payload : payloads) {
    auto ref_res = column_store::WriteColumnBlock(btree.store_, btree.tree_id_, payload.bytes_,
                                                  payload.row_count_);
    if (!ref_res) {
      return std::move(ref_res.error());
    }
    block_refs.push_back({.ref_ = ref_res.value(), .max_key_ = payload.max_key_});
    stats.block_count_++;
    stats.column_pages_ += ref_res.value().page_count_;
    stats.column_bytes_ += ref_res.value().block_bytes_;
  }
  return block_refs;
}

// Groups column block refs into leaf-sized batches based on B-tree page capacity and fences.
// It scans refs in key order, packs as many as fit, and assigns upper fences to each group so
// downstream leaf construction can preserve the parent subtree key range.
Result<std::vector<LeafGroup>> BuildLeafGroups(const std::vector<BlockRefEntry>& refs,
                                               const FenceKeyBytes& parent_lower,
                                               const FenceKeyBytes& parent_upper) {
  std::vector<LeafGroup> groups;
  if (refs.empty()) {
    return Error::General("no block refs to group");
  }

  FenceKeyBytes lower_fence = parent_lower;
  std::vector<BlockRefEntry> current;
  current.reserve(refs.size());
  std::vector<uint8_t> fit_buffer(BTreeNode::Size());

  for (const auto& entry : refs) {
    current.push_back(entry);
    FenceKeyBytes upper_candidate{.is_infinity_ = false, .key_ = entry.max_key_};
    if (CanFitRefs(lower_fence, upper_candidate, current, fit_buffer)) {
      continue;
    }

    if (current.size() == 1) {
      return Error::General("block ref too large for leaf");
    }
    BlockRefEntry overflow_entry = std::move(current.back());
    current.pop_back();
    const std::string current_upper = current.back().max_key_;
    groups.push_back({.refs_ = std::move(current),
                      .upper_fence_ = FenceKeyBytes{.is_infinity_ = false, .key_ = current_upper}});
    lower_fence = groups.back().upper_fence_;
    current.clear();
    current.push_back(std::move(overflow_entry));
  }

  if (!current.empty()) {
    const FenceKeyBytes& upper_fence = parent_upper;
    if (!CanFitRefs(lower_fence, upper_fence, current, fit_buffer)) {
      std::vector<BlockRefEntry> remaining = std::move(current);
      while (!remaining.empty() && !CanFitRefs(lower_fence, upper_fence, remaining, fit_buffer)) {
        std::vector<BlockRefEntry> prefix;
        prefix.reserve(remaining.size());
        for (size_t i = 0; i < remaining.size(); ++i) {
          prefix.push_back(remaining[i]);
          FenceKeyBytes candidate_upper{.is_infinity_ = false, .key_ = remaining[i].max_key_};
          if (!CanFitRefs(lower_fence, candidate_upper, prefix, fit_buffer)) {
            prefix.pop_back();
            break;
          }
        }
        if (prefix.empty()) {
          return Error::General("block ref too large for leaf");
        }
        FenceKeyBytes prefix_upper{.is_infinity_ = false, .key_ = prefix.back().max_key_};
        groups.push_back({.refs_ = std::move(prefix), .upper_fence_ = prefix_upper});
        lower_fence = groups.back().upper_fence_;
        remaining.erase(remaining.begin(), remaining.begin() + groups.back().refs_.size());
      }
      if (!remaining.empty()) {
        groups.push_back({.refs_ = std::move(remaining), .upper_fence_ = upper_fence});
      }
      return groups;
    }
    groups.push_back({.refs_ = std::move(current), .upper_fence_ = upper_fence});
  }

  return groups;
}

// Builds one inner level from child refs by packing separators until full, then splitting groups.
// It can run in "dry-run" mode when btree is null to preflight separator fit without allocating
// pages; otherwise it allocates inner nodes and returns swips plus their max-key fences.
Result<std::vector<NodeRef>> BuildInnerLevel(BasicKV* btree, const std::vector<NodeRef>& children,
                                             const FenceKeyBytes& parent_lower,
                                             const FenceKeyBytes& parent_upper) {
  std::vector<NodeRef> next_level;
  if (children.empty()) {
    return Error::General("no child refs to group");
  }

  auto emit_group = [&](const std::vector<NodeRef>& group, const FenceKeyBytes& lower_fence,
                        const FenceKeyBytes& upper_fence) -> Result<void> {
    if (group.empty()) {
      return Error::General("child ref too large for inner node");
    }
    const std::string max_key = group.back().max_key_;
    Swip swip;
    if (btree != nullptr) {
      auto& bf = btree->store_->buffer_manager_->AllocNewPage(btree->tree_id_);
      GuardedBufferFrame<BTreeNode> guarded_inner(btree->store_->buffer_manager_.get(), &bf);
      guarded_inner.ToExclusiveMayJump();
      auto* inner = BTreeNode::New(guarded_inner.bf_->page_.payload_, false,
                                   FenceSlice(lower_fence), FenceSlice(upper_fence));
      inner->has_garbage_ = false;
      for (size_t i = 0; i + 1 < group.size(); ++i) {
        const auto& key = group[i].max_key_;
        const uint16_t key_size = static_cast<uint16_t>(key.size());
        if (!inner->PrepareInsert(key_size, sizeof(Swip))) {
          return Error::General("inner node has insufficient space for separators");
        }
        Swip child_swip = group[i].swip_;
        inner->Insert(Slice(reinterpret_cast<const uint8_t*>(key.data()), key.size()),
                      Slice(reinterpret_cast<const uint8_t*>(&child_swip), sizeof(Swip)));
      }
      inner->right_most_child_swip_ = group.back().swip_;
      guarded_inner.UpdatePageVersion();
      swip = Swip(&bf);
    }
    next_level.push_back({.swip_ = swip, .max_key_ = max_key, .upper_fence_ = upper_fence});
    return {};
  };

  FenceKeyBytes lower_fence = parent_lower;
  std::vector<NodeRef> current;
  current.reserve(children.size());
  std::vector<uint8_t> fit_buffer(BTreeNode::Size());

  for (const auto& child : children) {
    current.push_back(child);
    FenceKeyBytes upper_candidate{.is_infinity_ = false, .key_ = child.max_key_};
    if (CanFitInner(lower_fence, upper_candidate, current, fit_buffer)) {
      continue;
    }

    if (current.size() == 1) {
      return Error::General("child ref too large for inner node");
    }
    NodeRef overflow_child = std::move(current.back());
    current.pop_back();
    FenceKeyBytes current_upper{.is_infinity_ = false, .key_ = current.back().max_key_};
    if (auto res = emit_group(current, lower_fence, current_upper); !res) {
      return std::move(res.error());
    }
    lower_fence = next_level.back().upper_fence_;
    current.clear();
    current.push_back(std::move(overflow_child));
  }

  if (!current.empty()) {
    const FenceKeyBytes& upper_fence = parent_upper;
    if (!CanFitInner(lower_fence, upper_fence, current, fit_buffer)) {
      std::vector<NodeRef> remaining = std::move(current);
      while (!remaining.empty() && !CanFitInner(lower_fence, upper_fence, remaining, fit_buffer)) {
        std::vector<NodeRef> prefix;
        prefix.reserve(remaining.size());
        for (size_t i = 0; i < remaining.size(); ++i) {
          prefix.push_back(remaining[i]);
          FenceKeyBytes candidate_upper{.is_infinity_ = false, .key_ = remaining[i].max_key_};
          if (!CanFitInner(lower_fence, candidate_upper, prefix, fit_buffer)) {
            prefix.pop_back();
            break;
          }
        }
        if (prefix.empty()) {
          return Error::General("child ref too large for inner node");
        }
        FenceKeyBytes prefix_upper{.is_infinity_ = false, .key_ = prefix.back().max_key_};
        if (auto res = emit_group(prefix, lower_fence, prefix_upper); !res) {
          return std::move(res.error());
        }
        lower_fence = next_level.back().upper_fence_;
        remaining.erase(remaining.begin(), remaining.begin() + prefix.size());
      }
      if (!remaining.empty()) {
        if (auto res = emit_group(remaining, lower_fence, upper_fence); !res) {
          return std::move(res.error());
        }
      }
      return next_level;
    }
    if (auto res = emit_group(current, lower_fence, upper_fence); !res) {
      return std::move(res.error());
    }
  }

  return next_level;
}

std::vector<NodeRef> BuildLevelRefsFromGroups(const std::vector<LeafGroup>& groups) {
  std::vector<NodeRef> level_refs;
  level_refs.reserve(groups.size());
  for (const auto& group : groups) {
    const std::string max_key = group.refs_.back().max_key_;
    level_refs.push_back(
        {.swip_ = Swip(), .max_key_ = max_key, .upper_fence_ = group.upper_fence_});
  }
  return level_refs;
}

std::vector<FenceKeyBytes> CollectUpperFences(const std::vector<NodeRef>& refs) {
  std::vector<FenceKeyBytes> fences;
  fences.reserve(refs.size());
  for (const auto& ref : refs) {
    fences.push_back(ref.upper_fence_);
  }
  return fences;
}

Result<std::vector<NodeRef>> BuildInnerLevels(BasicKV* btree, std::vector<NodeRef> level_refs,
                                              const FenceKeyBytes& parent_lower,
                                              const FenceKeyBytes& parent_upper,
                                              uint64_t levels_to_build) {
  for (uint64_t level = 0; level < levels_to_build; ++level) {
    auto next_level_res = BuildInnerLevel(btree, level_refs, parent_lower, parent_upper);
    if (!next_level_res) {
      return std::move(next_level_res.error());
    }
    level_refs = std::move(next_level_res.value());
  }
  return level_refs;
}

Result<ColumnSubtreePlan> PrepareColumnSubtreePlan(BasicKV& btree,
                                                   GuardedBufferFrame<BTreeNode>& guarded_parent,
                                                   const TableDefinition& def,
                                                   const column_store::ColumnStoreOptions& options,
                                                   column_store::ColumnStoreStats& stats) {
  ColumnSubtreePlan plan;
  plan.parent_lower_ = ExtractFence(guarded_parent.ref(), true);
  plan.parent_upper_ = ExtractFence(guarded_parent.ref(), false);

  SubtreeCollect collect;
  auto subtree_height_res = CollectSubtree(btree, guarded_parent, collect, false);
  if (!subtree_height_res) {
    return std::move(subtree_height_res.error());
  }
  plan.collect_ = std::move(collect);
  plan.subtree_height_ = subtree_height_res.value();

  if (plan.collect_.any_column_leaf_ && !plan.collect_.any_row_leaf_) {
    plan.skip_ = true;
    return plan;
  }
  if (plan.collect_.any_column_leaf_ && plan.collect_.any_row_leaf_) {
    return Error::General("mixed row/column leaves not supported");
  }
  if (plan.collect_.row_leaf_frames_.empty()) {
    return Error::General("no row leaves found");
  }

  TableCodec codec(def);
  column_store::ColumnBlockBuilder builder(def, options);
  std::string last_key_bytes;
  for (auto* leaf_bf : plan.collect_.row_leaf_frames_) {
    GuardedBufferFrame<BTreeNode> guarded_leaf(btree.store_->buffer_manager_.get(), leaf_bf);
    guarded_leaf.ToExclusiveMayJump();
    if (auto res = AppendLeafRows(def, codec, builder, guarded_leaf, stats, plan.payloads_,
                                  &last_key_bytes);
        !res) {
      return std::move(res.error());
    }
  }

  if (!builder.Empty()) {
    if (last_key_bytes.empty()) {
      return Error::General("missing last key for final column block");
    }
    auto block_res = builder.Finalize(Slice(last_key_bytes));
    if (!block_res) {
      return std::move(block_res.error());
    }
    plan.payloads_.push_back(std::move(block_res.value()));
  }

  if (plan.payloads_.empty()) {
    return Error::General("no column blocks created");
  }

  std::vector<BlockRefEntry> dummy_refs;
  dummy_refs.reserve(plan.payloads_.size());
  for (const auto& payload : plan.payloads_) {
    dummy_refs.push_back({.ref_ = column_store::ColumnBlockRef{}, .max_key_ = payload.max_key_});
  }

  auto groups_res = BuildLeafGroups(dummy_refs, plan.parent_lower_, plan.parent_upper_);
  if (!groups_res) {
    return std::move(groups_res.error());
  }
  plan.groups_ = std::move(groups_res.value());

  if (plan.subtree_height_ < 2) {
    return Error::General("subtree height too small for column conversion");
  }
  plan.levels_to_build_ = plan.subtree_height_ - 2;

  auto level_refs = BuildLevelRefsFromGroups(plan.groups_);
  auto preflight_res = BuildInnerLevels(nullptr, std::move(level_refs), plan.parent_lower_,
                                        plan.parent_upper_, plan.levels_to_build_);
  if (!preflight_res) {
    return std::move(preflight_res.error());
  }
  auto child_upper_fences = CollectUpperFences(preflight_res.value());
  if (!CanFitParentSeparators(plan.parent_lower_, plan.parent_upper_, child_upper_fences)) {
    return Error::General("parent has insufficient space for separators");
  }

  return plan;
}

Result<std::vector<NodeRef>> CreateColumnLeaves(BasicKV& btree,
                                                const std::vector<LeafGroup>& groups,
                                                const FenceKeyBytes& parent_lower) {
  std::vector<NodeRef> leaf_level;
  leaf_level.reserve(groups.size());
  FenceKeyBytes lower_fence = parent_lower;
  for (const auto& group : groups) {
    auto& bf = btree.store_->buffer_manager_->AllocNewPage(btree.tree_id_);
    GuardedBufferFrame<BTreeNode> guarded_leaf(btree.store_->buffer_manager_.get(), &bf);
    guarded_leaf.ToExclusiveMayJump();
    auto* leaf = BTreeNode::New(guarded_leaf.bf_->page_.payload_, true, FenceSlice(lower_fence),
                                FenceSlice(group.upper_fence_));
    leaf->has_garbage_ = false;
    for (const auto& entry : group.refs_) {
      const uint16_t key_size = static_cast<uint16_t>(entry.max_key_.size());
      const uint16_t val_size = sizeof(column_store::ColumnBlockRef);
      if (!leaf->PrepareInsert(key_size, val_size)) {
        return Error::General("column leaf has insufficient space for block refs");
      }
      int32_t slot = leaf->InsertDoNotCopyPayload(Slice(entry.max_key_), val_size);
      std::memcpy(leaf->ValData(slot), &entry.ref_, sizeof(entry.ref_));
    }
    guarded_leaf.bf_->page_.magic_debugging_ = column_store::kColumnLeafMagic;
    guarded_leaf.UpdatePageVersion();

    const std::string max_key = group.refs_.back().max_key_;
    leaf_level.push_back(
        {.swip_ = Swip(&bf), .max_key_ = max_key, .upper_fence_ = group.upper_fence_});
    lower_fence = group.upper_fence_;
    if (lower_fence.is_infinity_) {
      break;
    }
  }
  return leaf_level;
}

Result<void> RewriteParentAndReclaim(BasicKV& btree, GuardedBufferFrame<BTreeNode>& guarded_parent,
                                     const FenceKeyBytes& parent_lower,
                                     const FenceKeyBytes& parent_upper,
                                     const std::vector<NodeRef>& children,
                                     const SubtreeCollect& collect, uint64_t parent_version) {
  guarded_parent.ToExclusiveMayJump();
  if (guarded_parent.bf_->page_.page_version_ != parent_version) {
    return Error::General("parent modified during column conversion");
  }

  auto* new_parent = BTreeNode::New(guarded_parent.bf_->page_.payload_, false,
                                    FenceSlice(parent_lower), FenceSlice(parent_upper));
  new_parent->has_garbage_ = false;
  if (children.empty()) {
    return Error::General("no new children created");
  }
  for (size_t i = 0; i + 1 < children.size(); ++i) {
    const auto& fence = children[i].upper_fence_;
    if (fence.is_infinity_) {
      return Error::General("invalid infinity separator");
    }
    Swip swip = children[i].swip_;
    const auto& key = fence.key_;
    const uint16_t key_size = static_cast<uint16_t>(key.size());
    if (!new_parent->PrepareInsert(key_size, sizeof(Swip))) {
      return Error::General("parent has insufficient space for separators");
    }
    new_parent->Insert(Slice(reinterpret_cast<const uint8_t*>(key.data()), key.size()),
                       Slice(reinterpret_cast<const uint8_t*>(&swip), sizeof(Swip)));
  }
  new_parent->right_most_child_swip_ = children.back().swip_;
  guarded_parent.UpdatePageVersion();

  for (auto* old_bf : collect.subtree_frames_) {
    GuardedBufferFrame<BTreeNode> guarded_old(btree.store_->buffer_manager_.get(), old_bf);
    guarded_old.ToExclusiveMayJump();
    guarded_old.Reclaim();
  }

  return {};
}

// Converts a parent subtree to column storage while preserving subtree height.
// It scans row-store leaves in key order to build column blocks, groups blocks into column leaves,
// then rebuilds inner levels above those leaves to match the original subtree depth before
// rewriting the parent and reclaiming the old subtree pages.
Result<void> ConvertParentToColumn(BasicKV& btree, GuardedBufferFrame<BTreeNode>&& guarded_parent,
                                   const TableDefinition& def,
                                   const column_store::ColumnStoreOptions& options,
                                   column_store::ColumnStoreStats& stats) {
  // Keep optimistic latch during build; upgrade to exclusive only for rewriting.
  const auto parent_version = guarded_parent.bf_->page_.page_version_;

  if (guarded_parent->is_leaf_) {
    return Error::General("expected inner node for column conversion");
  }

  auto plan_res = PrepareColumnSubtreePlan(btree, guarded_parent, def, options, stats);
  if (!plan_res) {
    return std::move(plan_res.error());
  }
  ColumnSubtreePlan plan = std::move(plan_res.value());
  if (plan.skip_) {
    return {};
  }

  auto refs_res = PersistBlocks(btree, plan.payloads_, stats);
  if (!refs_res) {
    return std::move(refs_res.error());
  }
  auto block_refs = std::move(refs_res.value());

  size_t ref_idx = 0;
  for (auto& group : plan.groups_) {
    for (auto& entry : group.refs_) {
      entry = block_refs[ref_idx++];
    }
  }
  if (ref_idx != block_refs.size()) {
    return Error::General("block ref count mismatch");
  }

  auto leaf_level_res = CreateColumnLeaves(btree, plan.groups_, plan.parent_lower_);
  if (!leaf_level_res) {
    return std::move(leaf_level_res.error());
  }
  auto inner_levels_res =
      BuildInnerLevels(&btree, std::move(leaf_level_res.value()), plan.parent_lower_,
                       plan.parent_upper_, plan.levels_to_build_);
  if (!inner_levels_res) {
    return std::move(inner_levels_res.error());
  }
  auto final_level = std::move(inner_levels_res.value());

  return RewriteParentAndReclaim(btree, guarded_parent, plan.parent_lower_, plan.parent_upper_,
                                 final_level, plan.collect_, parent_version);
}

Result<void> ConvertParentsAtLevel(BasicKV& btree, GuardedBufferFrame<BTreeNode>&& guarded_node,
                                   uint64_t level, uint64_t target_level,
                                   const TableDefinition& def,
                                   const column_store::ColumnStoreOptions& options,
                                   column_store::ColumnStoreStats& stats) {
  if (level == target_level) {
    return ConvertParentToColumn(btree, std::move(guarded_node), def, options, stats);
  }

  if (guarded_node->is_leaf_) {
    return Error::General("unexpected leaf before parent conversion level");
  }

  // Traverse down while keeping ancestors optimistically latched to resolve swips.
  for (uint16_t i = 0; i <= guarded_node->num_slots_; ++i) {
    GuardedBufferFrame<BTreeNode> guarded_child(btree.store_->buffer_manager_.get(), guarded_node,
                                                *guarded_node->ChildSwipIncludingRightMost(i),
                                                LatchMode::kOptimisticSpin);
    auto res = ConvertParentsAtLevel(btree, std::move(guarded_child), level + 1, target_level, def,
                                     options, stats);
    if (!res) {
      return std::move(res.error());
    }
  }

  return {};
}

Result<column_store::ColumnStoreStats> BuildColumnStoreInternal(
    BasicKV& btree, const TableDefinition& def, const column_store::ColumnStoreOptions& options) {
  // Convert parent-of-leaf nodes incrementally without changing the tree height.
  column_store::ColumnStoreStats stats;
  const uint64_t height = btree.GetHeight();
  if (height <= 1) {
    return Error::General("tree height should at least 2");
  }
  const uint64_t target_height = options.target_height_ == 0 ? height : options.target_height_;
  if (target_height < 2 || target_height > height) {
    return Error::General("invalid column store target height");
  }
  GuardedBufferFrame<BTreeNode> guarded_meta(btree.store_->buffer_manager_.get(),
                                             btree.meta_node_swip_);
  GuardedBufferFrame<BTreeNode> guarded_root(btree.store_->buffer_manager_.get(), guarded_meta,
                                             guarded_meta->right_most_child_swip_);

  const uint64_t target_level = height - target_height;
  auto res =
      ConvertParentsAtLevel(btree, std::move(guarded_root), 0, target_level, def, options, stats);
  if (!res) {
    return std::move(res.error());
  }
  // TODO: rebalance the tree after convert to column store.
  return stats;
}

} // namespace

Result<column_store::ColumnStoreStats> Table::BuildColumnStore(
    const column_store::ColumnStoreOptions& options) {
  if (definition_.primary_index_config_.enable_wal_) {
    return Error::General("column store build requires WAL disabled");
  }
  auto* btree = std::get_if<std::reference_wrapper<BasicKV>>(&kv_interface_.AsVariant());
  if (btree == nullptr) {
    return Error::General("column store build requires BasicKV");
  }
  return BuildColumnStoreInternal(btree->get(), definition_, options);
}

} // namespace leanstore
