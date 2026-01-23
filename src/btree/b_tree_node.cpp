#include "leanstore/btree/b_tree_node.hpp"

#include "leanstore/base/defer.hpp"
#include "leanstore/base/log.hpp"
#include "leanstore/base/slice.hpp"
#include "leanstore/base/small_vector.hpp"
#include "leanstore/buffer/guarded_buffer_frame.hpp"
#include "leanstore/coro/coro_env.hpp"
#include "leanstore/exceptions.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace leanstore {

void BTreeNode::UpdateHint(uint16_t slot_id) {
  uint16_t dist = num_slots_ / (kHintCount + 1);
  uint16_t begin = 0;
  if ((num_slots_ > kHintCount * 2 + 1) && (((num_slots_ - 1) / (kHintCount + 1)) == dist) &&
      ((slot_id / dist) > 1)) {
    begin = (slot_id / dist) - 1;
  }
  for (uint16_t i = begin; i < kHintCount; i++) {
    hint_[i] = slot_[dist * (i + 1)].head_;
  }
  for (uint16_t i = 0; i < kHintCount; i++) {
    assert(hint_[i] == slot_[dist * (i + 1)].head_);
  }
}

void BTreeNode::SearchHint(HeadType key_head, uint16_t& lower_out, uint16_t& upper_out) {
  if (num_slots_ > kHintCount * 2) {
    if (CoroEnv::CurStore().store_option_->btree_hints_ == 2) {
#ifdef __AVX512F__
      const uint16_t dist = num_slots_ / (kHintCount + 1);
      uint16_t pos, pos2;
      __m512i key_head_reg = _mm512_set1_epi32(key_head);
      __m512i chunk = _mm512_loadu_si512(hint);
      __mmask16 compare_mask = _mm512_cmpge_epu32_mask(chunk, key_head_reg);
      if (compare_mask == 0)
        return;
      pos = __builtin_ctz(compare_mask);
      lower_out = pos * dist;
      // -------------------------------------------------------------------------------------
      for (pos2 = pos; pos2 < kHintCount; pos2++) {
        if (hint_[pos2] != key_head) {
          break;
        }
      }
      if (pos2 < kHintCount) {
        upper_out = (pos2 + 1) * dist;
      }
#else
      Log::Error("Search hint with AVX512 failed: __AVX512F__ not found");
#endif
    } else if (CoroEnv::CurStore().store_option_->btree_hints_ == 1) {
      const uint16_t dist = num_slots_ / (kHintCount + 1);
      uint16_t pos, pos2;

      for (pos = 0; pos < kHintCount; pos++) {
        if (hint_[pos] >= key_head) {
          break;
        }
      }
      for (pos2 = pos; pos2 < kHintCount; pos2++) {
        if (hint_[pos2] != key_head) {
          break;
        }
      }

      lower_out = pos * dist;
      if (pos2 < kHintCount) {
        upper_out = (pos2 + 1) * dist;
      }
    } else {
    }
  }
}

int16_t BTreeNode::InsertDoNotCopyPayload(Slice key, uint16_t val_size, int32_t pos) {
  LEAN_DCHECK(CanInsert(key.size(), val_size));
  PrepareInsert(key.size(), val_size);

  // calculate taret slotId for insertion
  int32_t slot_id = (pos == -1) ? LowerBound<false>(key) : pos;

  // 1. move slot_[slotId..num_slots_] to slot_[slotId+1..num_slots_+1]
  memmove(slot_ + slot_id + 1, slot_ + slot_id, sizeof(BTreeNodeSlot) * (num_slots_ - slot_id));

  // remove common key prefix
  key.remove_prefix(prefix_size_);

  //
  slot_[slot_id].head_ = Head(key);
  slot_[slot_id].key_size_without_prefix_ = key.size();
  slot_[slot_id].val_size_ = val_size;
  auto total_key_val_size = key.size() + val_size;
  AdvanceDataOffset(total_key_val_size);
  slot_[slot_id].offset_ = data_offset_;
  memcpy(KeyDataWithoutPrefix(slot_id), key.data(), key.size());

  num_slots_++;
  UpdateHint(slot_id);
  return slot_id;
}

int32_t BTreeNode::Insert(Slice key, Slice val) {
  LEAN_DEXEC() {
    assert(CanInsert(key.size(), val.size()));
    int32_t exact_pos = LowerBound<true>(key);
    static_cast<void>(exact_pos);
    assert(exact_pos == -1); // assert for duplicates
  }

  PrepareInsert(key.size(), val.size());
  int32_t slot_id = LowerBound<false>(key);
  memmove(slot_ + slot_id + 1, slot_ + slot_id, sizeof(BTreeNodeSlot) * (num_slots_ - slot_id));
  StoreKeyValue(slot_id, key, val);
  num_slots_++;
  UpdateHint(slot_id);
  return slot_id;

  LEAN_DEXEC() {
    int32_t exact_pos = LowerBound<true>(key);
    static_cast<void>(exact_pos);
    // assert for duplicates
    assert(exact_pos == slot_id);
  }
}

void BTreeNode::Compact() {
  uint16_t space_after_compaction [[maybe_unused]] = 0;
  LEAN_DEXEC() {
    space_after_compaction = FreeSpaceAfterCompaction();
  }
  LEAN_DEFER(LEAN_DEXEC() { LEAN_DCHECK(space_after_compaction == FreeSpace()); });

  // generate a temp node to store the compacted data
  auto tmp_node_buf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
  auto* tmp = BTreeNode::New(tmp_node_buf->get(), is_leaf_, GetLowerFence(), GetUpperFence());

  // copy the keys and values
  CopyKeyValueRange(tmp, 0, 0, num_slots_);

  // copy the right most child
  tmp->right_most_child_swip_ = right_most_child_swip_;

  // copy back
  memcpy(reinterpret_cast<char*>(this), tmp, BTreeNode::Size());
  MakeHint();
}

uint32_t BTreeNode::MergeSpaceUpperBound(ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_right) {
  LEAN_DCHECK(x_guarded_right->is_leaf_);

  auto tmp_node_buf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
  auto* tmp =
      BTreeNode::New(tmp_node_buf->get(), true, GetLowerFence(), x_guarded_right->GetUpperFence());

  uint32_t left_grow = (prefix_size_ - tmp->prefix_size_) * num_slots_;
  uint32_t right_grow =
      (x_guarded_right->prefix_size_ - tmp->prefix_size_) * x_guarded_right->num_slots_;
  uint32_t space_upper_bound =
      space_used_ + x_guarded_right->space_used_ +
      (reinterpret_cast<uint8_t*>(slot_ + num_slots_ + x_guarded_right->num_slots_) - NodeBegin()) +
      left_grow + right_grow;
  return space_upper_bound;
}

// right survives, this gets reclaimed left(this) into right
bool BTreeNode::merge(uint16_t slot_id, ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_parent,
                      ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_right) {
  if (is_leaf_) {
    assert(x_guarded_right->is_leaf_);
    assert(x_guarded_parent->IsInner());

    auto tmp_node_buf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
    auto* tmp = BTreeNode::New(tmp_node_buf->get(), true, GetLowerFence(),
                               x_guarded_right->GetUpperFence());
    uint16_t left_grow = (prefix_size_ - tmp->prefix_size_) * num_slots_;
    uint16_t right_grow =
        (x_guarded_right->prefix_size_ - tmp->prefix_size_) * x_guarded_right->num_slots_;
    uint16_t space_upper_bound =
        space_used_ + x_guarded_right->space_used_ +
        (reinterpret_cast<uint8_t*>(slot_ + num_slots_ + x_guarded_right->num_slots_) -
         NodeBegin()) +
        left_grow + right_grow;
    if (space_upper_bound > BTreeNode::Size()) {
      return false;
    }
    CopyKeyValueRange(tmp, 0, 0, num_slots_);
    x_guarded_right->CopyKeyValueRange(tmp, num_slots_, 0, x_guarded_right->num_slots_);
    x_guarded_parent->RemoveSlot(slot_id);

    x_guarded_right->has_garbage_ |= has_garbage_;

    memcpy(x_guarded_right.GetPagePayloadPtr(), tmp, BTreeNode::Size());
    x_guarded_right->MakeHint();
    return true;
  }

  // Inner node
  LEAN_DCHECK(!x_guarded_right->is_leaf_);
  LEAN_DCHECK(x_guarded_parent->IsInner());

  auto tmp_node_buf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
  auto* tmp = BTreeNode::New(tmp_node_buf->get(), is_leaf_, GetLowerFence(),
                             x_guarded_right->GetUpperFence());
  uint16_t left_grow = (prefix_size_ - tmp->prefix_size_) * num_slots_;
  uint16_t right_grow =
      (x_guarded_right->prefix_size_ - tmp->prefix_size_) * x_guarded_right->num_slots_;
  uint16_t extra_key_length = x_guarded_parent->GetFullKeyLen(slot_id);
  uint16_t space_upper_bound =
      space_used_ + x_guarded_right->space_used_ +
      (reinterpret_cast<uint8_t*>(slot_ + num_slots_ + x_guarded_right->num_slots_) - NodeBegin()) +
      left_grow + right_grow + SpaceNeeded(extra_key_length, sizeof(Swip), tmp->prefix_size_);
  if (space_upper_bound > BTreeNode::Size()) {
    return false;
  }
  CopyKeyValueRange(tmp, 0, 0, num_slots_);
  // Allocate in the stack, freed when the calling function exits.
  auto extra_key = utils::JumpScopedArray<uint8_t>(extra_key_length);
  x_guarded_parent->CopyFullKey(slot_id, extra_key->get());
  tmp->StoreKeyValue(num_slots_, Slice(extra_key->get(), extra_key_length),
                     Slice(reinterpret_cast<uint8_t*>(&right_most_child_swip_), sizeof(Swip)));
  tmp->num_slots_++;
  x_guarded_right->CopyKeyValueRange(tmp, tmp->num_slots_, 0, x_guarded_right->num_slots_);
  x_guarded_parent->RemoveSlot(slot_id);
  tmp->right_most_child_swip_ = x_guarded_right->right_most_child_swip_;
  tmp->MakeHint();
  memcpy(x_guarded_right.GetPagePayloadPtr(), tmp, BTreeNode::Size());
  return true;
}

void BTreeNode::StoreKeyValue(uint16_t slot_id, Slice key, Slice val) {
  // Head
  key.remove_prefix(prefix_size_);
  slot_[slot_id].head_ = Head(key);
  slot_[slot_id].key_size_without_prefix_ = key.size();
  slot_[slot_id].val_size_ = val.size();

  // Value
  AdvanceDataOffset(key.size() + val.size());
  slot_[slot_id].offset_ = data_offset_;
  memcpy(KeyDataWithoutPrefix(slot_id), key.data(), key.size());
  memcpy(ValData(slot_id), val.data(), val.size());
}

void BTreeNode::CopyKeyValueRange(BTreeNode* dst, uint16_t dst_slot, uint16_t src_slot,
                                  uint16_t count) {
  if (prefix_size_ == dst->prefix_size_) {
    // copy slot array
    memcpy(dst->slot_ + dst_slot, slot_ + src_slot, sizeof(BTreeNodeSlot) * count);

    for (auto i = 0U; i < count; i++) {
      // consolidate the offset of each slot
      uint32_t kv_size = KeySizeWithoutPrefix(src_slot + i) + ValSize(src_slot + i);
      dst->AdvanceDataOffset(kv_size);
      dst->slot_[dst_slot + i].offset_ = dst->data_offset_;

      // copy the key value pair
      memcpy(dst->NodeBegin() + dst->data_offset_, NodeBegin() + slot_[src_slot + i].offset_,
             kv_size);
    }
  } else {
    for (uint16_t i = 0; i < count; i++) {
      CopyKeyValue(src_slot + i, dst, dst_slot + i);
    }
  }
  dst->num_slots_ += count;
}

void BTreeNode::CopyKeyValue(uint16_t src_slot, BTreeNode* dst, uint16_t dst_slot) {
  uint16_t full_length = GetFullKeyLen(src_slot);
  auto key_buf = utils::JumpScopedArray<uint8_t>(full_length);
  auto* key = key_buf->get();
  CopyFullKey(src_slot, key);
  dst->StoreKeyValue(dst_slot, Slice(key, full_length), Value(src_slot));
}

void BTreeNode::InsertFence(BTreeNodeHeader::FenceKey& fk, Slice key) {
  if (!key.data()) {
    return;
  }
  assert(FreeSpace() >= key.size());

  AdvanceDataOffset(key.size());
  fk.offset_ = data_offset_;
  fk.size_ = key.size();
  memcpy(NodeBegin() + data_offset_, key.data(), key.size());
}

uint16_t BTreeNode::CommonPrefix(uint16_t slot_a, uint16_t slot_b) {
  if (num_slots_ == 0) {
    // Do not prefix compress if only one tuple is in to
    // avoid corner cases (e.g., SI Version)
    return 0;
  }

  // TODO: the following two checks work only in single threaded
  //   assert(aPos < num_slots_);
  //   assert(bPos < num_slots_);
  uint32_t limit =
      std::min(slot_[slot_a].key_size_without_prefix_, slot_[slot_b].key_size_without_prefix_);
  uint8_t *a = KeyDataWithoutPrefix(slot_a), *b = KeyDataWithoutPrefix(slot_b);
  uint32_t i;
  for (i = 0; i < limit; i++) {
    if (a[i] != b[i]) {
      break;
    }
  }
  return i;
}

BTreeNode::SeparatorInfo BTreeNode::FindSep() {
  LEAN_DCHECK(num_slots_ > 1);

  // Inner nodes are split in the middle
  if (IsInner()) {
    uint16_t slot_id = num_slots_ / 2;
    return SeparatorInfo{GetFullKeyLen(slot_id), slot_id, false};
  }

  // Find good separator slot
  uint16_t best_prefix_length, best_slot;
  if (num_slots_ > 16) {
    uint16_t lower = (num_slots_ / 2) - (num_slots_ / 16);
    uint16_t upper = (num_slots_ / 2);

    best_prefix_length = CommonPrefix(lower, 0);
    best_slot = lower;

    if (best_prefix_length != CommonPrefix(upper - 1, 0)) {
      for (best_slot = lower + 1;
           (best_slot < upper) && (CommonPrefix(best_slot, 0) == best_prefix_length); best_slot++) {
      }
    }
  } else {
    best_slot = (num_slots_ - 1) / 2;
    // bestPrefixLength = CommonPrefix(bestSlot, 0);
  }

  // Try to truncate separator
  uint16_t common = CommonPrefix(best_slot, best_slot + 1);
  if ((best_slot + 1 < num_slots_) && (slot_[best_slot].key_size_without_prefix_ > common) &&
      (slot_[best_slot + 1].key_size_without_prefix_ > (common + 1))) {
    return SeparatorInfo{static_cast<uint16_t>(prefix_size_ + common + 1), best_slot, true};
  }

  return SeparatorInfo{GetFullKeyLen(best_slot), best_slot, false};
}

int32_t BTreeNode::CompareKeyWithBoundaries(Slice key) {
  // Lower Bound exclusive, upper bound inclusive
  if (lower_fence_.offset_) {
    int cmp = CmpKeys(key, GetLowerFence());
    if (!(cmp > 0)) {
      return 1; // Key lower or equal LF
    }
  }

  if (upper_fence_.offset_) {
    int cmp = CmpKeys(key, GetUpperFence());
    if (!(cmp <= 0)) {
      return -1; // Key higher than UF
    }
  }
  return 0;
}

Swip& BTreeNode::LookupInner(Slice key) {
  int32_t slot_id = LowerBound<false>(key);
  if (slot_id == num_slots_) {
    LEAN_DCHECK(!right_most_child_swip_.IsEmpty());
    return right_most_child_swip_;
  }
  auto* child_swip = ChildSwip(slot_id);
  LEAN_DCHECK(!child_swip->IsEmpty(), "childSwip is empty, slotId={}", slot_id);
  return *child_swip;
}

/// xGuardedParent           xGuardedParent
///      |                      |       |
///     this           xGuardedNewLeft this
///
void BTreeNode::Split(ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_parent,
                      ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_new_left,
                      const BTreeNode::SeparatorInfo& sep_info) {
  LEAN_DCHECK(x_guarded_parent->CanInsert(sep_info.size_, sizeof(Swip)));

  // generate separator key
  SmallBuffer256 sep_key(sep_info.size_);
  BuildSeparator(sep_info, sep_key.Data());
  Slice seperator{sep_key.Data(), sep_info.size_};

  x_guarded_new_left->SetFences(GetLowerFence(), seperator);

  SmallBuffer256 tmp_right_buf(BTreeNode::Size());
  auto* tmp_right = BTreeNode::New(tmp_right_buf.Data(), is_leaf_, seperator, GetUpperFence());

  // insert (seperator, xGuardedNewLeft) into xGuardedParent
  auto swip = x_guarded_new_left.swip();
  x_guarded_parent->Insert(seperator, Slice(reinterpret_cast<uint8_t*>(&swip), sizeof(Swip)));

  if (is_leaf_) {
    // move slot 0..sepInfo.slot_id_ to xGuardedNewLeft
    CopyKeyValueRange(x_guarded_new_left.GetPagePayload(), 0, 0, sep_info.slot_id_ + 1);

    // move slot sepInfo.slot_id_+1..num_slots_ to tmpRight
    CopyKeyValueRange(tmp_right, 0, x_guarded_new_left->num_slots_,
                      num_slots_ - x_guarded_new_left->num_slots_);
    tmp_right->has_garbage_ = has_garbage_;
    x_guarded_new_left->has_garbage_ = has_garbage_;
  } else {
    CopyKeyValueRange(x_guarded_new_left.GetPagePayload(), 0, 0, sep_info.slot_id_);
    CopyKeyValueRange(tmp_right, 0, x_guarded_new_left->num_slots_ + 1,
                      num_slots_ - x_guarded_new_left->num_slots_ - 1);
    x_guarded_new_left->right_most_child_swip_ = *ChildSwip(x_guarded_new_left->num_slots_);
    tmp_right->right_most_child_swip_ = right_most_child_swip_;
  }
  x_guarded_new_left->MakeHint();
  tmp_right->MakeHint();
  memcpy(reinterpret_cast<char*>(this), tmp_right, BTreeNode::Size());
}

bool BTreeNode::RemoveSlot(uint16_t slot_id) {
  space_used_ -= KeySizeWithoutPrefix(slot_id) + ValSize(slot_id);
  memmove(slot_ + slot_id, slot_ + slot_id + 1, sizeof(BTreeNodeSlot) * (num_slots_ - slot_id - 1));
  num_slots_--;
  MakeHint();
  return true;
}

bool BTreeNode::Remove(Slice key) {
  int slot_id = LowerBound<true>(key);
  if (slot_id == -1) {
    // key not found
    return false;
  }
  return RemoveSlot(slot_id);
}

void BTreeNode::Reset() {
  space_used_ = upper_fence_.size_ + lower_fence_.size_;
  data_offset_ = BTreeNode::Size() - space_used_;
  num_slots_ = 0;
}

int32_t BTreeNode::CmpKeys(Slice lhs, Slice rhs) {
  auto min_length = std::min(lhs.size(), rhs.size());
  if (min_length < 4) {
    for (size_t i = 0; i < min_length; ++i) {
      if (lhs[i] != rhs[i]) {
        return lhs[i] < rhs[i] ? -1 : 1;
      }
    }
    return (lhs.size() - rhs.size());
  }

  int c = memcmp(lhs.data(), rhs.data(), min_length);
  if (c != 0) {
    return c;
  }
  return (lhs.size() - rhs.size());
}

HeadType BTreeNode::Head(Slice key) {
  switch (key.size()) {
  case 0: {
    return 0;
  }
  case 1: {
    return static_cast<uint32_t>(key[0]) << 24;
  }
  case 2: {
    const uint16_t big_endian_val = *reinterpret_cast<const uint16_t*>(key.data());
    const uint16_t little_endian_val = __builtin_bswap16(big_endian_val);
    return static_cast<uint32_t>(little_endian_val) << 16;
  }
  case 3: {
    const uint16_t big_endian_val = *reinterpret_cast<const uint16_t*>(key.data());
    const uint16_t little_endian_val = __builtin_bswap16(big_endian_val);
    return (static_cast<uint32_t>(little_endian_val) << 16) | (static_cast<uint32_t>(key[2]) << 8);
  }
  default: {
    return __builtin_bswap32(*reinterpret_cast<const uint32_t*>(key.data()));
  }
  }

  return __builtin_bswap32(*reinterpret_cast<const uint32_t*>(key.data()));
}

} // namespace leanstore
