#pragma once

#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/buffer-manager/guarded_buffer_frame.hpp"
#include "leanstore/common/portable.h"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/managed_thread.hpp"
#include "utils/small_vector.hpp"

#include <cstdint>
#include <cstring>

namespace leanstore::storage::btree {

class BTreeNode;
using HeadType = uint32_t;

class BTreeNodeHeader {
public:
  static constexpr uint16_t kHintCount = 16;

  struct SeparatorInfo {
    /// The full length of the separator key.
    uint16_t size_;

    /// The slot id of the separator key.
    uint16_t slot_id_;

    /// Indicates whether the separator key is truncated.
    bool trunc_;

    SeparatorInfo(uint16_t size = 0, uint16_t slot_id = 0, bool trunc = false)
        : size_(size),
          slot_id_(slot_id),
          trunc_(trunc) {
    }
  };

  /// The fence key information of a BTreeNode.
  struct FenceKey {
    /// The offset of the fence key in the BTreeNode.
    uint16_t offset_;

    /// The length of the fence key.
    uint16_t size_;

    /// Whether the fence key represents infinity.
    bool IsInfinity() {
      return offset_ == 0;
    }
  };

  /// The swip of the right-most child, can be nullptr for leaf nodes.
  Swip right_most_child_swip_ = nullptr;

  /// The lower fence of the node. Exclusive.
  FenceKey lower_fence_ = {0, 0};

  /// The upper fence of the node. Inclusive.
  FenceKey upper_fence_ = {0, 0};

  /// Size of the slot array.
  uint16_t num_slots_ = 0;

  /// Indicates whether this node is leaf node without any child.
  bool is_leaf_;

  /// Indicates the space used for the node.
  /// @note !!! does not include the header, but includes fences !!!
  uint16_t space_used_ = 0;

  /// Data offset of the current slot in the BTreeNode. The BTreeNode is organized as follows:
  ///
  ///   | BTreeNodeHeader | info of slot 0..N |  ... | data of slot N..0 |
  ///
  /// It's initialized to the total size of the btree node, reduced and assigned to each slot when
  /// the number of slots is increasing.
  uint16_t data_offset_;

  uint16_t prefix_size_ = 0;

  uint32_t hint_[kHintCount];

  /// Needed for GC
  bool has_garbage_ = false;

  /// Constructs a BTreeNodeHeader.
  BTreeNodeHeader(bool is_leaf, uint16_t size) : is_leaf_(is_leaf), data_offset_(size) {
  }

  /// Destructs a BTreeNodeHeader.
  ~BTreeNodeHeader() = default;

  /// Returns the start address of the node.
  uint8_t* NodeBegin() {
    return reinterpret_cast<uint8_t*>(this);
  }

  /// Whether the node is an inner node.
  bool IsInner() {
    return !is_leaf_;
  }

  /// Get the lower fence key slice.
  Slice GetLowerFence() {
    return Slice(LowerFenceAddr(), lower_fence_.size_);
  }

  /// Get the address of lower fence key. nullptr if the lower fence is infinity.
  uint8_t* LowerFenceAddr() {
    return lower_fence_.IsInfinity() ? nullptr : NodeBegin() + lower_fence_.offset_;
  }

  /// Get the upper fence key slice.
  Slice GetUpperFence() {
    return Slice(UpperFenceAddr(), upper_fence_.size_);
  }

  /// Get the address of upper fence key. nullptr if the upper fence is infinity.
  uint8_t* UpperFenceAddr() {
    return upper_fence_.IsInfinity() ? nullptr : NodeBegin() + upper_fence_.offset_;
  }
};

/// The slot inside a btree node. Slot records the metadata for the key-value position inside a
/// page. Common prefix among all keys are removed in a btree node. Slot key-value layout:
///  | key without prefix | value |
struct PACKED BTreeNodeSlot {
  /// Data offset of the slot, also the offset of the slot key
  uint16_t offset_;

  /// Slot key size
  uint16_t key_size_without_prefix_;

  /// Slot value size
  uint16_t val_size_;

  /// The key header, used to improve key comparation performance
  union {
    HeadType head_;

    uint8_t head_bytes_[4];
  };
};

class BTreeNode : public BTreeNodeHeader {
public:
  static BTreeNode* From(BufferFrame* bf) {
    return reinterpret_cast<BTreeNode*>(bf->page_.payload_);
  }

  /// The slot array, which stores all the key-value positions inside a BTreeNode.
  BTreeNodeSlot slot_[];

  /// Creates a BTreeNode. Since BTreeNode creations and utilizations are critical, please use
  /// ExclusiveGuardedBufferFrame::InitPayload() or BTreeNode::New() to construct a BTreeNode on an
  /// existing buffer which has at least BTreeNode::Size() bytes:
  /// 1. ExclusiveGuardedBufferFrame::InitPayload() creates a BTreeNode on the holding BufferFrame.
  /// 2. BTreeNode::New(): creates a BTreeNode on the providing buffer. The size of the underlying
  ///    buffer to store a BTreeNode can be obtained through BTreeNode::Size()
  BTreeNode(bool is_leaf) : BTreeNodeHeader(is_leaf, BTreeNode::Size()) {
  }

  /// Creates a BTreeNode on the providing buffer. Callers should ensure the buffer has at least
  /// BTreeNode::Size() bytes to store the BTreeNode.
  /// @param buf: the buffer to store the BTreeNode.
  /// @param isLeaf: whether the BTreeNode is a leaf node.
  /// @param lowerFence: the lower fence of the BTreeNode.
  /// @param upperFence: the upper fence of the BTreeNode.
  /// @return the created BTreeNode.
  static BTreeNode* New(void* buf, bool is_leaf, Slice lower_fence, Slice upper_fence) {
    auto* node = new (buf) BTreeNode(is_leaf);
    node->SetFences(lower_fence, upper_fence);
    return node;
  }

  uint16_t FreeSpace() {
    auto slot_array_end_offset = reinterpret_cast<uint8_t*>(&slot_[num_slots_]) - NodeBegin();
    return data_offset_ - slot_array_end_offset;
  }

  uint16_t FreeSpaceAfterCompaction() {
    auto slot_array_end_offset = reinterpret_cast<uint8_t*>(&slot_[num_slots_]) - NodeBegin();
    return BTreeNode::Size() - slot_array_end_offset - space_used_;
  }

  double FillFactorAfterCompaction() {
    return (1 - (FreeSpaceAfterCompaction() * 1.0 / BTreeNode::Size()));
  }

  bool HasEnoughSpaceFor(uint32_t space_needed) {
    return (space_needed <= FreeSpace() || space_needed <= FreeSpaceAfterCompaction());
  }

  // ATTENTION: this method has side effects !
  bool RequestSpaceFor(uint16_t space_needed) {
    if (space_needed <= FreeSpace())
      return true;
    if (space_needed <= FreeSpaceAfterCompaction()) {
      Compact();
      return true;
    }
    return false;
  }

  Slice KeyWithoutPrefix(uint16_t slot_id) {
    return Slice(KeyDataWithoutPrefix(slot_id), KeySizeWithoutPrefix(slot_id));
  }

  uint8_t* KeyDataWithoutPrefix(uint16_t slot_id) {
    return NodeBegin() + slot_[slot_id].offset_;
  }

  uint16_t KeySizeWithoutPrefix(uint16_t slot_id) {
    return slot_[slot_id].key_size_without_prefix_;
  }

  Slice Value(uint16_t slot_id) {
    return Slice(ValData(slot_id), ValSize(slot_id));
  }

  // Each slot is composed of:
  // key (key_size_without_prefix_), payload (val_size_)
  uint8_t* ValData(uint16_t slot_id) {
    auto val_offset = slot_[slot_id].offset_ + slot_[slot_id].key_size_without_prefix_;
    return NodeBegin() + val_offset;
  }

  uint16_t ValSize(uint16_t slot_id) {
    return slot_[slot_id].val_size_;
  }

  Swip* ChildSwipIncludingRightMost(uint16_t slot_id) {
    if (slot_id == num_slots_) {
      return &right_most_child_swip_;
    }

    return reinterpret_cast<Swip*>(ValData(slot_id));
  }

  Swip* ChildSwip(uint16_t slot_id) {
    LEAN_DCHECK(slot_id < num_slots_);
    return reinterpret_cast<Swip*>(ValData(slot_id));
  }

  uint16_t GetKVConsumedSpace(uint16_t slot_id) {
    return sizeof(BTreeNodeSlot) + KeySizeWithoutPrefix(slot_id) + ValSize(slot_id);
  }

  // Attention: the caller has to hold a copy of the existing payload
  void ShortenPayload(uint16_t slot_id, uint16_t target_size) {
    LEAN_DCHECK(target_size <= slot_[slot_id].val_size_);
    const uint16_t space_released = slot_[slot_id].val_size_ - target_size;
    space_used_ -= space_released;
    slot_[slot_id].val_size_ = target_size;
  }

  bool CanExtendPayload(uint16_t slot_id, uint16_t target_size) {
    LEAN_DCHECK(target_size > ValSize(slot_id),
                "Target size must be larger than current size, "
                "targetSize={}, currentSize={}",
                target_size, ValSize(slot_id));

    const uint16_t extra_space_needed = target_size - ValSize(slot_id);
    return FreeSpaceAfterCompaction() >= extra_space_needed;
  }

  /// Move key-value pair to a new location
  void ExtendPayload(uint16_t slot_id, uint16_t target_size) {
    LEAN_DCHECK(CanExtendPayload(slot_id, target_size),
                "ExtendPayload failed, not enough space in the current node, "
                "slotId={}, targetSize={}, FreeSpace={}, currentSize={}",
                slot_id, target_size, FreeSpaceAfterCompaction(), ValSize(slot_id));
    auto key_size_without_prefix = KeySizeWithoutPrefix(slot_id);
    const uint16_t old_total_size = key_size_without_prefix + ValSize(slot_id);
    const uint16_t new_total_size = key_size_without_prefix + target_size;

    // store the keyWithoutPrefix temporarily before moving the payload
    SmallBuffer256 sb(key_size_without_prefix);
    uint8_t* copied_key = sb.Data();
    std::memcpy(copied_key, KeyDataWithoutPrefix(slot_id), key_size_without_prefix);

    // release the old space occupied by the payload (keyWithoutPrefix + value)
    space_used_ -= old_total_size;

    slot_[slot_id].val_size_ = 0;
    slot_[slot_id].key_size_without_prefix_ = 0;
    if (FreeSpace() < new_total_size) {
      Compact();
    }

    LEAN_DCHECK(FreeSpace() >= new_total_size);
    AdvanceDataOffset(new_total_size);
    slot_[slot_id].offset_ = data_offset_;
    slot_[slot_id].key_size_without_prefix_ = key_size_without_prefix;
    slot_[slot_id].val_size_ = target_size;
    std::memcpy(KeyDataWithoutPrefix(slot_id), copied_key, key_size_without_prefix);
  }

  Slice KeyPrefix() {
    return Slice(LowerFenceAddr(), prefix_size_);
  }

  uint8_t* GetPrefix() {
    return LowerFenceAddr();
  }

  void CopyPrefix(uint8_t* out) {
    memcpy(out, LowerFenceAddr(), prefix_size_);
  }

  void CopyKeyWithoutPrefix(uint16_t slot_id, uint8_t* dest) {
    auto key = KeyWithoutPrefix(slot_id);
    memcpy(dest, key.data(), key.size());
  }

  uint16_t GetFullKeyLen(uint16_t slot_id) {
    return prefix_size_ + KeySizeWithoutPrefix(slot_id);
  }

  void CopyFullKey(uint16_t slot_id, uint8_t* dest) {
    memcpy(dest, GetPrefix(), prefix_size_);
    auto remaining = KeyWithoutPrefix(slot_id);
    memcpy(dest + prefix_size_, remaining.data(), remaining.size());
  }

  void MakeHint() {
    uint16_t dist = num_slots_ / (kHintCount + 1);
    for (uint16_t i = 0; i < kHintCount; i++)
      hint_[i] = slot_[dist * (i + 1)].head_;
  }

  int32_t CompareKeyWithBoundaries(Slice key);

  void SearchHint(HeadType key_head, uint16_t& lower_out, uint16_t& upper_out);

  template <bool equality_only = false>
  int16_t LinearSearchWithBias(Slice key, uint16_t start_pos, bool higher = true);

  /// Returns the position where the key[pos] (if exists) >= key (not less than the given key):
  /// (2) (2) (1) ->
  /// (2) (2) (1) (0) ->
  /// (2) (2) (1) (0) (0) ->
  /// ...  ->
  /// (2) (2) (2)
  template <bool equality_only = false>
  int16_t LowerBound(Slice key, bool* is_equal = nullptr);

  void UpdateHint(uint16_t slot_id);

  int16_t InsertDoNotCopyPayload(Slice key, uint16_t val_size, int32_t pos = -1);

  int32_t Insert(Slice key, Slice val);

  uint16_t SpaceNeeded(uint16_t key_size, uint16_t val_size) {
    return SpaceNeeded(key_size, val_size, prefix_size_);
  }

  bool CanInsert(uint16_t key_size, uint16_t val_size) {
    return HasEnoughSpaceFor(SpaceNeeded(key_size, val_size));
  }

  bool PrepareInsert(uint16_t key_size, uint16_t val_size) {
    return RequestSpaceFor(SpaceNeeded(key_size, val_size));
  }

  void Compact();

  /// merge right node into this node
  uint32_t MergeSpaceUpperBound(ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_right);

  uint32_t SpaceUsedBySlot(uint16_t slot_id) {
    return sizeof(BTreeNodeSlot) + KeySizeWithoutPrefix(slot_id) + ValSize(slot_id);
  }

  // NOLINTNEXTLINE
  bool merge(uint16_t slot_id, ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_parent,
             ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_right);

  /// store key/value pair at slotId
  void StoreKeyValue(uint16_t slot_id, Slice key, Slice val);

  // ATTENTION: dstSlot then srcSlot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  void CopyKeyValueRange(BTreeNode* dst, uint16_t dst_slot, uint16_t src_slot, uint16_t count);

  void CopyKeyValue(uint16_t src_slot, BTreeNode* dst, uint16_t dst_slot);

  void InsertFence(FenceKey& fk, Slice key);

  void Split(ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_parent,
             ExclusiveGuardedBufferFrame<BTreeNode>& x_guarded_new_left,
             const BTreeNode::SeparatorInfo& sep_info);

  uint16_t CommonPrefix(uint16_t a_pos, uint16_t b_pos);

  SeparatorInfo FindSep();

  Swip& LookupInner(Slice key);

  // Not synchronized or todo section
  bool RemoveSlot(uint16_t slot_id);

  bool Remove(Slice key);

  void Reset();

private:
  void SetFences(Slice lower_key, Slice upper_key);

  void BuildSeparator(const SeparatorInfo& sep_info, uint8_t* sep_key) {
    // prefix
    memcpy(sep_key, LowerFenceAddr(), prefix_size_);

    if (sep_info.trunc_) {
      memcpy(sep_key + prefix_size_, KeyDataWithoutPrefix(sep_info.slot_id_ + 1),
             sep_info.size_ - prefix_size_);
    } else {
      memcpy(sep_key + prefix_size_, KeyDataWithoutPrefix(sep_info.slot_id_),
             sep_info.size_ - prefix_size_);
    }
  }

  bool ShrinkSearchRange(uint16_t& lower, uint16_t& upper, Slice key) {
    auto mid = ((upper - lower) / 2) + lower;
    auto cmp = CmpKeys(key, KeyWithoutPrefix(mid));
    if (cmp < 0) {
      upper = mid;
      return false;
    }

    if (cmp > 0) {
      lower = mid + 1;
      return false;
    }

    lower = mid;
    upper = mid;
    return true;
  }

  bool ShrinkSearchRangeWithHead(uint16_t& lower, uint16_t& upper, Slice key, HeadType key_head) {
    auto mid = ((upper - lower) / 2) + lower;
    auto mid_head = slot_[mid].head_;
    auto mid_size = slot_[mid].key_size_without_prefix_;
    if ((key_head < mid_head) || (key_head == mid_head && mid_size <= 4 && key.size() < mid_size)) {
      upper = mid;
      return false;
    }

    if ((key_head > mid_head) || (key_head == mid_head && mid_size <= 4 && key.size() > mid_size)) {
      lower = mid + 1;
      return false;
    }

    // now we must have: keyHead == midHead
    if (mid_size <= 4 && key.size() == mid_size) {
      lower = mid;
      upper = mid;
      return true;
    }

    // now we must have: keyHead == midHead && midSize > 4
    // fallback to the normal compare
    return ShrinkSearchRange(lower, upper, key);
  }

public:
  static HeadType Head(Slice key);

  static int32_t CmpKeys(Slice lhs, Slice rhs);

  static uint16_t SpaceNeeded(uint16_t key_size, uint16_t val_size, uint16_t prefix_size) {
    return sizeof(BTreeNodeSlot) + (key_size - prefix_size) + val_size;
  }

  static uint16_t Size() {
    return static_cast<uint16_t>(CoroEnv::CurStore()->store_option_->page_size_ - sizeof(Page));
  }

  static uint16_t UnderFullSize() {
    return BTreeNode::Size() * 0.6;
  }

private:
  /// Advance the data offset by size
  void AdvanceDataOffset(uint16_t size) {
    data_offset_ -= size;
    space_used_ += size;
  }
};

template <bool equality_only>
inline int16_t BTreeNode::LinearSearchWithBias(Slice key, uint16_t start_pos, bool higher) {
  if (key.size() < prefix_size_ || (bcmp(key.data(), LowerFenceAddr(), prefix_size_) != 0)) {
    return -1;
  }

  LEAN_DCHECK(key.size() >= prefix_size_ && bcmp(key.data(), LowerFenceAddr(), prefix_size_) == 0);

  // the compared key has the same prefix
  key.remove_prefix(prefix_size_);

  if (higher) {
    auto cur = start_pos + 1;
    for (; cur < num_slots_; cur++) {
      if (CmpKeys(key, KeyWithoutPrefix(cur)) == 0) {
        return cur;
      }
      break;
    }
    return equality_only ? -1 : cur;
  }

  auto cur = start_pos - 1;
  for (; cur >= 0; cur--) {
    if (CmpKeys(key, KeyWithoutPrefix(cur)) == 0) {
      return cur;
    }
    break;
  }
  return equality_only ? -1 : cur;
}

template <bool equality_only>
inline int16_t BTreeNode::LowerBound(Slice key, bool* is_equal) {
  if (is_equal != nullptr && is_leaf_) {
    *is_equal = false;
  }

  // compare prefix firstly
  if (equality_only) {
    if ((key.size() < prefix_size_) || (bcmp(key.data(), LowerFenceAddr(), prefix_size_) != 0)) {
      return -1;
    }
  } else if (prefix_size_ != 0) {
    Slice key_prefix(key.data(), std::min<uint16_t>(key.size(), prefix_size_));
    Slice lower_fence_prefix(LowerFenceAddr(), prefix_size_);
    int cmp_prefix = CmpKeys(key_prefix, lower_fence_prefix);
    if (cmp_prefix < 0) {
      return 0;
    }

    if (cmp_prefix > 0) {
      return num_slots_;
    }
  }

  // the compared key has the same prefix
  key.remove_prefix(prefix_size_);
  uint16_t lower = 0;
  uint16_t upper = num_slots_;
  HeadType key_head = Head(key);
  SearchHint(key_head, lower, upper);
  while (lower < upper) {
    bool found_equal(false);
    if (CoroEnv::CurStore()->store_option_->enable_head_optimization_) {
      found_equal = ShrinkSearchRangeWithHead(lower, upper, key, key_head);
    } else {
      found_equal = ShrinkSearchRange(lower, upper, key);
    }
    if (found_equal) {
      if (is_equal != nullptr && is_leaf_) {
        *is_equal = true;
      }
      return lower;
    }
  }

  return equality_only ? -1 : lower;
}

inline void BTreeNode::SetFences(Slice lower_key, Slice upper_key) {
  InsertFence(lower_fence_, lower_key);
  InsertFence(upper_fence_, upper_key);
  LEAN_DCHECK(LowerFenceAddr() == nullptr || UpperFenceAddr() == nullptr ||
              *LowerFenceAddr() <= *UpperFenceAddr());

  // prefix compression
  for (prefix_size_ = 0; (prefix_size_ < std::min(lower_key.size(), upper_key.size())) &&
                         (lower_key[prefix_size_] == upper_key[prefix_size_]);
       prefix_size_++)
    ;
}

} // namespace leanstore::storage::btree
