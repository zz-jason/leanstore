#pragma once

#include "b_tree_generic.hpp"
#include "coroutine/mvcc_manager.hpp"
#include "iterator.hpp"
#include "leanstore/btree/core/b_tree_node.hpp"
#include "leanstore/buffer-manager/guarded_buffer_frame.hpp"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/cpp/base/slice.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/sync/hybrid_mutex.hpp"
#include "leanstore/utils/managed_thread.hpp"

#include <functional>

#include <sys/syscall.h>

namespace leanstore {

using LeafCallback = std::function<void(GuardedBufferFrame<BTreeNode>& guarded_leaf)>;

// Iterator
class BTreeIterPessistic : public Iterator {
private:
  friend class BTreeGeneric;

public:
  /// The working btree, all the seek operations are based on this tree.
  BTreeGeneric& btree_;

  /// The latch mode on the leaf node.
  const LatchMode mode_;

  /// func_enter_leaf_ is called when the target leaf node is found.
  LeafCallback func_enter_leaf_;

  /// func_exit_leaf_ is called when leaving the target leaf node.
  LeafCallback func_exit_leaf_;

  /// func_clean_up_ is called when both parent and leaf are unlatched and before
  /// seeking for another key.
  std::function<void()> func_clean_up_;

  /// The slot id of the current key in the leaf.
  /// Reset after every leaf change.
  int32_t slot_id_;

  /// Indicates whether the prefix is copied in buffer_, reset to false after every leaf change.
  bool is_prefix_copied_;

  /// The latched leaf node of the current key.
  GuardedBufferFrame<BTreeNode> guarded_leaf_;

  /// The latched parent node of guarded_leaf_.
  GuardedBufferFrame<BTreeNode> guarded_parent_;

  /// The slot id in guarded_parent_ of guarded_leaf_.
  int32_t leaf_pos_in_parent_;

  /// Used to buffer the key at slot_id_ or lower/upper fence keys.
  std::basic_string<uint8_t> buffer_;

  /// The length of the lower or upper fence key.
  uint16_t fence_size_;

  /// Tndicates whether the fence_size_ is for lower or upper fence key.
  bool is_using_upper_fence_;

  explicit BTreeIterPessistic(BTreeGeneric& tree,
                              const LatchMode mode = LatchMode::kSharedPessimistic)
      : btree_(tree),
        mode_(mode),
        func_enter_leaf_(nullptr),
        func_exit_leaf_(nullptr),
        func_clean_up_(nullptr),
        slot_id_(-1),
        is_prefix_copied_(false),
        guarded_leaf_(),
        guarded_parent_(),
        leaf_pos_in_parent_(-1),
        buffer_(),
        fence_size_(0),
        is_using_upper_fence_(false) {
  }

  /// Seek to the position of the key which = the given key
  void SeekToEqual(Slice key) override {
    SeekTargetPageOnDemand(key);
    slot_id_ = guarded_leaf_->LowerBound<true>(key);
  }

  /// Seek to the position of the first key
  void SeekToFirst() override {
    SeekTargetPage([](GuardedBufferFrame<BTreeNode>&) { return 0; });
    if (guarded_leaf_->num_slots_ == 0) {
      SetToInvalid();
      return;
    }
    slot_id_ = 0;
  }

  /// Seek to the position of the first key which >= the given key
  void SeekToFirstGreaterEqual(Slice key) override {
    SeekTargetPageOnDemand(key);

    slot_id_ = guarded_leaf_->LowerBound<false>(key);
    if (slot_id_ < guarded_leaf_->num_slots_) {
      return;
    }

    Next();
  }

  /// Whether a next key exists in the tree
  /// @return true if the next key exists, false otherwise
  bool HasNext() override {
    // iterator is not initialized, return false
    if (!Valid()) {
      return false;
    }

    // If we are not at the end of the leaf, return true
    if (slot_id_ < guarded_leaf_->num_slots_ - 1) {
      return true;
    }

    // No more keys in the BTree, return false
    if (guarded_leaf_->upper_fence_.IsInfinity()) {
      return false;
    }

    return true;
  }

  /// Iterate to the next key in the tree
  void Next() override;

  /// Seek to the position of the last key
  void SeekToLast() override {
    SeekTargetPage([](GuardedBufferFrame<BTreeNode>& parent) { return parent->num_slots_; });
    if (guarded_leaf_->num_slots_ == 0) {
      SetToInvalid();
      return;
    }

    slot_id_ = guarded_leaf_->num_slots_ - 1;
  }

  /// Seek to the position of the last key which <= the given key
  void SeekToLastLessEqual(Slice key) override {
    SeekTargetPageOnDemand(key);

    bool is_equal = false;
    slot_id_ = guarded_leaf_->LowerBound<false>(key, &is_equal);
    if (is_equal == true) {
      return;
    }

    if (slot_id_ == 0) {
      return Prev();
    }

    slot_id_--;
    return;
  }

  /// Whether a previous key exists in the tree
  /// @return true if the previous key exists, false otherwise
  bool HasPrev() override {
    // iterator is not initialized, return false
    if (!Valid()) {
      return false;
    }

    // If we are not at the beginning of the leaf, return true
    if (slot_id_ > 0) {
      return true;
    }

    // No more keys in the BTree, return false
    if (guarded_leaf_->lower_fence_.IsInfinity()) {
      return false;
    }

    return true;
  }

  /// Iterate to the previous key in the tree
  void Prev() override;

  /// Whether the iterator is valid
  /// @return true if the iterator is pointing to a valid key-value pair, false otherwise
  bool Valid() override {
    return slot_id_ != -1;
  }

  void SetToInvalid() {
    slot_id_ = -1;
  }

  /// Get the key of the current iterator position, the key is read-only
  /// NOTE: AssembleKey() should be called before calling this function to make sure the key is
  ///       copied to the buffer.
  Slice Key() override {
    LEAN_DCHECK(buffer_.size() >= guarded_leaf_->GetFullKeyLen(slot_id_));
    return Slice(&buffer_[0], guarded_leaf_->GetFullKeyLen(slot_id_));
  }

  /// Get the value of the current iterator position, the value is read-only
  Slice Val() override {
    return guarded_leaf_->Value(slot_id_);
  }

  void SetEnterLeafCallback(LeafCallback cb) {
    func_enter_leaf_ = cb;
  }

  void SetExitLeafCallback(LeafCallback cb) {
    func_exit_leaf_ = cb;
  }

  void SetCleanUpCallback(std::function<void()> cb) {
    func_clean_up_ = cb;
  }

  /// Experimental API
  OpCode SeekExactWithHint(Slice key, bool higher = true) {
    if (!Valid()) {
      SeekToEqual(key);
      return Valid() ? OpCode::kOK : OpCode::kNotFound;
    }

    slot_id_ = guarded_leaf_->LinearSearchWithBias<true>(key, slot_id_, higher);
    if (!Valid()) {
      SeekToEqual(key);
      return Valid() ? OpCode::kOK : OpCode::kNotFound;
    }

    return OpCode::kOK;
  }

  void AssembleKey() {
    // extend the buffer if necessary
    if (auto full_key_size = guarded_leaf_->GetFullKeyLen(slot_id_);
        buffer_.size() < full_key_size) {
      buffer_.resize(full_key_size, 0);
      is_prefix_copied_ = false;
    }

    // copy the key prefix
    if (!is_prefix_copied_) {
      guarded_leaf_->CopyPrefix(&buffer_[0]);
      is_prefix_copied_ = true;
    }

    // copy the remaining key
    guarded_leaf_->CopyKeyWithoutPrefix(slot_id_, &buffer_[guarded_leaf_->prefix_size_]);
  }

  bool KeyInCurrentNode(Slice key) {
    return guarded_leaf_->CompareKeyWithBoundaries(key) == 0;
  }

  bool IsLastOne() {
    LEAN_DCHECK(slot_id_ != -1);
    LEAN_DCHECK(slot_id_ != guarded_leaf_->num_slots_);
    return (slot_id_ + 1) == guarded_leaf_->num_slots_;
  }

  void Reset() {
    guarded_leaf_.unlock();
    SetToInvalid();
    leaf_pos_in_parent_ = -1;
    is_prefix_copied_ = false;
  }

protected:
  /// Seek to the target page of the BTree on demand
  void SeekTargetPageOnDemand(Slice key) {
    if (!Valid() || !KeyInCurrentNode(key)) {
      SeekTargetPage([&key](GuardedBufferFrame<BTreeNode>& guarded_node) {
        return guarded_node->LowerBound<false>(key);
      });
    }
  }

  /// Seek to the target page of the BTree
  /// @param childPosGetter a function to get the child position in the parent node
  void SeekTargetPage(std::function<int32_t(GuardedBufferFrame<BTreeNode>&)> child_pos_getter);

  void AssembleUpperFence() {
    fence_size_ = guarded_leaf_->upper_fence_.size_ + 1;
    is_using_upper_fence_ = true;
    if (buffer_.size() < fence_size_) {
      buffer_.resize(fence_size_, 0);
    }
    std::memcpy(buffer_.data(), guarded_leaf_->UpperFenceAddr(), guarded_leaf_->upper_fence_.size_);
    buffer_[fence_size_ - 1] = 0;
  }

  Slice AssembedFence() {
    LEAN_DCHECK(buffer_.size() >= fence_size_);
    return Slice(&buffer_[0], fence_size_);
  }
};

inline void BTreeIterPessistic::Next() {
  if (!Valid()) {
    return;
  }
  while (true) {
    ENSURE(guarded_leaf_.guard_.state_ != GuardState::kSharedOptimistic);

    // If we are not at the end of the leaf, return the next key in the leaf.
    if ((slot_id_ + 1) < guarded_leaf_->num_slots_) {
      slot_id_ += 1;
      return;
    }

    // No more keys in the BTree, return false
    if (guarded_leaf_->upper_fence_.IsInfinity()) {
      SetToInvalid();
      return;
    }

    AssembleUpperFence();

    if (func_exit_leaf_ != nullptr) {
      func_exit_leaf_(guarded_leaf_);
      func_exit_leaf_ = nullptr;
    }

    guarded_leaf_.unlock();

    if (func_clean_up_ != nullptr) {
      func_clean_up_();
      func_clean_up_ = nullptr;
    }

    if (CoroEnv::CurStore().store_option_->enable_optimistic_scan_ && leaf_pos_in_parent_ != -1) {
      JUMPMU_TRY() {
        if ((leaf_pos_in_parent_ + 1) <= guarded_parent_->num_slots_) {
          int32_t next_leaf_pos = leaf_pos_in_parent_ + 1;
          auto* next_leaf_swip = guarded_parent_->ChildSwipIncludingRightMost(next_leaf_pos);
          GuardedBufferFrame<BTreeNode> guarded_next_leaf(btree_.store_->buffer_manager_.get(),
                                                          guarded_parent_, *next_leaf_swip,
                                                          LatchMode::kOptimisticOrJump);
          if (mode_ == LatchMode::kExclusivePessimistic) {
            guarded_next_leaf.TryToExclusiveMayJump();
          } else {
            guarded_next_leaf.TryToSharedMayJump();
          }
          guarded_leaf_.JumpIfModifiedByOthers();
          guarded_leaf_ = std::move(guarded_next_leaf);
          leaf_pos_in_parent_ = next_leaf_pos;
          slot_id_ = 0;
          is_prefix_copied_ = false;

          if (func_enter_leaf_ != nullptr) {
            func_enter_leaf_(guarded_leaf_);
          }

          if (guarded_leaf_->num_slots_ == 0) {
            JUMPMU_CONTINUE;
          }
          ENSURE(slot_id_ < guarded_leaf_->num_slots_);
          JUMPMU_RETURN;
        }
      }
      JUMPMU_CATCH() {
      }
    }

    guarded_parent_.unlock();
    Slice fence_key = AssembedFence();
    SeekTargetPage([&fence_key](GuardedBufferFrame<BTreeNode>& guarded_node) {
      return guarded_node->LowerBound<false>(fence_key);
    });

    if (guarded_leaf_->num_slots_ == 0) {
      SetCleanUpCallback([&, to_merge = guarded_leaf_.bf_]() {
        JUMPMU_TRY() {
          lean_txid_t sys_tx_id = btree_.store_->GetMvccManager().AllocSysTxTs();
          btree_.TryMergeMayJump(sys_tx_id, *to_merge, true);
        }
        JUMPMU_CATCH() {
        }
      });
      continue;
    }
    slot_id_ = guarded_leaf_->LowerBound<false>(AssembedFence());
    if (slot_id_ == guarded_leaf_->num_slots_) {
      continue;
    }
    return;
  }
}

inline void BTreeIterPessistic::Prev() {
  while (true) {
    ENSURE(guarded_leaf_.guard_.state_ != GuardState::kSharedOptimistic);
    // If we are not at the beginning of the leaf, return the previous key
    // in the leaf.
    if (slot_id_ > 0) {
      slot_id_ -= 1;
      return;
    }

    // No more keys in the BTree, return false
    if (guarded_leaf_->lower_fence_.IsInfinity()) {
      SetToInvalid();
      return;
    }

    // Construct the previous key (upper bound)
    fence_size_ = guarded_leaf_->lower_fence_.size_;
    is_using_upper_fence_ = false;
    if (buffer_.size() < fence_size_) {
      buffer_.resize(fence_size_, 0);
    }
    std::memcpy(&buffer_[0], guarded_leaf_->LowerFenceAddr(), fence_size_);

    // callback before exiting current leaf
    if (func_exit_leaf_ != nullptr) {
      func_exit_leaf_(guarded_leaf_);
      func_exit_leaf_ = nullptr;
    }

    guarded_parent_.unlock();
    guarded_leaf_.unlock();

    // callback after exiting current leaf
    if (func_clean_up_ != nullptr) {
      func_clean_up_();
      func_clean_up_ = nullptr;
    }

    if (CoroEnv::CurStore().store_option_->enable_optimistic_scan_ && leaf_pos_in_parent_ != -1) {
      JUMPMU_TRY() {
        if ((leaf_pos_in_parent_ - 1) >= 0) {
          int32_t next_leaf_pos = leaf_pos_in_parent_ - 1;
          auto* next_leaf_swip = guarded_parent_->ChildSwip(next_leaf_pos);
          GuardedBufferFrame<BTreeNode> guarded_next_leaf(btree_.store_->buffer_manager_.get(),
                                                          guarded_parent_, *next_leaf_swip,
                                                          LatchMode::kOptimisticOrJump);
          if (mode_ == LatchMode::kExclusivePessimistic) {
            guarded_next_leaf.TryToExclusiveMayJump();
          } else {
            guarded_next_leaf.TryToSharedMayJump();
          }
          guarded_leaf_.JumpIfModifiedByOthers();
          guarded_leaf_ = std::move(guarded_next_leaf);
          leaf_pos_in_parent_ = next_leaf_pos;
          slot_id_ = guarded_leaf_->num_slots_ - 1;
          is_prefix_copied_ = false;

          if (func_enter_leaf_ != nullptr) {
            func_enter_leaf_(guarded_leaf_);
          }

          if (guarded_leaf_->num_slots_ == 0) {
            JUMPMU_CONTINUE;
          }
          JUMPMU_RETURN;
        }
      }
      JUMPMU_CATCH() {
      }
    }

    // Construct the next key (lower bound)
    Slice fence_key = AssembedFence();
    SeekTargetPage([&fence_key](GuardedBufferFrame<BTreeNode>& guarded_node) {
      return guarded_node->LowerBound<false>(fence_key);
    });

    if (guarded_leaf_->num_slots_ == 0) {
      continue;
    }
    bool is_equal = false;
    slot_id_ = guarded_leaf_->LowerBound<false>(AssembedFence(), &is_equal);
    if (is_equal) {
      return;
    }

    if (slot_id_ > 0) {
      slot_id_ -= 1;
    } else {
      continue;
    }
  }
}

inline void BTreeIterPessistic::SeekTargetPage(
    std::function<int32_t(GuardedBufferFrame<BTreeNode>&)> child_pos_getter) {
  LEAN_DCHECK(mode_ == LatchMode::kSharedPessimistic || mode_ == LatchMode::kExclusivePessimistic,
              "Unsupported latch mode: {}", uint64_t(mode_));

  while (true) {
    leaf_pos_in_parent_ = -1;
    JUMPMU_TRY() {
      // lock meta node
      guarded_parent_ = GuardedBufferFrame<BTreeNode>(btree_.store_->buffer_manager_.get(),
                                                      btree_.meta_node_swip_);
      guarded_leaf_.unlock();

      // lock root node
      guarded_leaf_ =
          GuardedBufferFrame<BTreeNode>(btree_.store_->buffer_manager_.get(), guarded_parent_,
                                        guarded_parent_->right_most_child_swip_);

      for (uint16_t level = 0; !guarded_leaf_->is_leaf_; level++) {
        leaf_pos_in_parent_ = child_pos_getter(guarded_leaf_);
        auto* child_swip = guarded_leaf_->ChildSwipIncludingRightMost(leaf_pos_in_parent_);

        guarded_parent_ = std::move(guarded_leaf_);
        if (level == btree_.height_ - 1) {
          guarded_leaf_ = GuardedBufferFrame<BTreeNode>(btree_.store_->buffer_manager_.get(),
                                                        guarded_parent_, *child_swip, mode_);
        } else {
          // latch the middle node optimistically
          guarded_leaf_ =
              GuardedBufferFrame<BTreeNode>(btree_.store_->buffer_manager_.get(), guarded_parent_,
                                            *child_swip, LatchMode::kOptimisticSpin);
        }
      }

      guarded_parent_.unlock();
      if (mode_ == LatchMode::kExclusivePessimistic) {
        guarded_leaf_.ToExclusiveMayJump();
      } else {
        guarded_leaf_.ToSharedMayJump();
      }

      is_prefix_copied_ = false;
      if (func_enter_leaf_ != nullptr) {
        func_enter_leaf_(guarded_leaf_);
      }
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
    }
  }
}

} // namespace leanstore
