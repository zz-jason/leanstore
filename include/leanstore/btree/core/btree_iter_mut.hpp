#pragma once

#include "coroutine/mvcc_manager.hpp"
#include "leanstore/btree/core/btree_iter_pessistic.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/utils/counter_util.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/managed_thread.hpp"
#include "leanstore/utils/random_generator.hpp"

#include <memory>

namespace leanstore {

class BTreeIterMut : public BTreeIterPessistic {
public:
  BTreeIterMut(BTreeGeneric& tree) : BTreeIterPessistic(tree, LatchMode::kExclusivePessimistic) {
  }

  BTreeIterMut(BTreeGeneric& tree, BufferFrame* bf, const uint64_t bf_version)
      : BTreeIterPessistic(tree, LatchMode::kExclusivePessimistic) {
    HybridGuard optimistic_guard(bf->header_.latch_, bf_version);
    optimistic_guard.JumpIfModifiedByOthers();
    guarded_leaf_ = GuardedBufferFrame<BTreeNode>(tree.store_->buffer_manager_.get(),
                                                  std::move(optimistic_guard), bf);
    guarded_leaf_.ToExclusiveMayJump();
  }

  void IntoBtreeIter(BTreeIter* iter);

  OpCode SeekToInsertWithHint(Slice key, bool higher = true) {
    LEAN_DCHECK(Valid());
    slot_id_ = guarded_leaf_->LinearSearchWithBias(key, slot_id_, higher);
    if (slot_id_ == -1) {
      return SeekToInsert(key);
    }
    return OpCode::kOK;
  }

  OpCode SeekToInsert(Slice key) {
    SeekTargetPageOnDemand(key);

    bool is_equal = false;
    slot_id_ = guarded_leaf_->LowerBound<false>(key, &is_equal);
    if (is_equal) {
      return OpCode::kDuplicated;
    }
    return OpCode::kOK;
  }

  bool HasEnoughSpaceFor(const uint16_t key_size, const uint16_t val_size) {
    return guarded_leaf_->CanInsert(key_size, val_size);
  }

  void InsertToCurrentNode(Slice key, uint16_t val_size) {
    LEAN_DCHECK(KeyInCurrentNode(key));
    LEAN_DCHECK(HasEnoughSpaceFor(key.size(), val_size));
    slot_id_ = guarded_leaf_->InsertDoNotCopyPayload(key, val_size, slot_id_);
  }

  void InsertToCurrentNode(Slice key, Slice val) {
    LEAN_DCHECK(KeyInCurrentNode(key));
    LEAN_DCHECK(HasEnoughSpaceFor(key.size(), val.size()));
    LEAN_DCHECK(Valid());
    slot_id_ = guarded_leaf_->InsertDoNotCopyPayload(key, val.size(), slot_id_);
    std::memcpy(guarded_leaf_->ValData(slot_id_), val.data(), val.size());
  }

  void SplitForKey(Slice key) {
    auto sys_tx_id = btree_.store_->GetMvccManager()->AllocSysTxTs();
    while (true) {
      JUMPMU_TRY() {
        if (!Valid() || !KeyInCurrentNode(key)) {
          btree_.FindLeafCanJump(key, guarded_leaf_);
        }
        BufferFrame* bf = guarded_leaf_.bf_;
        guarded_leaf_.unlock();
        SetToInvalid();

        btree_.TrySplitMayJump(sys_tx_id, *bf);
        COUNTER_INC(&tls_perf_counters.split_succeed_);
        JUMPMU_BREAK;
      }
      JUMPMU_CATCH() {
        COUNTER_INC(&tls_perf_counters.split_failed_);
      }
    }
  }

  OpCode InsertKV(Slice key, Slice val) {
    while (true) {
      OpCode ret = SeekToInsert(key);
      if (ret != OpCode::kOK) {
        return ret;
      }
      LEAN_DCHECK(KeyInCurrentNode(key));
      if (!HasEnoughSpaceFor(key.size(), val.length())) {
        SplitForKey(key);
        continue;
      }

      InsertToCurrentNode(key, val);
      return OpCode::kOK;
    }
  }

  /// The caller must retain the payload when using any of the following payload resize functions
  void ShortenWithoutCompaction(const uint16_t target_size) {
    guarded_leaf_->ShortenPayload(slot_id_, target_size);
  }

  bool ExtendPayload(const uint16_t target_size) {
    if (target_size >= BTreeNode::Size()) {
      return false;
    }
    LEAN_DCHECK(slot_id_ != -1 && target_size > guarded_leaf_->ValSize(slot_id_));
    while (!guarded_leaf_->CanExtendPayload(slot_id_, target_size)) {
      if (guarded_leaf_->num_slots_ == 1) {
        return false;
      }
      AssembleKey();
      Slice key = this->Key();
      SplitForKey(key);
      SeekToEqual(key);
      LEAN_DCHECK(Valid());
    }
    LEAN_DCHECK(slot_id_ != -1);
    guarded_leaf_->ExtendPayload(slot_id_, target_size);
    return true;
  }

  MutableSlice MutableVal() {
    return MutableSlice(guarded_leaf_->ValData(slot_id_), guarded_leaf_->ValSize(slot_id_));
  }

  /// Updates contention statistics after each slot modification on the page.
  void UpdateContentionStats() {
    if (!CoroEnv::CurStore()->store_option_->enable_contention_split_) {
      return;
    }
    const uint64_t random_number = utils::RandomGenerator::RandU64();

    // haven't met the contention stats update probability
    if ((random_number &
         ((1ull << CoroEnv::CurStore()->store_option_->contention_split_sample_probability_) -
          1)) != 0) {
      return;
    }
    auto& contention_stats = guarded_leaf_.bf_->header_.contention_stats_;
    auto last_updated_slot = contention_stats.last_updated_slot_;
    contention_stats.Update(guarded_leaf_.EncounteredContention(), slot_id_);
    LEAN_DLOG("[Contention Split] ContentionStats updated, pageId={}, slot={}, "
              "encountered contention={}",
              guarded_leaf_.bf_->header_.page_id_, slot_id_, guarded_leaf_.EncounteredContention());

    // haven't met the contention split validation probability
    if ((random_number &
         ((1ull << CoroEnv::CurStore()->store_option_->contention_split_probility_) - 1)) != 0) {
      return;
    }
    auto contention_pct = contention_stats.ContentionPercentage();
    contention_stats.Reset();
    if (last_updated_slot != slot_id_ &&
        contention_pct >= CoroEnv::CurStore()->store_option_->contention_split_threshold_pct_ &&
        guarded_leaf_->num_slots_ > 2) {
      int16_t split_slot = std::min<int16_t>(last_updated_slot, slot_id_);
      guarded_leaf_.unlock();

      slot_id_ = -1;
      JUMPMU_TRY() {
        lean_txid_t sys_tx_id = btree_.store_->GetMvccManager()->AllocSysTxTs();
        btree_.TrySplitMayJump(sys_tx_id, *guarded_leaf_.bf_, split_slot);

        COUNTER_INC(&tls_perf_counters.contention_split_succeed_);
        LEAN_DLOG("[Contention Split] succeed, pageId={}, contention pct={}, split "
                  "slot={}",
                  guarded_leaf_.bf_->header_.page_id_, contention_pct, split_slot);
      }
      JUMPMU_CATCH() {
        COUNTER_INC(&tls_perf_counters.contention_split_failed_);
        Log::Info("[Contention Split] contention split failed, pageId={}, contention "
                  "pct={}, split slot={}",
                  guarded_leaf_.bf_->header_.page_id_, contention_pct, split_slot);
      }
    }
  }

  OpCode RemoveCurrent() {
    if (!(guarded_leaf_.bf_ != nullptr && slot_id_ >= 0 && slot_id_ < guarded_leaf_->num_slots_)) {
      LEAN_DCHECK(false, "RemoveCurrent failed, pageId={}, slotId={}",
                  guarded_leaf_.bf_->header_.page_id_, slot_id_);
      return OpCode::kOther;
    }
    guarded_leaf_->RemoveSlot(slot_id_);
    return OpCode::kOK;
  }

  // Returns true if it tried to merge
  bool TryMergeIfNeeded() {
    if (guarded_leaf_->FreeSpaceAfterCompaction() >= BTreeNode::UnderFullSize()) {
      guarded_leaf_.unlock();
      slot_id_ = -1;
      JUMPMU_TRY() {
        lean_txid_t sys_tx_id = btree_.store_->GetMvccManager()->AllocSysTxTs();
        btree_.TryMergeMayJump(sys_tx_id, *guarded_leaf_.bf_);
      }
      JUMPMU_CATCH() {
        LEAN_DLOG("TryMergeIfNeeded failed, pageId={}", guarded_leaf_.bf_->header_.page_id_);
      }
      return true;
    }
    return false;
  }
};

} // namespace leanstore
