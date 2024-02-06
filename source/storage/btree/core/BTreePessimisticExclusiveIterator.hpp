#pragma once

#include "BTreePessimisticIterator.hpp"
#include "KVInterface.hpp"

#include <glog/logging.h>

using namespace leanstore::storage;

namespace leanstore::storage::btree {

class BTreePessimisticExclusiveIterator : public BTreePessimisticIterator {
public:
  BTreePessimisticExclusiveIterator(BTreeGeneric& tree)
      : BTreePessimisticIterator(tree, LatchMode::kPessimisticExclusive) {
  }

  BTreePessimisticExclusiveIterator(BTreeGeneric& tree, BufferFrame* bf,
                                    const u64 bfVersion)
      : BTreePessimisticIterator(tree, LatchMode::kPessimisticExclusive) {
    HybridGuard optimisticGuard(bf->header.mLatch, bfVersion);
    optimisticGuard.JumpIfModifiedByOthers();
    mGuardedLeaf = GuardedBufferFrame<BTreeNode>(
        tree.mStore->mBufferManager.get(), std::move(optimisticGuard), bf);
    mGuardedLeaf.ToExclusiveMayJump();
  }

  void MarkAsDirty() {
    mGuardedLeaf.MarkAsDirty();
  }

  virtual OpCode SeekToInsertWithHint(Slice key, bool higher = true) {
    ENSURE(mSlotId != -1);
    mSlotId = mGuardedLeaf->linearSearchWithBias(key, mSlotId, higher);
    if (mSlotId == -1) {
      return SeekToInsert(key);
    }
    return OpCode::kOK;
  }

  virtual OpCode SeekToInsert(Slice key) {
    if (mSlotId == -1 || !KeyInCurrentNode(key)) {
      gotoPage(key);
    }
    bool isEqual = false;
    mSlotId = mGuardedLeaf->lowerBound<false>(key, &isEqual);
    if (isEqual) {
      return OpCode::kDuplicated;
    }
    return OpCode::kOK;
  }

  virtual bool HasEnoughSpaceFor(const u16 keySize, const u16 valSize) {
    return mGuardedLeaf->canInsert(keySize, valSize);
  }

  virtual void InsertToCurrentNode(Slice key, u16 valSize) {
    DCHECK(KeyInCurrentNode(key));
    DCHECK(HasEnoughSpaceFor(key.size(), valSize));
    mSlotId = mGuardedLeaf->insertDoNotCopyPayload(key, valSize, mSlotId);
  }

  virtual void InsertToCurrentNode(Slice key, Slice val) {
    DCHECK(KeyInCurrentNode(key));
    DCHECK(HasEnoughSpaceFor(key.size(), val.size()));
    DCHECK(mSlotId != -1);
    mSlotId = mGuardedLeaf->insertDoNotCopyPayload(key, val.size(), mSlotId);
    std::memcpy(mGuardedLeaf->ValData(mSlotId), val.data(), val.size());
  }

  virtual void SplitForKey(Slice key) {
    volatile u64 numAttempts(0);
    while (true) {
      JUMPMU_TRY() {
        if (mSlotId == -1 || !KeyInCurrentNode(key)) {
          mBTree.FindLeafCanJump<LatchMode::kPessimisticShared>(key,
                                                                mGuardedLeaf);
        }
        BufferFrame* bf = mGuardedLeaf.mBf;
        mGuardedLeaf.unlock();
        mSlotId = -1;

        mBTree.TrySplitMayJump(*bf);
        JUMPMU_BREAK;
      }
      JUMPMU_CATCH() {
        numAttempts = numAttempts + 1;
        LOG_IF(WARNING, (numAttempts) % 5 == 0)
            << "SplitForKey failed for " << numAttempts << " times";
      }
    }
  }

  virtual OpCode InsertKV(Slice key, Slice val) {
    while (true) {
      OpCode ret = SeekToInsert(key);
      if (ret != OpCode::kOK) {
        return ret;
      }
      DCHECK(KeyInCurrentNode(key));
      if (!HasEnoughSpaceFor(key.size(), val.length())) {
        SplitForKey(key);
        continue;
      }

      InsertToCurrentNode(key, val);
      return OpCode::kOK;
    }
  }

  // The caller must retain the payload when using any of the following payload
  // resize functions
  virtual void ShortenWithoutCompaction(const u16 targetSize) {
    mGuardedLeaf->shortenPayload(mSlotId, targetSize);
  }

  bool ExtendPayload(const u16 targetSize) {
    if (targetSize >= BTreeNode::Size()) {
      return false;
    }
    DCHECK(mSlotId != -1 && targetSize > mGuardedLeaf->ValSize(mSlotId));
    while (!mGuardedLeaf->CanExtendPayload(mSlotId, targetSize)) {
      if (mGuardedLeaf->mNumSeps == 1) {
        return false;
      }
      AssembleKey();
      Slice key = this->key();
      SplitForKey(key);
      auto succeed = SeekExact(key);
      DCHECK(succeed);
    }
    DCHECK(mSlotId != -1);
    mGuardedLeaf->ExtendPayload(mSlotId, targetSize);
    return true;
  }

  virtual MutableSlice MutableVal() {
    return MutableSlice(mGuardedLeaf->ValData(mSlotId),
                        mGuardedLeaf->ValSize(mSlotId));
  }

  /// @brief UpdateContentionStats updates the contention statistics after each
  /// slot modification on the page.
  virtual void UpdateContentionStats() {
    if (!FLAGS_contention_split) {
      return;
    }
    const u64 randomNumber = utils::RandomGenerator::RandU64();

    // haven't met the contention stats update probability
    if ((randomNumber &
         ((1ull << FLAGS_contention_split_sample_probability) - 1)) != 0) {
      return;
    }
    auto& contentionStats = mGuardedLeaf.mBf->header.mContentionStats;
    auto lastUpdatedSlot = contentionStats.mLastUpdatedSlot;
    contentionStats.Update(mGuardedLeaf.EncounteredContention(), mSlotId);
    DLOG(INFO) << "[Contention Split] ContentionStats updated"
               << ", pageId=" << mGuardedLeaf.mBf->header.mPageId
               << ", slot=" << mSlotId << ", encountered contention="
               << mGuardedLeaf.EncounteredContention();

    // haven't met the contention split validation probability
    if ((randomNumber & ((1ull << FLAGS_cm_period) - 1)) != 0) {
      return;
    }
    auto contentionPct = contentionStats.ContentionPercentage();
    contentionStats.Reset();
    if (lastUpdatedSlot != mSlotId &&
        contentionPct >= FLAGS_contention_split_threshold_pct &&
        mGuardedLeaf->mNumSeps > 2) {
      s16 splitSlot = std::min<s16>(lastUpdatedSlot, mSlotId);
      mGuardedLeaf.unlock();

      mSlotId = -1;
      JUMPMU_TRY() {
        mBTree.TrySplitMayJump(*mGuardedLeaf.mBf, splitSlot);
        WorkerCounters::MyCounters()
            .contention_split_succ_counter[mBTree.mTreeId]++;
        DLOG(INFO) << "[Contention Split] contention split succeed"
                   << ", pageId=" << mGuardedLeaf.mBf->header.mPageId
                   << ", contention pct=" << contentionPct
                   << ", split slot=" << splitSlot;
      }
      JUMPMU_CATCH() {
        WorkerCounters::MyCounters()
            .contention_split_fail_counter[mBTree.mTreeId]++;
        LOG(INFO) << "[Contention Split] contention split failed"
                  << ", pageId=" << mGuardedLeaf.mBf->header.mPageId
                  << ", contention pct=" << contentionPct
                  << ", split slot=" << splitSlot;
      }
    }
  }

  virtual OpCode RemoveCurrent() {
    if (!(mGuardedLeaf.mBf != nullptr && mSlotId >= 0 &&
          mSlotId < mGuardedLeaf->mNumSeps)) {
      DCHECK(false) << "RemoveCurrent failed"
                    << ", pageId=" << mGuardedLeaf.mBf->header.mPageId
                    << ", slotId=" << mSlotId;
      return OpCode::kOther;
    }
    mGuardedLeaf->removeSlot(mSlotId);
    return OpCode::kOK;
  }

  // Returns true if it tried to merge
  bool TryMergeIfNeeded() {
    if (mGuardedLeaf->FreeSpaceAfterCompaction() >=
        BTreeNode::UnderFullSize()) {
      mGuardedLeaf.unlock();
      mSlotId = -1;
      JUMPMU_TRY() {
        mBTree.TryMergeMayJump(*mGuardedLeaf.mBf);
      }
      JUMPMU_CATCH() {
        DLOG(INFO) << "TryMergeIfNeeded failed"
                   << ", pageId=" << mGuardedLeaf.mBf->header.mPageId;
      }
      return true;
    }
    return false;
  }
};

} // namespace leanstore::storage::btree
