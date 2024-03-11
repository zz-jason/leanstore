#pragma once

#include "BTreePessimisticIterator.hpp"
#include "leanstore/KVInterface.hpp"

#include <glog/logging.h>

using namespace leanstore::storage;

namespace leanstore::storage::btree {

class BTreePessimisticExclusiveIterator : public BTreePessimisticIterator {
public:
  BTreePessimisticExclusiveIterator(BTreeGeneric& tree)
      : BTreePessimisticIterator(tree, LatchMode::kPessimisticExclusive) {
  }

  BTreePessimisticExclusiveIterator(BTreeGeneric& tree, BufferFrame* bf,
                                    const uint64_t bfVersion)
      : BTreePessimisticIterator(tree, LatchMode::kPessimisticExclusive) {
    HybridGuard optimisticGuard(bf->mHeader.mLatch, bfVersion);
    optimisticGuard.JumpIfModifiedByOthers();
    mGuardedLeaf = GuardedBufferFrame<BTreeNode>(
        tree.mStore->mBufferManager.get(), std::move(optimisticGuard), bf);
    mGuardedLeaf.ToExclusiveMayJump();
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

  virtual bool HasEnoughSpaceFor(const uint16_t keySize,
                                 const uint16_t valSize) {
    return mGuardedLeaf->canInsert(keySize, valSize);
  }

  virtual void InsertToCurrentNode(Slice key, uint16_t valSize) {
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

  void SplitForKey(Slice key) {
    while (true) {
      JUMPMU_TRY() {
        if (mSlotId == -1 || !KeyInCurrentNode(key)) {
          mBTree.FindLeafCanJump(key, mGuardedLeaf);
        }
        BufferFrame* bf = mGuardedLeaf.mBf;
        mGuardedLeaf.unlock();
        mSlotId = -1;

        mBTree.TrySplitMayJump(*bf);
        COUNTERS_BLOCK() {
          WorkerCounters::MyCounters().dt_split[mBTree.mTreeId]++;
        }
        JUMPMU_BREAK;
      }
      JUMPMU_CATCH() {
        LOG(INFO) << "Split failed"
                  << ", treeId=" << mBTree.mTreeId
                  << ", pageId=" << mGuardedLeaf.mBf->mHeader.mPageId;
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
  virtual void ShortenWithoutCompaction(const uint16_t targetSize) {
    mGuardedLeaf->shortenPayload(mSlotId, targetSize);
  }

  bool ExtendPayload(const uint16_t targetSize) {
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
    const uint64_t randomNumber = utils::RandomGenerator::RandU64();

    // haven't met the contention stats update probability
    if ((randomNumber &
         ((1ull << FLAGS_contention_split_sample_probability) - 1)) != 0) {
      return;
    }
    auto& contentionStats = mGuardedLeaf.mBf->mHeader.mContentionStats;
    auto lastUpdatedSlot = contentionStats.mLastUpdatedSlot;
    contentionStats.Update(mGuardedLeaf.EncounteredContention(), mSlotId);
    DLOG(INFO) << "[Contention Split] ContentionStats updated"
               << ", pageId=" << mGuardedLeaf.mBf->mHeader.mPageId
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
      int16_t splitSlot = std::min<int16_t>(lastUpdatedSlot, mSlotId);
      mGuardedLeaf.unlock();

      mSlotId = -1;
      JUMPMU_TRY() {
        mBTree.TrySplitMayJump(*mGuardedLeaf.mBf, splitSlot);

        DLOG(INFO) << "[Contention Split] succeed"
                   << ", pageId=" << mGuardedLeaf.mBf->mHeader.mPageId
                   << ", contention pct=" << contentionPct
                   << ", split slot=" << splitSlot;

        COUNTERS_BLOCK() {
          WorkerCounters::MyCounters()
              .contention_split_succ_counter[mBTree.mTreeId]++;
          WorkerCounters::MyCounters().dt_split[mBTree.mTreeId]++;
        }
      }
      JUMPMU_CATCH() {
        LOG(INFO) << "[Contention Split] contention split failed"
                  << ", pageId=" << mGuardedLeaf.mBf->mHeader.mPageId
                  << ", contention pct=" << contentionPct
                  << ", split slot=" << splitSlot;

        COUNTERS_BLOCK() {
          WorkerCounters::MyCounters()
              .contention_split_fail_counter[mBTree.mTreeId]++;
        }
      }
    }
  }

  virtual OpCode RemoveCurrent() {
    if (!(mGuardedLeaf.mBf != nullptr && mSlotId >= 0 &&
          mSlotId < mGuardedLeaf->mNumSeps)) {
      DCHECK(false) << "RemoveCurrent failed"
                    << ", pageId=" << mGuardedLeaf.mBf->mHeader.mPageId
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
                   << ", pageId=" << mGuardedLeaf.mBf->mHeader.mPageId;
      }
      return true;
    }
    return false;
  }
};

} // namespace leanstore::storage::btree
