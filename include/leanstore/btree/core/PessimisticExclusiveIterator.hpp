#pragma once

#include "leanstore/KVInterface.hpp"
#include "leanstore/btree/core/PessimisticIterator.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/UserThread.hpp"

namespace leanstore::storage::btree {

class PessimisticExclusiveIterator : public PessimisticIterator {
public:
  PessimisticExclusiveIterator(BTreeGeneric& tree)
      : PessimisticIterator(tree, LatchMode::kPessimisticExclusive) {
  }

  PessimisticExclusiveIterator(BTreeGeneric& tree, BufferFrame* bf, const uint64_t bfVersion)
      : PessimisticIterator(tree, LatchMode::kPessimisticExclusive) {
    HybridGuard optimisticGuard(bf->mHeader.mLatch, bfVersion);
    optimisticGuard.JumpIfModifiedByOthers();
    mGuardedLeaf = GuardedBufferFrame<BTreeNode>(tree.mStore->mBufferManager.get(),
                                                 std::move(optimisticGuard), bf);
    mGuardedLeaf.ToExclusiveMayJump();
  }

  virtual OpCode SeekToInsertWithHint(Slice key, bool higher = true) {
    ENSURE(mSlotId != -1);
    mSlotId = mGuardedLeaf->LinearSearchWithBias(key, mSlotId, higher);
    if (mSlotId == -1) {
      return SeekToInsert(key);
    }
    return OpCode::kOK;
  }

  virtual OpCode SeekToInsert(Slice key) {
    seekToTargetPageOnDemand(key);

    bool isEqual = false;
    mSlotId = mGuardedLeaf->LowerBound<false>(key, &isEqual);
    if (isEqual) {
      return OpCode::kDuplicated;
    }
    return OpCode::kOK;
  }

  virtual bool HasEnoughSpaceFor(const uint16_t keySize, const uint16_t valSize) {
    return mGuardedLeaf->CanInsert(keySize, valSize);
  }

  virtual void InsertToCurrentNode(Slice key, uint16_t valSize) {
    LS_DCHECK(KeyInCurrentNode(key));
    LS_DCHECK(HasEnoughSpaceFor(key.size(), valSize));
    mSlotId = mGuardedLeaf->InsertDoNotCopyPayload(key, valSize, mSlotId);
  }

  virtual void InsertToCurrentNode(Slice key, Slice val) {
    LS_DCHECK(KeyInCurrentNode(key));
    LS_DCHECK(HasEnoughSpaceFor(key.size(), val.size()));
    LS_DCHECK(mSlotId != -1);
    mSlotId = mGuardedLeaf->InsertDoNotCopyPayload(key, val.size(), mSlotId);
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
      }
    }
  }

  virtual OpCode InsertKV(Slice key, Slice val) {
    while (true) {
      OpCode ret = SeekToInsert(key);
      if (ret != OpCode::kOK) {
        return ret;
      }
      LS_DCHECK(KeyInCurrentNode(key));
      if (!HasEnoughSpaceFor(key.size(), val.length())) {
        SplitForKey(key);
        continue;
      }

      InsertToCurrentNode(key, val);
      return OpCode::kOK;
    }
  }

  //! The caller must retain the payload when using any of the following payload resize functions
  virtual void ShortenWithoutCompaction(const uint16_t targetSize) {
    mGuardedLeaf->ShortenPayload(mSlotId, targetSize);
  }

  bool ExtendPayload(const uint16_t targetSize) {
    if (targetSize >= BTreeNode::Size()) {
      return false;
    }
    LS_DCHECK(mSlotId != -1 && targetSize > mGuardedLeaf->ValSize(mSlotId));
    while (!mGuardedLeaf->CanExtendPayload(mSlotId, targetSize)) {
      if (mGuardedLeaf->mNumSeps == 1) {
        return false;
      }
      AssembleKey();
      Slice key = this->Key();
      SplitForKey(key);
      SeekToEqual(key);
      LS_DCHECK(Valid());
    }
    LS_DCHECK(mSlotId != -1);
    mGuardedLeaf->ExtendPayload(mSlotId, targetSize);
    return true;
  }

  virtual MutableSlice MutableVal() {
    return MutableSlice(mGuardedLeaf->ValData(mSlotId), mGuardedLeaf->ValSize(mSlotId));
  }

  //! Updates contention statistics after each slot modification on the page.
  virtual void UpdateContentionStats() {
    if (!utils::tlsStore->mStoreOption->mEnableContentionSplit) {
      return;
    }
    const uint64_t randomNumber = utils::RandomGenerator::RandU64();

    // haven't met the contention stats update probability
    if ((randomNumber &
         ((1ull << utils::tlsStore->mStoreOption->mContentionSplitSampleProbability) - 1)) != 0) {
      return;
    }
    auto& contentionStats = mGuardedLeaf.mBf->mHeader.mContentionStats;
    auto lastUpdatedSlot = contentionStats.mLastUpdatedSlot;
    contentionStats.Update(mGuardedLeaf.EncounteredContention(), mSlotId);
    LS_DLOG("[Contention Split] ContentionStats updated, pageId={}, slot={}, "
            "encountered contention={}",
            mGuardedLeaf.mBf->mHeader.mPageId, mSlotId, mGuardedLeaf.EncounteredContention());

    // haven't met the contention split validation probability
    if ((randomNumber & ((1ull << utils::tlsStore->mStoreOption->mContentionSplitProbility) - 1)) !=
        0) {
      return;
    }
    auto contentionPct = contentionStats.ContentionPercentage();
    contentionStats.Reset();
    if (lastUpdatedSlot != mSlotId &&
        contentionPct >= utils::tlsStore->mStoreOption->mContentionSplitThresholdPct &&
        mGuardedLeaf->mNumSeps > 2) {
      int16_t splitSlot = std::min<int16_t>(lastUpdatedSlot, mSlotId);
      mGuardedLeaf.unlock();

      mSlotId = -1;
      JUMPMU_TRY() {
        mBTree.TrySplitMayJump(*mGuardedLeaf.mBf, splitSlot);

        LS_DLOG("[Contention Split] succeed, pageId={}, contention pct={}, split "
                "slot={}",
                mGuardedLeaf.mBf->mHeader.mPageId, contentionPct, splitSlot);

        COUNTERS_BLOCK() {
          WorkerCounters::MyCounters().contention_split_succ_counter[mBTree.mTreeId]++;
          WorkerCounters::MyCounters().dt_split[mBTree.mTreeId]++;
        }
      }
      JUMPMU_CATCH() {
        Log::Info("[Contention Split] contention split failed, pageId={}, contention "
                  "pct={}, split slot={}",
                  mGuardedLeaf.mBf->mHeader.mPageId, contentionPct, splitSlot);

        COUNTERS_BLOCK() {
          WorkerCounters::MyCounters().contention_split_fail_counter[mBTree.mTreeId]++;
        }
      }
    }
  }

  virtual OpCode RemoveCurrent() {
    if (!(mGuardedLeaf.mBf != nullptr && mSlotId >= 0 && mSlotId < mGuardedLeaf->mNumSeps)) {
      LS_DCHECK(false, "RemoveCurrent failed, pageId={}, slotId={}",
                mGuardedLeaf.mBf->mHeader.mPageId, mSlotId);
      return OpCode::kOther;
    }
    mGuardedLeaf->RemoveSlot(mSlotId);
    return OpCode::kOK;
  }

  // Returns true if it tried to merge
  bool TryMergeIfNeeded() {
    if (mGuardedLeaf->FreeSpaceAfterCompaction() >= BTreeNode::UnderFullSize()) {
      mGuardedLeaf.unlock();
      mSlotId = -1;
      JUMPMU_TRY() {
        mBTree.TryMergeMayJump(*mGuardedLeaf.mBf);
      }
      JUMPMU_CATCH() {
        LS_DLOG("TryMergeIfNeeded failed, pageId={}", mGuardedLeaf.mBf->mHeader.mPageId);
      }
      return true;
    }
    return false;
  }
};

} // namespace leanstore::storage::btree
