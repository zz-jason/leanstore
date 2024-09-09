#pragma once

#include "leanstore/KVInterface.hpp"
#include "leanstore/btree/core/PessimisticIterator.hpp"
#include "leanstore/concurrency/WorkerContext.hpp"
#include "leanstore/utils/CounterUtil.hpp"
#include "leanstore/utils/Defer.hpp"
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
    LS_DCHECK(Valid());
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
    LS_DCHECK(Valid());
    mSlotId = mGuardedLeaf->InsertDoNotCopyPayload(key, val.size(), mSlotId);
    std::memcpy(mGuardedLeaf->ValData(mSlotId), val.data(), val.size());
  }

  void SplitForKey(Slice key) {
    TXID sysTxId = cr::WorkerContext::My().StartSysTx();
    SCOPED_DEFER(cr::WorkerContext::My().CommitSysTx());

    while (true) {
      JUMPMU_TRY() {
        if (!Valid() || !KeyInCurrentNode(key)) {
          mBTree.FindLeafCanJump(key, mGuardedLeaf);
        }
        BufferFrame* bf = mGuardedLeaf.mBf;
        mGuardedLeaf.unlock();
        SetToInvalid();

        mBTree.TrySplitMayJump(sysTxId, *bf);
        COUNTER_INC(&leanstore::cr::tlsPerfCounters.mSplitSucceed);
        JUMPMU_BREAK;
      }
      JUMPMU_CATCH() {
        COUNTER_INC(&leanstore::cr::tlsPerfCounters.mSplitFailed);
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
      if (mGuardedLeaf->mNumSlots == 1) {
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
        mGuardedLeaf->mNumSlots > 2) {
      int16_t splitSlot = std::min<int16_t>(lastUpdatedSlot, mSlotId);
      mGuardedLeaf.unlock();

      mSlotId = -1;
      JUMPMU_TRY() {
        TXID sysTxId = cr::WorkerContext::My().StartSysTx();
        mBTree.TrySplitMayJump(sysTxId, *mGuardedLeaf.mBf, splitSlot);
        cr::WorkerContext::My().CommitSysTx();

        COUNTER_INC(&leanstore::cr::tlsPerfCounters.mContentionSplitSucceed);
        LS_DLOG("[Contention Split] succeed, pageId={}, contentionPct={}, splitSlot={}",
                mGuardedLeaf.mBf->mHeader.mPageId, contentionPct, splitSlot);
      }
      JUMPMU_CATCH() {
        COUNTER_INC(&leanstore::cr::tlsPerfCounters.mContentionSplitFailed);
        Log::Info("[Contention Split] contention split failed, pageId={}, "
                  "contentionPct={}, splitSlot={}",
                  mGuardedLeaf.mBf->mHeader.mPageId, contentionPct, splitSlot);
      }
    }
  }

  virtual OpCode RemoveCurrent() {
    if (!(mGuardedLeaf.mBf != nullptr && mSlotId >= 0 && mSlotId < mGuardedLeaf->mNumSlots)) {
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
        TXID sysTxId = cr::WorkerContext::My().StartSysTx();
        mBTree.TryMergeMayJump(sysTxId, *mGuardedLeaf.mBf);
        cr::WorkerContext::My().CommitSysTx();
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
