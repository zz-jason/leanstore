#pragma once

#include "BTreePessimisticIterator.hpp"

#include <glog/logging.h>

using namespace leanstore::storage;

namespace leanstore {
namespace storage {
namespace btree {

class BTreeExclusiveIterator : public BTreePessimisticIterator {
public:
  BTreeExclusiveIterator(BTreeGeneric& tree)
      : BTreePessimisticIterator(tree, LATCH_FALLBACK_MODE::EXCLUSIVE) {
  }

  BTreeExclusiveIterator(BTreeGeneric& tree, BufferFrame* bf,
                         const u64 bf_version)
      : BTreePessimisticIterator(tree, LATCH_FALLBACK_MODE::EXCLUSIVE) {
    HybridGuard as_it_was_witnessed(bf->header.mLatch, bf_version);
    as_it_was_witnessed.JumpIfModifiedByOthers();
    mGuardedLeaf =
        GuardedBufferFrame<BTreeNode>(std::move(as_it_was_witnessed), bf);
    mGuardedLeaf.ToExclusiveMayJump();
  }

  void MarkAsDirty() {
    mGuardedLeaf.MarkAsDirty();
  }

  virtual OP_RESULT seekToInsertWithHint(Slice key, bool higher = true) {
    ENSURE(mSlotId != -1);
    mSlotId = mGuardedLeaf->linearSearchWithBias(key, mSlotId, higher);
    if (mSlotId == -1) {
      return seekToInsert(key);
    } else {
      return OP_RESULT::OK;
    }
  }

  virtual OP_RESULT seekToInsert(Slice key) {
    if (mSlotId == -1 || !keyInCurrentBoundaries(key)) {
      gotoPage(key);
    }
    bool isEqual = false;
    mSlotId = mGuardedLeaf->lowerBound<false>(key, &isEqual);
    if (isEqual) {
      return OP_RESULT::DUPLICATE;
    } else {
      return OP_RESULT::OK;
    }
  }

  virtual OP_RESULT enoughSpaceInCurrentNode(const u16 keySize,
                                             const u16 valSize) {
    return (mGuardedLeaf->canInsert(keySize, valSize))
               ? OP_RESULT::OK
               : OP_RESULT::NOT_ENOUGH_SPACE;
  }
  virtual OP_RESULT enoughSpaceInCurrentNode(Slice key, const u16 valSize) {
    return (mGuardedLeaf->canInsert(key.size(), valSize))
               ? OP_RESULT::OK
               : OP_RESULT::NOT_ENOUGH_SPACE;
  }

  virtual void insertInCurrentNode(Slice key, u16 valSize) {
    DCHECK(keyInCurrentBoundaries(key));
    DCHECK(enoughSpaceInCurrentNode(key, valSize) == OP_RESULT::OK);
    mSlotId = mGuardedLeaf->insertDoNotCopyPayload(key, valSize, mSlotId);
  }

  virtual void insertInCurrentNode(Slice key, Slice val) {
    DCHECK(keyInCurrentBoundaries(key));
    DCHECK(enoughSpaceInCurrentNode(key, val.size()) == OP_RESULT::OK);
    DCHECK(mSlotId != -1);
    mSlotId = mGuardedLeaf->insertDoNotCopyPayload(key, val.size(), mSlotId);
    std::memcpy(mGuardedLeaf->ValData(mSlotId), val.data(), val.size());
  }

  virtual void splitForKey(Slice key) {
    int i = 0;
    while (true) {
      JUMPMU_TRY() {
        if (mSlotId == -1 || !keyInCurrentBoundaries(key)) {
          mBTree.FindLeafCanJump<LATCH_FALLBACK_MODE::SHARED>(key,
                                                              mGuardedLeaf);
        }
        BufferFrame* bf = mGuardedLeaf.mBf;
        mGuardedLeaf.unlock();
        mSlotId = -1;

        mBTree.trySplit(*bf);
        JUMPMU_BREAK;
      }
      JUMPMU_CATCH() {
      }
    }
  }

  virtual OP_RESULT insertKV(Slice key, Slice val) {
  restart : {
    OP_RESULT ret = seekToInsert(key);
    if (ret != OP_RESULT::OK) {
      return ret;
    }

    assert(keyInCurrentBoundaries(key));

    ret = enoughSpaceInCurrentNode(key, val.length());
    switch (ret) {
    case OP_RESULT::NOT_ENOUGH_SPACE: {
      splitForKey(key);
      goto restart;
    }
    case OP_RESULT::OK: {
      insertInCurrentNode(key, val);
      return OP_RESULT::OK;
    }
    default: {
      return ret;
    }
    }
  }
  }

  virtual OP_RESULT replaceKV(Slice, Slice) {
    ENSURE(false);
    return OP_RESULT::NOT_FOUND;
  }

  // The caller must retain the payload when using any of the following payload
  // resize functions
  virtual void shorten(const u16 new_size) {
    mGuardedLeaf->shortenPayload(mSlotId, new_size);
  }

  bool extendPayload(const u16 new_length) {
    if (new_length >= BTreeNode::Size()) {
      return false;
    }
    ENSURE(mSlotId != -1 && new_length > mGuardedLeaf->ValSize(mSlotId));
    OP_RESULT ret;
    while (!mGuardedLeaf->canExtendPayload(mSlotId, new_length)) {
      if (mGuardedLeaf->mNumSeps == 1) {
        return false;
      }
      assembleKey();
      Slice key = this->key();
      splitForKey(key);
      ret = seekExact(key);
      ENSURE(ret == OP_RESULT::OK);
    }
    assert(mSlotId != -1);
    mGuardedLeaf->extendPayload(mSlotId, new_length);
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
    const u64 randomNumber = utils::RandomGenerator::getRandU64();

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
        mBTree.trySplit(*mGuardedLeaf.mBf, splitSlot);
        WorkerCounters::myCounters()
            .contention_split_succ_counter[mBTree.mTreeId]++;
        DLOG(INFO) << "[Contention Split] contention split succeed"
                   << ", pageId=" << mGuardedLeaf.mBf->header.mPageId
                   << ", contention pct=" << contentionPct
                   << ", split slot=" << splitSlot;
      }
      JUMPMU_CATCH() {
        WorkerCounters::myCounters()
            .contention_split_fail_counter[mBTree.mTreeId]++;
        LOG(INFO) << "[Contention Split] contention split failed"
                  << ", pageId=" << mGuardedLeaf.mBf->header.mPageId
                  << ", contention pct=" << contentionPct
                  << ", split slot=" << splitSlot;
      }
    }
  }

  virtual OP_RESULT removeCurrent() {
    if (!(mGuardedLeaf.mBf != nullptr && mSlotId >= 0 &&
          mSlotId < mGuardedLeaf->mNumSeps)) {
      ENSURE(false);
      return OP_RESULT::OTHER;
    } else {
      mGuardedLeaf->removeSlot(mSlotId);
      return OP_RESULT::OK;
    }
  }

  virtual OP_RESULT removeKV(Slice key) {
    auto ret = seekExact(key);
    if (ret == OP_RESULT::OK) {
      mGuardedLeaf->removeSlot(mSlotId);
      return OP_RESULT::OK;
    } else {
      return ret;
    }
  }

  // Returns true if it tried to merge
  bool mergeIfNeeded() {
    if (mGuardedLeaf->freeSpaceAfterCompaction() >=
        BTreeNode::UnderFullSize()) {
      mGuardedLeaf.unlock();
      mSlotId = -1;
      JUMPMU_TRY() {
        mBTree.tryMerge(*mGuardedLeaf.mBf);
      }
      JUMPMU_CATCH() {
        // nothing, it is fine not to merge
      }
      return true;
    } else {
      return false;
    }
  }
};

} // namespace btree
} // namespace storage
} // namespace leanstore
