#pragma once

#include "BTreeGeneric.hpp"
#include "BTreeIteratorInterface.hpp"
#include "KVInterface.hpp"

#include <glog/logging.h>

using namespace leanstore::storage;

namespace leanstore {
namespace storage {
namespace btree {

using LeafCallback =
    std::function<void(GuardedBufferFrame<BTreeNode>& guardedLeaf)>;

// Iterator
class BTreePessimisticIterator : public BTreeIteratorInterface {
  friend class BTreeGeneric;

public:
  /// The working btree, all the seek operations are based on this tree.
  BTreeGeneric& mBTree;

  const LatchMode mMode;

  /// mFuncEnterLeaf is called when the target leaf node is found.
  LeafCallback mFuncEnterLeaf = nullptr;

  /// mFuncExitLeaf is called when leaving the target leaf node.
  LeafCallback mFuncExitLeaf = nullptr;

  /// mFuncCleanUp is called when both parent and leaf are unlatched and before
  /// seeking for another key.
  std::function<void()> mFuncCleanUp = nullptr;

  /// The slot id of the current key in the leaf.
  /// Reset after every leaf change.
  s32 mSlotId = -1;

  /// Indicates whether the prefix is copied in mBuffer
  bool mIsPrefixCopied = false;

  /// The latched leaf node of the current key.
  GuardedBufferFrame<BTreeNode> mGuardedLeaf;

  /// The latched parent node of mGuardedLeaf.
  GuardedBufferFrame<BTreeNode> mGuardedParent;

  /// The slot id in mGuardedParent of mGuardedLeaf.
  s32 mLeafPosInParent = -1;

  /// Used to buffer the key at mSlotId or lower/upper fence keys.
  std::basic_string<u8> mBuffer;

  /// The length of the lower or upper fence key.
  u16 mFenceSize = 0;

  /// Tndicates whether the mFenceSize is for lower or upper fence key.
  bool mIsUsingUpperFence;

protected:
  // We need a custom findLeafAndLatch to track the position in parent node
  template <LatchMode mode = LatchMode::kShared>
  void findLeafAndLatch(GuardedBufferFrame<BTreeNode>& guardedChild,
                        Slice key) {
    while (true) {
      mLeafPosInParent = -1;
      JUMPMU_TRY() {
        mGuardedParent = GuardedBufferFrame<BTreeNode>(
            mBTree.mStore->mBufferManager.get(), mBTree.mMetaNodeSwip);
        guardedChild.unlock();

        // it's the root node right now.
        guardedChild = GuardedBufferFrame<BTreeNode>(
            mBTree.mStore->mBufferManager.get(), mGuardedParent,
            mGuardedParent->mRightMostChildSwip);

        for (u16 level = 0; !guardedChild->mIsLeaf; level++) {
          COUNTERS_BLOCK() {
            WorkerCounters::MyCounters().dt_inner_page[mBTree.mTreeId]++;
          }
          mLeafPosInParent = guardedChild->lowerBound<false>(key);
          auto* childSwip =
              &guardedChild->GetChildIncludingRightMost(mLeafPosInParent);
          mGuardedParent = std::move(guardedChild);
          if (level == mBTree.mHeight - 1) {
            guardedChild =
                GuardedBufferFrame(mBTree.mStore->mBufferManager.get(),
                                   mGuardedParent, *childSwip, mode);
          } else {
            guardedChild =
                GuardedBufferFrame(mBTree.mStore->mBufferManager.get(),
                                   mGuardedParent, *childSwip);
          }
        }

        mGuardedParent.unlock();
        if (mode == LatchMode::kExclusive) {
          guardedChild.ToExclusiveMayJump();
        } else {
          guardedChild.ToSharedMayJump();
        }
        mIsPrefixCopied = false;
        if (mFuncEnterLeaf != nullptr) {
          mFuncEnterLeaf(guardedChild);
        }
        JUMPMU_RETURN;
      }
      JUMPMU_CATCH() {
      }
    }
  }

  void gotoPage(const Slice& key) {
    COUNTERS_BLOCK() {
      if (mMode == LatchMode::kExclusive) {
        WorkerCounters::MyCounters().dt_goto_page_exec[mBTree.mTreeId]++;
      } else {
        WorkerCounters::MyCounters().dt_goto_page_shared[mBTree.mTreeId]++;
      }
    }

    // TODO: refactor when we get ride of serializability tests
    if (mMode == LatchMode::kShared) {
      findLeafAndLatch<LatchMode::kShared>(mGuardedLeaf, key);
    } else if (mMode == LatchMode::kExclusive) {
      findLeafAndLatch<LatchMode::kExclusive>(mGuardedLeaf, key);
    } else {
      UNREACHABLE();
    }
  }

public:
  BTreePessimisticIterator(BTreeGeneric& tree,
                           const LatchMode mode = LatchMode::kShared)
      : mBTree(tree),
        mMode(mode),
        mBuffer(FLAGS_page_size, 0) {
  }

  void SetEnterLeafCallback(LeafCallback cb) {
    mFuncEnterLeaf = cb;
  }

  void SetExitLeafCallback(LeafCallback cb) {
    mFuncExitLeaf = cb;
  }

  void SetCleanUpCallback(std::function<void()> cb) {
    mFuncCleanUp = cb;
  }

  // EXP
  OpCode SeekExactWithHint(Slice key, bool higher = true) {
    if (mSlotId == -1) {
      return SeekExact(key) ? OpCode::kOK : OpCode::kNotFound;
    }
    mSlotId = mGuardedLeaf->linearSearchWithBias<true>(key, mSlotId, higher);
    if (mSlotId == -1) {
      return SeekExact(key) ? OpCode::kOK : OpCode::kNotFound;
    }
    return OpCode::kOK;
  }

  virtual bool SeekExact(Slice key) override {
    if (mSlotId == -1 || !KeyInCurrentNode(key)) {
      gotoPage(key);
    }
    mSlotId = mGuardedLeaf->lowerBound<true>(key);
    return mSlotId != -1;
  }

  virtual bool Seek(Slice key) override {
    if (mSlotId == -1 || mGuardedLeaf->compareKeyWithBoundaries(key) != 0) {
      gotoPage(key);
    }
    mSlotId = mGuardedLeaf->lowerBound<false>(key);
    if (mSlotId < mGuardedLeaf->mNumSeps) {
      return true;
    }

    // TODO: Is there a better solution?
    // In composed keys {K1, K2}, it can happen that when we look for {2, 0}
    // we always land on {1,..} page because its upper bound is beyond {2,0}
    // Example: TPC-C Neworder
    return Next();
  }

  virtual bool SeekForPrev(Slice key) override {
    if (mSlotId == -1 || mGuardedLeaf->compareKeyWithBoundaries(key) != 0) {
      gotoPage(key);
    }

    bool isEqual = false;
    mSlotId = mGuardedLeaf->lowerBound<false>(key, &isEqual);
    if (isEqual == true) {
      return true;
    }

    if (mSlotId == 0) {
      return Prev();
    }

    mSlotId -= 1;
    return true;
  }

  virtual bool Next() override {
    COUNTERS_BLOCK() {
      WorkerCounters::MyCounters().dt_next_tuple[mBTree.mTreeId]++;
    }
    while (true) {
      ENSURE(mGuardedLeaf.mGuard.mState != GuardState::kOptimistic);

      // If we are not at the end of the leaf, return the next key in the leaf.
      if ((mSlotId + 1) < mGuardedLeaf->mNumSeps) {
        mSlotId += 1;
        return true;
      }

      // No more keys in the BTree, return false
      if (mGuardedLeaf->mUpperFence.length == 0) {
        return false;
      }

      assembleUpperFence();

      if (mFuncExitLeaf != nullptr) {
        mFuncExitLeaf(mGuardedLeaf);
        mFuncExitLeaf = nullptr;
      }

      mGuardedLeaf.unlock();

      if (mFuncCleanUp != nullptr) {
        mFuncCleanUp();
        mFuncCleanUp = nullptr;
      }

      if (FLAGS_optimistic_scan && mLeafPosInParent != -1) {
        JUMPMU_TRY() {
          if ((mLeafPosInParent + 1) <= mGuardedParent->mNumSeps) {
            s32 nextLeafPos = mLeafPosInParent + 1;
            auto& nextLeafSwip =
                mGuardedParent->GetChildIncludingRightMost(nextLeafPos);
            GuardedBufferFrame guardedNextLeaf(
                mBTree.mStore->mBufferManager.get(), mGuardedParent,
                nextLeafSwip, LatchMode::kJump);
            if (mMode == LatchMode::kExclusive) {
              guardedNextLeaf.TryToExclusiveMayJump();
            } else {
              guardedNextLeaf.TryToSharedMayJump();
            }
            mGuardedLeaf.JumpIfModifiedByOthers();
            mGuardedLeaf = std::move(guardedNextLeaf);
            mLeafPosInParent = nextLeafPos;
            mSlotId = 0;
            mIsPrefixCopied = false;

            if (mFuncEnterLeaf != nullptr) {
              mFuncEnterLeaf(mGuardedLeaf);
            }

            if (mGuardedLeaf->mNumSeps == 0) {
              JUMPMU_CONTINUE;
            }
            ENSURE(mSlotId < mGuardedLeaf->mNumSeps);
            COUNTERS_BLOCK() {
              WorkerCounters::MyCounters().dt_next_tuple_opt[mBTree.mTreeId]++;
            }
            JUMPMU_RETURN true;
          }
        }
        JUMPMU_CATCH() {
        }
      }

      mGuardedParent.unlock();
      gotoPage(assembedFence());

      if (mGuardedLeaf->mNumSeps == 0) {
        SetCleanUpCallback([&, toMerge = mGuardedLeaf.mBf]() {
          JUMPMU_TRY() {
            mBTree.TryMergeMayJump(*toMerge, true);
          }
          JUMPMU_CATCH() {
          }
        });
        COUNTERS_BLOCK() {
          WorkerCounters::MyCounters().dt_empty_leaf[mBTree.mTreeId]++;
        }
        continue;
      }
      mSlotId = mGuardedLeaf->lowerBound<false>(assembedFence());
      if (mSlotId == mGuardedLeaf->mNumSeps) {
        continue;
      }
      return true;
    }
  }

  virtual bool Prev() override {
    COUNTERS_BLOCK() {
      WorkerCounters::MyCounters().dt_prev_tuple[mBTree.mTreeId]++;
    }

    while (true) {
      ENSURE(mGuardedLeaf.mGuard.mState != GuardState::kOptimistic);
      // If we are not at the beginning of the leaf, return the previous key
      // in the leaf.
      if (mSlotId > 0) {
        mSlotId -= 1;
        return true;
      }

      // No more keys in the BTree, return false
      if (mGuardedLeaf->mLowerFence.length == 0) {
        return false;
      }

      // Seek to the previous leaf
      mFenceSize = mGuardedLeaf->mLowerFence.length;
      mIsUsingUpperFence = false;
      DCHECK(mBuffer.size() >= mFenceSize);
      std::memcpy(&mBuffer[0], mGuardedLeaf->getLowerFenceKey(), mFenceSize);

      // callback before exiting current leaf
      if (mFuncExitLeaf != nullptr) {
        mFuncExitLeaf(mGuardedLeaf);
        mFuncExitLeaf = nullptr;
      }

      mGuardedParent.unlock();
      mGuardedLeaf.unlock();

      // callback after exiting current leaf
      if (mFuncCleanUp != nullptr) {
        mFuncCleanUp();
        mFuncCleanUp = nullptr;
      }

      if (FLAGS_optimistic_scan && mLeafPosInParent != -1) {
        JUMPMU_TRY() {
          if ((mLeafPosInParent - 1) >= 0) {
            s32 nextLeafPos = mLeafPosInParent - 1;
            auto& nextLeafSwip = mGuardedParent->getChild(nextLeafPos);
            GuardedBufferFrame guardedNextLeaf(
                mBTree.mStore->mBufferManager.get(), mGuardedParent,
                nextLeafSwip, LatchMode::kJump);
            if (mMode == LatchMode::kExclusive) {
              guardedNextLeaf.TryToExclusiveMayJump();
            } else {
              guardedNextLeaf.TryToSharedMayJump();
            }
            mGuardedLeaf.JumpIfModifiedByOthers();
            mGuardedLeaf = std::move(guardedNextLeaf);
            mLeafPosInParent = nextLeafPos;
            mSlotId = mGuardedLeaf->mNumSeps - 1;
            mIsPrefixCopied = false;

            if (mFuncEnterLeaf != nullptr) {
              mFuncEnterLeaf(mGuardedLeaf);
            }

            if (mGuardedLeaf->mNumSeps == 0) {
              JUMPMU_CONTINUE;
            }
            COUNTERS_BLOCK() {
              WorkerCounters::MyCounters().dt_prev_tuple_opt[mBTree.mTreeId]++;
            }
            JUMPMU_RETURN true;
          }
        }
        JUMPMU_CATCH() {
        }
      }

      // Construct the next key (lower bound)
      gotoPage(assembedFence());

      if (mGuardedLeaf->mNumSeps == 0) {
        COUNTERS_BLOCK() {
          WorkerCounters::MyCounters().dt_empty_leaf[mBTree.mTreeId]++;
        }
        continue;
      }
      bool isEqual = false;
      mSlotId = mGuardedLeaf->lowerBound<false>(assembedFence(), &isEqual);
      if (isEqual) {
        return true;
      }

      if (mSlotId > 0) {
        mSlotId -= 1;
      } else {
        continue;
      }
    }
  }

  virtual void AssembleKey() {
    if (!mIsPrefixCopied) {
      mGuardedLeaf->copyPrefix(&mBuffer[0]);
      mIsPrefixCopied = true;
    }
    mGuardedLeaf->copyKeyWithoutPrefix(mSlotId,
                                       &mBuffer[mGuardedLeaf->mPrefixSize]);
  }

  virtual Slice key() override {
    DCHECK(mBuffer.size() >= mGuardedLeaf->getFullKeyLen(mSlotId));
    return Slice(&mBuffer[0], mGuardedLeaf->getFullKeyLen(mSlotId));
  }

  virtual Slice KeyWithoutPrefix() override {
    return mGuardedLeaf->KeyWithoutPrefix(mSlotId);
  }

  virtual Slice value() override {
    return mGuardedLeaf->Value(mSlotId);
  }

  virtual bool KeyInCurrentNode(Slice key) {
    return mGuardedLeaf->compareKeyWithBoundaries(key) == 0;
  }

  bool IsLastOne() {
    DCHECK(mSlotId != -1);
    DCHECK(mSlotId != mGuardedLeaf->mNumSeps);
    return (mSlotId + 1) == mGuardedLeaf->mNumSeps;
  }

  void Reset() {
    mGuardedLeaf.unlock();
    mSlotId = -1;
    mLeafPosInParent = -1;
    mIsPrefixCopied = false;
  }

  PID CurrentPageID() {
    return mGuardedLeaf.mBf->header.mPageId;
  }

private:
  void assembleUpperFence() {
    mFenceSize = mGuardedLeaf->mUpperFence.length + 1;
    mIsUsingUpperFence = true;
    DCHECK(mBuffer.size() >= mFenceSize);
    std::memcpy(mBuffer.data(), mGuardedLeaf->getUpperFenceKey(),
                mGuardedLeaf->mUpperFence.length);
    mBuffer[mFenceSize - 1] = 0;
  }

  inline Slice assembedFence() {
    return Slice(&mBuffer[0], mFenceSize);
  }
};

} // namespace btree
} // namespace storage
} // namespace leanstore
