#pragma once

#include "BTreeGeneric.hpp"
#include "BTreeIteratorInterface.hpp"
#include "btree/core/BTreeNode.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/Units.hpp"
#include "sync/HybridLatch.hpp"
#include "utils/Log.hpp"
#include "utils/UserThread.hpp"

using namespace leanstore::storage;

namespace leanstore::storage::btree {
using LeafCallback = std::function<void(GuardedBufferFrame<BTreeNode>& guardedLeaf)>;

// Iterator
class BTreePessimisticIterator : public BTreeIteratorInterface {
  friend class BTreeGeneric;

public:
  //! The working btree, all the seek operations are based on this tree.
  BTreeGeneric& mBTree;

  const LatchMode mMode;

  //! mFuncEnterLeaf is called when the target leaf node is found.
  LeafCallback mFuncEnterLeaf = nullptr;

  //! mFuncExitLeaf is called when leaving the target leaf node.
  LeafCallback mFuncExitLeaf = nullptr;

  //! mFuncCleanUp is called when both parent and leaf are unlatched and before
  //! seeking for another key.
  std::function<void()> mFuncCleanUp = nullptr;

  //! The slot id of the current key in the leaf.
  //! Reset after every leaf change.
  int32_t mSlotId = -1;

  //! Indicates whether the prefix is copied in mBuffer
  bool mIsPrefixCopied = false;

  //! The latched leaf node of the current key.
  GuardedBufferFrame<BTreeNode> mGuardedLeaf;

  //! The latched parent node of mGuardedLeaf.
  GuardedBufferFrame<BTreeNode> mGuardedParent;

  //! The slot id in mGuardedParent of mGuardedLeaf.
  int32_t mLeafPosInParent = -1;

  //! Used to buffer the key at mSlotId or lower/upper fence keys.
  std::basic_string<uint8_t> mBuffer;

  //! The length of the lower or upper fence key.
  uint16_t mFenceSize = 0;

  //! Tndicates whether the mFenceSize is for lower or upper fence key.
  bool mIsUsingUpperFence;

protected:
  // We need a custom findLeafAndLatch to track the position in parent node
  void findLeafAndLatch(GuardedBufferFrame<BTreeNode>& guardedChild, Slice key,
                        LatchMode mode = LatchMode::kPessimisticShared) {
    while (true) {
      mLeafPosInParent = -1;
      JUMPMU_TRY() {
        mGuardedParent = GuardedBufferFrame<BTreeNode>(mBTree.mStore->mBufferManager.get(),
                                                       mBTree.mMetaNodeSwip);
        guardedChild.unlock();

        // it's the root node right now.
        guardedChild =
            GuardedBufferFrame<BTreeNode>(mBTree.mStore->mBufferManager.get(), mGuardedParent,
                                          mGuardedParent->mRightMostChildSwip);

        for (uint16_t level = 0; !guardedChild->mIsLeaf; level++) {
          COUNTERS_BLOCK() {
            WorkerCounters::MyCounters().dt_inner_page[mBTree.mTreeId]++;
          }
          mLeafPosInParent = guardedChild->LowerBound<false>(key);
          auto* childSwip = guardedChild->ChildSwipIncludingRightMost(mLeafPosInParent);
          mGuardedParent = std::move(guardedChild);
          if (level == mBTree.mHeight - 1) {
            guardedChild = GuardedBufferFrame<BTreeNode>(mBTree.mStore->mBufferManager.get(),
                                                         mGuardedParent, *childSwip, mode);
          } else {
            // latch the middle node optimistically
            guardedChild =
                GuardedBufferFrame<BTreeNode>(mBTree.mStore->mBufferManager.get(), mGuardedParent,
                                              *childSwip, LatchMode::kOptimisticSpin);
          }
        }

        mGuardedParent.unlock();
        if (mode == LatchMode::kPessimisticExclusive) {
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
      if (mMode == LatchMode::kPessimisticExclusive) {
        WorkerCounters::MyCounters().dt_goto_page_exec[mBTree.mTreeId]++;
      } else {
        WorkerCounters::MyCounters().dt_goto_page_shared[mBTree.mTreeId]++;
      }
    }

    // TODO: refactor when we get ride of serializability tests
    if (mMode == LatchMode::kPessimisticShared || mMode == LatchMode::kPessimisticExclusive) {
      findLeafAndLatch(mGuardedLeaf, key, mMode);
    } else {
      Log::Fatal("Unsupported latch mode: {}", uint64_t(mMode));
    }
  }

public:
  BTreePessimisticIterator(BTreeGeneric& tree, const LatchMode mode = LatchMode::kPessimisticShared)
      : mBTree(tree),
        mMode(mode),
        mBuffer() {
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
    mSlotId = mGuardedLeaf->LinearSearchWithBias<true>(key, mSlotId, higher);
    if (mSlotId == -1) {
      return SeekExact(key) ? OpCode::kOK : OpCode::kNotFound;
    }
    return OpCode::kOK;
  }

  virtual bool SeekExact(Slice key) override {
    if (mSlotId == -1 || !KeyInCurrentNode(key)) {
      gotoPage(key);
    }
    mSlotId = mGuardedLeaf->LowerBound<true>(key);
    return mSlotId != -1;
  }

  virtual bool Seek(Slice key) override {
    if (mSlotId == -1 || mGuardedLeaf->CompareKeyWithBoundaries(key) != 0) {
      gotoPage(key);
    }
    mSlotId = mGuardedLeaf->LowerBound<false>(key);
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
    if (mSlotId == -1 || mGuardedLeaf->CompareKeyWithBoundaries(key) != 0) {
      gotoPage(key);
    }

    bool isEqual = false;
    mSlotId = mGuardedLeaf->LowerBound<false>(key, &isEqual);
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
      ENSURE(mGuardedLeaf.mGuard.mState != GuardState::kOptimisticShared);

      // If we are not at the end of the leaf, return the next key in the leaf.
      if ((mSlotId + 1) < mGuardedLeaf->mNumSeps) {
        mSlotId += 1;
        return true;
      }

      // No more keys in the BTree, return false
      if (mGuardedLeaf->mUpperFence.mLength == 0) {
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

      if (utils::tlsStore->mStoreOption.mEnableOptimisticScan && mLeafPosInParent != -1) {
        JUMPMU_TRY() {
          if ((mLeafPosInParent + 1) <= mGuardedParent->mNumSeps) {
            int32_t nextLeafPos = mLeafPosInParent + 1;
            auto* nextLeafSwip = mGuardedParent->ChildSwipIncludingRightMost(nextLeafPos);
            GuardedBufferFrame<BTreeNode> guardedNextLeaf(mBTree.mStore->mBufferManager.get(),
                                                          mGuardedParent, *nextLeafSwip,
                                                          LatchMode::kOptimisticOrJump);
            if (mMode == LatchMode::kPessimisticExclusive) {
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
      mSlotId = mGuardedLeaf->LowerBound<false>(assembedFence());
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
      ENSURE(mGuardedLeaf.mGuard.mState != GuardState::kOptimisticShared);
      // If we are not at the beginning of the leaf, return the previous key
      // in the leaf.
      if (mSlotId > 0) {
        mSlotId -= 1;
        return true;
      }

      // No more keys in the BTree, return false
      if (mGuardedLeaf->mLowerFence.mLength == 0) {
        return false;
      }

      // Seek to the previous leaf
      mFenceSize = mGuardedLeaf->mLowerFence.mLength;
      mIsUsingUpperFence = false;
      if (mBuffer.size() < mFenceSize) {
        mBuffer.resize(mFenceSize, 0);
      }
      std::memcpy(&mBuffer[0], mGuardedLeaf->GetLowerFenceKey(), mFenceSize);

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

      if (utils::tlsStore->mStoreOption.mEnableOptimisticScan && mLeafPosInParent != -1) {
        JUMPMU_TRY() {
          if ((mLeafPosInParent - 1) >= 0) {
            int32_t nextLeafPos = mLeafPosInParent - 1;
            auto* nextLeafSwip = mGuardedParent->ChildSwip(nextLeafPos);
            GuardedBufferFrame<BTreeNode> guardedNextLeaf(mBTree.mStore->mBufferManager.get(),
                                                          mGuardedParent, *nextLeafSwip,
                                                          LatchMode::kOptimisticOrJump);
            if (mMode == LatchMode::kPessimisticExclusive) {
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
      mSlotId = mGuardedLeaf->LowerBound<false>(assembedFence(), &isEqual);
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
    if (auto fullKeySize = mGuardedLeaf->GetFullKeyLen(mSlotId); mBuffer.size() < fullKeySize) {
      mBuffer.resize(fullKeySize, 0);
      mIsPrefixCopied = false;
    }

    if (!mIsPrefixCopied) {
      mGuardedLeaf->CopyPrefix(&mBuffer[0]);
      mIsPrefixCopied = true;
    }
    mGuardedLeaf->CopyKeyWithoutPrefix(mSlotId, &mBuffer[mGuardedLeaf->mPrefixSize]);
  }

  virtual Slice key() override {
    LS_DCHECK(mBuffer.size() >= mGuardedLeaf->GetFullKeyLen(mSlotId));
    return Slice(&mBuffer[0], mGuardedLeaf->GetFullKeyLen(mSlotId));
  }

  virtual Slice KeyWithoutPrefix() override {
    return mGuardedLeaf->KeyWithoutPrefix(mSlotId);
  }

  virtual Slice value() override {
    return mGuardedLeaf->Value(mSlotId);
  }

  virtual bool KeyInCurrentNode(Slice key) {
    return mGuardedLeaf->CompareKeyWithBoundaries(key) == 0;
  }

  bool IsLastOne() {
    LS_DCHECK(mSlotId != -1);
    LS_DCHECK(mSlotId != mGuardedLeaf->mNumSeps);
    return (mSlotId + 1) == mGuardedLeaf->mNumSeps;
  }

  void Reset() {
    mGuardedLeaf.unlock();
    mSlotId = -1;
    mLeafPosInParent = -1;
    mIsPrefixCopied = false;
  }

  PID CurrentPageID() {
    return mGuardedLeaf.mBf->mHeader.mPageId;
  }

private:
  void assembleUpperFence() {
    mFenceSize = mGuardedLeaf->mUpperFence.mLength + 1;
    mIsUsingUpperFence = true;
    if (mBuffer.size() < mFenceSize) {
      mBuffer.resize(mFenceSize, 0);
    }
    std::memcpy(mBuffer.data(), mGuardedLeaf->GetUpperFenceKey(),
                mGuardedLeaf->mUpperFence.mLength);
    mBuffer[mFenceSize - 1] = 0;
  }

  inline Slice assembedFence() {
    LS_DCHECK(mBuffer.size() >= mFenceSize);
    return Slice(&mBuffer[0], mFenceSize);
  }
};

} // namespace leanstore::storage::btree
