#pragma once

#include "BTreeGeneric.hpp"
#include "BTreeIteratorInterface.hpp"

#include <glog/logging.h>

using namespace leanstore::storage;

namespace leanstore {
namespace storage {
namespace btree {

using LeafCallback =
    std::function<void(GuardedBufferFrame<BTreeNode>& guardedLeaf)>;

// Iterator
class BTreePessimisticIterator : public BTreePessimisticIteratorInterface {
  friend class BTreeGeneric;

public:
  /// The working btree, all the seek operations are based on this tree.
  BTreeGeneric& mBTree;

  const LATCH_FALLBACK_MODE mode;

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
  template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
  void findLeafAndLatch(GuardedBufferFrame<BTreeNode>& guardedChild,
                        Slice key) {
    while (true) {
      mLeafPosInParent = -1;
      JUMPMU_TRY() {
        mGuardedParent = GuardedBufferFrame<BTreeNode>(mBTree.mMetaNodeSwip);
        guardedChild.unlock();

        // it's the root node right now.
        guardedChild = GuardedBufferFrame<BTreeNode>(
            mGuardedParent, mGuardedParent->mRightMostChildSwip);

        for (u16 level = 0; !guardedChild->mIsLeaf; level++) {
          COUNTERS_BLOCK() {
            WorkerCounters::myCounters().dt_inner_page[mBTree.mTreeId]++;
          }
          mLeafPosInParent = guardedChild->lowerBound<false>(key);
          auto childSwip =
              &guardedChild->GetChildIncludingRightMost(mLeafPosInParent);
          mGuardedParent = std::move(guardedChild);
          if (level == mBTree.mHeight - 1) {
            guardedChild = GuardedBufferFrame(mGuardedParent, *childSwip, mode);
          } else {
            guardedChild = GuardedBufferFrame(mGuardedParent, *childSwip);
          }
        }

        mGuardedParent.unlock();
        if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
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
      if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
        WorkerCounters::myCounters().dt_goto_page_exec[mBTree.mTreeId]++;
      } else {
        WorkerCounters::myCounters().dt_goto_page_shared[mBTree.mTreeId]++;
      }
    }

    // TODO: refactor when we get ride of serializability tests
    if (mode == LATCH_FALLBACK_MODE::SHARED) {
      findLeafAndLatch<LATCH_FALLBACK_MODE::SHARED>(mGuardedLeaf, key);
    } else if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
      findLeafAndLatch<LATCH_FALLBACK_MODE::EXCLUSIVE>(mGuardedLeaf, key);
    } else {
      UNREACHABLE();
    }
  }

public:
  BTreePessimisticIterator(BTreeGeneric& tree, const LATCH_FALLBACK_MODE mode =
                                                   LATCH_FALLBACK_MODE::SHARED)
      : mBTree(tree), mode(mode), mBuffer(FLAGS_page_size, 0) {
  }

  void enterLeafCallback(LeafCallback cb) {
    mFuncEnterLeaf = cb;
  }

  void exitLeafCallback(LeafCallback cb) {
    mFuncExitLeaf = cb;
  }

  void cleanUpCallback(std::function<void()> cb) {
    mFuncCleanUp = cb;
  }

  inline Slice BufferedFence() {
    return Slice(&mBuffer[0], mFenceSize);
  }

  // EXP
  OpCode
  seekExactWithHint(Slice key, bool higher = true) {
    if (mSlotId == -1) {
      return seekExact(key);
    }
    mSlotId = mGuardedLeaf->linearSearchWithBias<true>(key, mSlotId, higher);
    if (mSlotId == -1) {
      return seekExact(key);
    } else {
      return OpCode::OK;
    }
  }

  virtual OpCode seekExact(Slice key) override {
    if (mSlotId == -1 || !keyInCurrentBoundaries(key)) {
      gotoPage(key);
    }
    mSlotId = mGuardedLeaf->lowerBound<true>(key);
    if (mSlotId != -1) {
      return OpCode::OK;
    } else {
      return OpCode::NOT_FOUND;
    }
  }

  virtual OpCode seek(Slice key) override {
    if (mSlotId == -1 || mGuardedLeaf->compareKeyWithBoundaries(key) != 0) {
      gotoPage(key);
    }
    mSlotId = mGuardedLeaf->lowerBound<false>(key);
    if (mSlotId < mGuardedLeaf->mNumSeps) {
      return OpCode::OK;
    } else {
      // TODO: Is there a better solution?
      // In composed keys {K1, K2}, it can happen that when we look for {2, 0}
      // we always land on {1,..} page because its upper bound is beyond {2,0}
      // Example: TPC-C Neworder
      return next();
    }
  }

  virtual OpCode seekForPrev(Slice key) override {
    if (mSlotId == -1 || mGuardedLeaf->compareKeyWithBoundaries(key) != 0) {
      gotoPage(key);
    }
    bool is_equal = false;
    mSlotId = mGuardedLeaf->lowerBound<false>(key, &is_equal);
    if (is_equal == true) {
      return OpCode::OK;
    } else if (mSlotId == 0) {
      return prev();
    } else {
      mSlotId -= 1;
      return OpCode::OK;
    }
  }

  virtual OpCode next() override {
    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().dt_next_tuple[mBTree.mTreeId]++;
    }
    while (true) {
      ENSURE(mGuardedLeaf.mGuard.mState != GUARD_STATE::OPTIMISTIC);
      if ((mSlotId + 1) < mGuardedLeaf->mNumSeps) {
        mSlotId += 1;
        return OpCode::OK;
      } else if (mGuardedLeaf->mUpperFence.length == 0) {
        return OpCode::NOT_FOUND;
      } else {
        mFenceSize = mGuardedLeaf->mUpperFence.length + 1;
        mIsUsingUpperFence = true;
        DCHECK(mBuffer.size() >= mFenceSize);
        std::memcpy(mBuffer.data(), mGuardedLeaf->getUpperFenceKey(),
                    mGuardedLeaf->mUpperFence.length);
        mBuffer[mFenceSize - 1] = 0;

        if (mFuncExitLeaf != nullptr) {
          mFuncExitLeaf(mGuardedLeaf);
          mFuncExitLeaf = nullptr;
        }

        mGuardedParent.unlock();
        mGuardedLeaf.unlock();

        if (mFuncCleanUp != nullptr) {
          mFuncCleanUp();
          mFuncCleanUp = nullptr;
        }

        if (FLAGS_optimistic_scan && mLeafPosInParent != -1) {
          JUMPMU_TRY() {
            if ((mLeafPosInParent + 1) <= mGuardedParent->mNumSeps) {
              s32 next_leaf_pos = mLeafPosInParent + 1;
              auto& c_swip =
                  mGuardedParent->GetChildIncludingRightMost(next_leaf_pos);
              GuardedBufferFrame guardedNextLeaf(mGuardedParent, c_swip,
                                                 LATCH_FALLBACK_MODE::JUMP);
              if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
                guardedNextLeaf.TryToExclusiveMayJump();
              } else {
                guardedNextLeaf.TryToSharedMayJump();
              }
              mGuardedLeaf.JumpIfModifiedByOthers();
              mGuardedLeaf = std::move(guardedNextLeaf);
              mLeafPosInParent = next_leaf_pos;
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
                WorkerCounters::myCounters()
                    .dt_next_tuple_opt[mBTree.mTreeId]++;
              }
              JUMPMU_RETURN OpCode::OK;
            }
          }
          JUMPMU_CATCH() {
          }
        }
        // Construct the next key (lower bound)
        gotoPage(BufferedFence());

        if (mGuardedLeaf->mNumSeps == 0) {
          cleanUpCallback([&, to_find = mGuardedLeaf.mBf]() {
            JUMPMU_TRY() {
              mBTree.tryMerge(*to_find, true);
            }
            JUMPMU_CATCH() {
            }
          });
          COUNTERS_BLOCK() {
            WorkerCounters::myCounters().dt_empty_leaf[mBTree.mTreeId]++;
          }
          continue;
        }
        mSlotId = mGuardedLeaf->lowerBound<false>(BufferedFence());
        if (mSlotId == mGuardedLeaf->mNumSeps) {
          continue;
        }
        return OpCode::OK;
      }
    }
  }

  virtual OpCode prev() override {
    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().dt_prev_tuple[mBTree.mTreeId]++;
    }

    while (true) {
      ENSURE(mGuardedLeaf.mGuard.mState != GUARD_STATE::OPTIMISTIC);
      if ((mSlotId - 1) >= 0) {
        mSlotId -= 1;
        return OpCode::OK;
      } else if (mGuardedLeaf->mLowerFence.length == 0) {
        return OpCode::NOT_FOUND;
      } else {
        mFenceSize = mGuardedLeaf->mLowerFence.length;
        mIsUsingUpperFence = false;
        DCHECK(mBuffer.size() >= mFenceSize);
        std::memcpy(&mBuffer[0], mGuardedLeaf->getLowerFenceKey(), mFenceSize);

        if (mFuncExitLeaf != nullptr) {
          mFuncExitLeaf(mGuardedLeaf);
          mFuncExitLeaf = nullptr;
        }

        mGuardedParent.unlock();
        mGuardedLeaf.unlock();

        if (mFuncCleanUp != nullptr) {
          mFuncCleanUp();
          mFuncCleanUp = nullptr;
        }

        if (FLAGS_optimistic_scan && mLeafPosInParent != -1) {
          JUMPMU_TRY() {
            if ((mLeafPosInParent - 1) >= 0) {
              s32 next_leaf_pos = mLeafPosInParent - 1;
              Swip<BTreeNode>& c_swip = mGuardedParent->getChild(next_leaf_pos);
              GuardedBufferFrame guardedNextLeaf(mGuardedParent, c_swip,
                                                 LATCH_FALLBACK_MODE::JUMP);
              if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
                guardedNextLeaf.TryToExclusiveMayJump();
              } else {
                guardedNextLeaf.TryToSharedMayJump();
              }
              mGuardedLeaf.JumpIfModifiedByOthers();
              mGuardedLeaf = std::move(guardedNextLeaf);
              mLeafPosInParent = next_leaf_pos;
              mSlotId = mGuardedLeaf->mNumSeps - 1;
              mIsPrefixCopied = false;

              if (mFuncEnterLeaf != nullptr) {
                mFuncEnterLeaf(mGuardedLeaf);
              }

              if (mGuardedLeaf->mNumSeps == 0) {
                JUMPMU_CONTINUE;
              }
              COUNTERS_BLOCK() {
                WorkerCounters::myCounters()
                    .dt_prev_tuple_opt[mBTree.mTreeId]++;
              }
              JUMPMU_RETURN OpCode::OK;
            }
          }
          JUMPMU_CATCH() {
          }
        }
        // Construct the next key (lower bound)
        gotoPage(BufferedFence());

        if (mGuardedLeaf->mNumSeps == 0) {
          COUNTERS_BLOCK() {
            WorkerCounters::myCounters().dt_empty_leaf[mBTree.mTreeId]++;
          }
          continue;
        }
        bool is_equal = false;
        mSlotId = mGuardedLeaf->lowerBound<false>(BufferedFence(), &is_equal);
        if (is_equal) {
          return OpCode::OK;
        } else if (mSlotId > 0) {
          mSlotId -= 1;
        } else {
          continue;
        }
      }
    }
  }

  virtual void assembleKey() {
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

  virtual MutableSlice mutableKeyInBuffer() {
    DCHECK(mBuffer.size() >= mGuardedLeaf->getFullKeyLen(mSlotId));
    return MutableSlice(&mBuffer[0], mGuardedLeaf->getFullKeyLen(mSlotId));
  }

  virtual MutableSlice mutableKeyInBuffer(u16 size) {
    DCHECK(mBuffer.size() >= size);
    return MutableSlice(&mBuffer[0], size);
  }

  virtual bool isKeyEqualTo(Slice other) override {
    ENSURE(false);
    return other == key();
  }

  virtual Slice KeyWithoutPrefix() override {
    return mGuardedLeaf->KeyWithoutPrefix(mSlotId);
  }

  virtual Slice value() override {
    return mGuardedLeaf->Value(mSlotId);
  }

  virtual bool keyInCurrentBoundaries(Slice key) {
    return mGuardedLeaf->compareKeyWithBoundaries(key) == 0;
  }

  bool isValid() {
    return mSlotId != -1;
  }

  bool isLastOne() {
    DCHECK(isValid());
    DCHECK(mSlotId != mGuardedLeaf->mNumSeps);
    return (mSlotId + 1) == mGuardedLeaf->mNumSeps;
  }

  void reset() {
    mGuardedLeaf.unlock();
    mSlotId = -1;
    mLeafPosInParent = -1;
    mIsPrefixCopied = false;
  }

  PID CurrentPageID() {
    return mGuardedLeaf.mBf->header.mPageId;
  }
};

} // namespace btree
} // namespace storage
} // namespace leanstore
