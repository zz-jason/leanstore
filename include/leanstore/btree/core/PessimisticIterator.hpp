#pragma once

#include "BTreeGeneric.hpp"
#include "Iterator.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/Slice.hpp"
#include "leanstore/btree/core/BTreeNode.hpp"
#include "leanstore/buffer-manager/GuardedBufferFrame.hpp"
#include "leanstore/sync/HybridLatch.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/UserThread.hpp"

#include <functional>

#include <sys/syscall.h>

namespace leanstore::storage::btree {

using LeafCallback = std::function<void(GuardedBufferFrame<BTreeNode>& guardedLeaf)>;

// Iterator
class PessimisticIterator : public Iterator {
private:
  friend class BTreeGeneric;

public:
  //! The working btree, all the seek operations are based on this tree.
  BTreeGeneric& mBTree;

  //! The latch mode on the leaf node.
  const LatchMode mMode;

  //! mFuncEnterLeaf is called when the target leaf node is found.
  LeafCallback mFuncEnterLeaf;

  //! mFuncExitLeaf is called when leaving the target leaf node.
  LeafCallback mFuncExitLeaf;

  //! mFuncCleanUp is called when both parent and leaf are unlatched and before
  //! seeking for another key.
  std::function<void()> mFuncCleanUp;

  //! The slot id of the current key in the leaf.
  //! Reset after every leaf change.
  int32_t mSlotId;

  //! Indicates whether the prefix is copied in mBuffer, reset to false after every leaf change.
  bool mIsPrefixCopied;

  //! The latched leaf node of the current key.
  GuardedBufferFrame<BTreeNode> mGuardedLeaf;

  //! The latched parent node of mGuardedLeaf.
  GuardedBufferFrame<BTreeNode> mGuardedParent;

  //! The slot id in mGuardedParent of mGuardedLeaf.
  int32_t mLeafPosInParent;

  //! Used to buffer the key at mSlotId or lower/upper fence keys.
  std::basic_string<uint8_t> mBuffer;

  //! The length of the lower or upper fence key.
  uint16_t mFenceSize;

  //! Tndicates whether the mFenceSize is for lower or upper fence key.
  bool mIsUsingUpperFence;

public:
  PessimisticIterator(BTreeGeneric& tree, const LatchMode mode = LatchMode::kPessimisticShared)
      : mBTree(tree),
        mMode(mode),
        mFuncEnterLeaf(nullptr),
        mFuncExitLeaf(nullptr),
        mFuncCleanUp(nullptr),
        mSlotId(-1),
        mIsPrefixCopied(false),
        mGuardedLeaf(),
        mGuardedParent(),
        mLeafPosInParent(-1),
        mBuffer(),
        mFenceSize(0),
        mIsUsingUpperFence(false) {
  }

  //! move constructor
  PessimisticIterator(PessimisticIterator&& other)
      : mBTree(other.mBTree),
        mMode(other.mMode),
        mFuncEnterLeaf(std::move(other.mFuncEnterLeaf)),
        mFuncExitLeaf(std::move(other.mFuncExitLeaf)),
        mFuncCleanUp(std::move(other.mFuncCleanUp)),
        mSlotId(other.mSlotId),
        mIsPrefixCopied(other.mIsPrefixCopied),
        mGuardedLeaf(std::move(other.mGuardedLeaf)),
        mGuardedParent(std::move(other.mGuardedParent)),
        mLeafPosInParent(other.mLeafPosInParent),
        mBuffer(std::move(other.mBuffer)),
        mFenceSize(other.mFenceSize),
        mIsUsingUpperFence(other.mIsUsingUpperFence) {
    other.SetToInvalid();
    other.mLeafPosInParent = -1;
  }

  //! Seek to the position of the key which = the given key
  void SeekToEqual(Slice key) override {
    seekToTargetPageOnDemand(key);
    mSlotId = mGuardedLeaf->LowerBound<true>(key);
  }

  //! Seek to the position of the first key
  void SeekToFirst() override {
    seekToTargetPage([](GuardedBufferFrame<BTreeNode>&) { return 0; });
    if (mGuardedLeaf->mNumSlots == 0) {
      SetToInvalid();
      return;
    }
    mSlotId = 0;
  }

  //! Seek to the position of the first key which >= the given key
  void SeekToFirstGreaterEqual(Slice key) override {
    seekToTargetPageOnDemand(key);

    mSlotId = mGuardedLeaf->LowerBound<false>(key);
    if (mSlotId < mGuardedLeaf->mNumSlots) {
      return;
    }

    Next();
  }

  //! Whether a next key exists in the tree
  //! @return true if the next key exists, false otherwise
  bool HasNext() override {
    // iterator is not initialized, return false
    if (!Valid()) {
      return false;
    }

    // If we are not at the end of the leaf, return true
    if (mSlotId < mGuardedLeaf->mNumSlots - 1) {
      return true;
    }

    // No more keys in the BTree, return false
    if (mGuardedLeaf->mUpperFence.IsInfinity()) {
      return false;
    }

    return true;
  }

  //! Iterate to the next key in the tree
  void Next() override;

  //! Seek to the position of the last key
  void SeekToLast() override {
    seekToTargetPage([](GuardedBufferFrame<BTreeNode>& parent) { return parent->mNumSlots; });
    if (mGuardedLeaf->mNumSlots == 0) {
      SetToInvalid();
      return;
    }

    mSlotId = mGuardedLeaf->mNumSlots - 1;
  }

  //! Seek to the position of the last key which <= the given key
  void SeekToLastLessEqual(Slice key) override {
    seekToTargetPageOnDemand(key);

    bool isEqual = false;
    mSlotId = mGuardedLeaf->LowerBound<false>(key, &isEqual);
    if (isEqual == true) {
      return;
    }

    if (mSlotId == 0) {
      return Prev();
    }

    mSlotId--;
    return;
  }

  //! Whether a previous key exists in the tree
  //! @return true if the previous key exists, false otherwise
  bool HasPrev() override {
    // iterator is not initialized, return false
    if (!Valid()) {
      return false;
    }

    // If we are not at the beginning of the leaf, return true
    if (mSlotId > 0) {
      return true;
    }

    // No more keys in the BTree, return false
    if (mGuardedLeaf->mLowerFence.IsInfinity()) {
      return false;
    }

    return true;
  }

  //! Iterate to the previous key in the tree
  void Prev() override;

  //! Whether the iterator is valid
  //! @return true if the iterator is pointing to a valid key-value pair, false otherwise
  bool Valid() override {
    return mSlotId != -1;
  }

  void SetToInvalid() {
    mSlotId = -1;
  }

  //! Get the key of the current iterator position, the key is read-only
  //! NOTE: AssembleKey() should be called before calling this function to make sure the key is
  //!       copied to the buffer.
  Slice Key() override {
    LS_DCHECK(mBuffer.size() >= mGuardedLeaf->GetFullKeyLen(mSlotId));
    return Slice(&mBuffer[0], mGuardedLeaf->GetFullKeyLen(mSlotId));
  }

  //! Get the value of the current iterator position, the value is read-only
  Slice Val() override {
    return mGuardedLeaf->Value(mSlotId);
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

  //! Experimental API
  OpCode SeekExactWithHint(Slice key, bool higher = true) {
    if (!Valid()) {
      SeekToEqual(key);
      return Valid() ? OpCode::kOK : OpCode::kNotFound;
    }

    mSlotId = mGuardedLeaf->LinearSearchWithBias<true>(key, mSlotId, higher);
    if (!Valid()) {
      SeekToEqual(key);
      return Valid() ? OpCode::kOK : OpCode::kNotFound;
    }

    return OpCode::kOK;
  }

  void AssembleKey() {
    // extend the buffer if necessary
    if (auto fullKeySize = mGuardedLeaf->GetFullKeyLen(mSlotId); mBuffer.size() < fullKeySize) {
      mBuffer.resize(fullKeySize, 0);
      mIsPrefixCopied = false;
    }

    // copy the key prefix
    if (!mIsPrefixCopied) {
      mGuardedLeaf->CopyPrefix(&mBuffer[0]);
      mIsPrefixCopied = true;
    }

    // copy the remaining key
    mGuardedLeaf->CopyKeyWithoutPrefix(mSlotId, &mBuffer[mGuardedLeaf->mPrefixSize]);
  }

  bool KeyInCurrentNode(Slice key) {
    return mGuardedLeaf->CompareKeyWithBoundaries(key) == 0;
  }

  bool IsLastOne() {
    LS_DCHECK(mSlotId != -1);
    LS_DCHECK(mSlotId != mGuardedLeaf->mNumSlots);
    return (mSlotId + 1) == mGuardedLeaf->mNumSlots;
  }

  void Reset() {
    mGuardedLeaf.unlock();
    SetToInvalid();
    mLeafPosInParent = -1;
    mIsPrefixCopied = false;
  }

protected:
  //! Seek to the target page of the BTree on demand
  void seekToTargetPageOnDemand(Slice key) {
    if (!Valid() || !KeyInCurrentNode(key)) {
      seekToTargetPage([&key](GuardedBufferFrame<BTreeNode>& guardedNode) {
        return guardedNode->LowerBound<false>(key);
      });
    }
  }

  //! Seek to the target page of the BTree
  //! @param childPosGetter a function to get the child position in the parent node
  void seekToTargetPage(std::function<int32_t(GuardedBufferFrame<BTreeNode>&)> childPosGetter);

  void assembleUpperFence() {
    mFenceSize = mGuardedLeaf->mUpperFence.mSize + 1;
    mIsUsingUpperFence = true;
    if (mBuffer.size() < mFenceSize) {
      mBuffer.resize(mFenceSize, 0);
    }
    std::memcpy(mBuffer.data(), mGuardedLeaf->UpperFenceAddr(), mGuardedLeaf->mUpperFence.mSize);
    mBuffer[mFenceSize - 1] = 0;
  }

  Slice assembedFence() {
    LS_DCHECK(mBuffer.size() >= mFenceSize);
    return Slice(&mBuffer[0], mFenceSize);
  }
};

inline void PessimisticIterator::Next() {
  if (!Valid()) {
    return;
  }
  while (true) {
    ENSURE(mGuardedLeaf.mGuard.mState != GuardState::kOptimisticShared);

    // If we are not at the end of the leaf, return the next key in the leaf.
    if ((mSlotId + 1) < mGuardedLeaf->mNumSlots) {
      mSlotId += 1;
      return;
    }

    // No more keys in the BTree, return false
    if (mGuardedLeaf->mUpperFence.IsInfinity()) {
      SetToInvalid();
      return;
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

    if (utils::tlsStore->mStoreOption->mEnableOptimisticScan && mLeafPosInParent != -1) {
      JUMPMU_TRY() {
        if ((mLeafPosInParent + 1) <= mGuardedParent->mNumSlots) {
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

          if (mGuardedLeaf->mNumSlots == 0) {
            JUMPMU_CONTINUE;
          }
          ENSURE(mSlotId < mGuardedLeaf->mNumSlots);
          JUMPMU_RETURN;
        }
      }
      JUMPMU_CATCH() {
      }
    }

    mGuardedParent.unlock();
    Slice fenceKey = assembedFence();
    seekToTargetPage([&fenceKey](GuardedBufferFrame<BTreeNode>& guardedNode) {
      return guardedNode->LowerBound<false>(fenceKey);
    });

    if (mGuardedLeaf->mNumSlots == 0) {
      SetCleanUpCallback([&, toMerge = mGuardedLeaf.mBf]() {
        JUMPMU_TRY() {
          TXID sysTxId = mBTree.mStore->AllocSysTxTs();
          mBTree.TryMergeMayJump(sysTxId, *toMerge, true);
        }
        JUMPMU_CATCH() {
        }
      });
      continue;
    }
    mSlotId = mGuardedLeaf->LowerBound<false>(assembedFence());
    if (mSlotId == mGuardedLeaf->mNumSlots) {
      continue;
    }
    return;
  }
}

inline void PessimisticIterator::Prev() {
  while (true) {
    ENSURE(mGuardedLeaf.mGuard.mState != GuardState::kOptimisticShared);
    // If we are not at the beginning of the leaf, return the previous key
    // in the leaf.
    if (mSlotId > 0) {
      mSlotId -= 1;
      return;
    }

    // No more keys in the BTree, return false
    if (mGuardedLeaf->mLowerFence.IsInfinity()) {
      SetToInvalid();
      return;
    }

    // Construct the previous key (upper bound)
    mFenceSize = mGuardedLeaf->mLowerFence.mSize;
    mIsUsingUpperFence = false;
    if (mBuffer.size() < mFenceSize) {
      mBuffer.resize(mFenceSize, 0);
    }
    std::memcpy(&mBuffer[0], mGuardedLeaf->LowerFenceAddr(), mFenceSize);

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

    if (utils::tlsStore->mStoreOption->mEnableOptimisticScan && mLeafPosInParent != -1) {
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
          mSlotId = mGuardedLeaf->mNumSlots - 1;
          mIsPrefixCopied = false;

          if (mFuncEnterLeaf != nullptr) {
            mFuncEnterLeaf(mGuardedLeaf);
          }

          if (mGuardedLeaf->mNumSlots == 0) {
            JUMPMU_CONTINUE;
          }
          JUMPMU_RETURN;
        }
      }
      JUMPMU_CATCH() {
      }
    }

    // Construct the next key (lower bound)
    Slice fenceKey = assembedFence();
    seekToTargetPage([&fenceKey](GuardedBufferFrame<BTreeNode>& guardedNode) {
      return guardedNode->LowerBound<false>(fenceKey);
    });

    if (mGuardedLeaf->mNumSlots == 0) {
      continue;
    }
    bool isEqual = false;
    mSlotId = mGuardedLeaf->LowerBound<false>(assembedFence(), &isEqual);
    if (isEqual) {
      return;
    }

    if (mSlotId > 0) {
      mSlotId -= 1;
    } else {
      continue;
    }
  }
}

inline void PessimisticIterator::seekToTargetPage(
    std::function<int32_t(GuardedBufferFrame<BTreeNode>&)> childPosGetter) {
  if (mMode != LatchMode::kPessimisticShared && mMode != LatchMode::kPessimisticExclusive) {
    Log::Fatal("Unsupported latch mode: {}", uint64_t(mMode));
  }

  while (true) {
    mLeafPosInParent = -1;
    JUMPMU_TRY() {
      // lock meta node
      mGuardedParent =
          GuardedBufferFrame<BTreeNode>(mBTree.mStore->mBufferManager.get(), mBTree.mMetaNodeSwip);
      mGuardedLeaf.unlock();

      // lock root node
      mGuardedLeaf = GuardedBufferFrame<BTreeNode>(
          mBTree.mStore->mBufferManager.get(), mGuardedParent, mGuardedParent->mRightMostChildSwip);

      for (uint16_t level = 0; !mGuardedLeaf->mIsLeaf; level++) {
        mLeafPosInParent = childPosGetter(mGuardedLeaf);
        auto* childSwip = mGuardedLeaf->ChildSwipIncludingRightMost(mLeafPosInParent);

        mGuardedParent = std::move(mGuardedLeaf);
        if (level == mBTree.mHeight - 1) {
          mGuardedLeaf = GuardedBufferFrame<BTreeNode>(mBTree.mStore->mBufferManager.get(),
                                                       mGuardedParent, *childSwip, mMode);
        } else {
          // latch the middle node optimistically
          mGuardedLeaf =
              GuardedBufferFrame<BTreeNode>(mBTree.mStore->mBufferManager.get(), mGuardedParent,
                                            *childSwip, LatchMode::kOptimisticSpin);
        }
      }

      mGuardedParent.unlock();
      if (mMode == LatchMode::kPessimisticExclusive) {
        mGuardedLeaf.ToExclusiveMayJump();
      } else {
        mGuardedLeaf.ToSharedMayJump();
      }

      mIsPrefixCopied = false;
      if (mFuncEnterLeaf != nullptr) {
        mFuncEnterLeaf(mGuardedLeaf);
      }
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
    }
  }
}

} // namespace leanstore::storage::btree
