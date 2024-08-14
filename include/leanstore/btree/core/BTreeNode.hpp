#pragma once

#include "leanstore/buffer-manager/BufferFrame.hpp"
#include "leanstore/buffer-manager/GuardedBufferFrame.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/UserThread.hpp"

#include <cstring>

namespace leanstore::storage::btree {

class BTreeNode;
using HeadType = uint32_t;

class BTreeNodeHeader {
public:
  static const uint16_t sHintCount = 16;

  struct SeparatorInfo {
    //! The full length of the separator key.
    uint16_t mSize;

    //! The slot id of the separator key.
    uint16_t mSlotId;

    //! Indicates whether the separator key is truncated.
    bool mTrunc;

    SeparatorInfo(uint16_t size = 0, uint16_t slotId = 0, bool trunc = false)
        : mSize(size),
          mSlotId(slotId),
          mTrunc(trunc) {
    }
  };

  struct FenceKey {
    uint16_t mOffset;

    uint16_t mLength;
  };

public:
  //! The swip of the right-most child.
  //! TODO(zz-jason): can it be moved to the slot array?
  Swip mRightMostChildSwip = nullptr;

  //! The lower fence of the node. Exclusive.
  FenceKey mLowerFence = {0, 0};

  //! The upper fence of the node. Inclusive.
  FenceKey mUpperFence = {0, 0};

  //! The number of seperators. #slots = #seps + 1.
  //! The first mNumSeps children are stored in the payload, while the last child are stored in
  //! upper.
  uint16_t mNumSeps = 0;

  //! Indicates whether this node is leaf node without any child.
  bool mIsLeaf;

  //! Indicates the space used for the node.
  //! @note !!! does not include the header, but includes fences !!!
  uint16_t mSpaceUsed = 0;

  //! Data offset of the current slot in the BTreeNode. The BTreeNode is organized as follows:
  //!
  //!   | BTreeNodeHeader | info of slot 0..N |  ... | data of slot N..0 |
  //!
  //! It's initialized to the total size of the btree node, reduced and assigned to each slot when
  //! the number of slots is increasing.
  uint16_t mDataOffset;

  uint16_t mPrefixSize = 0;

  uint32_t mHint[sHintCount];

  //! Needed for GC
  bool mHasGarbage = false;

public:
  BTreeNodeHeader(bool isLeaf, uint16_t size) : mIsLeaf(isLeaf), mDataOffset(size) {
  }

  ~BTreeNodeHeader() {
  }

public:
  uint8_t* RawPtr() {
    return reinterpret_cast<uint8_t*>(this);
  }

  bool IsInner() {
    return !mIsLeaf;
  }

  Slice GetLowerFence() {
    return Slice(GetLowerFenceKey(), mLowerFence.mLength);
  }

  uint8_t* GetLowerFenceKey() {
    return mLowerFence.mOffset ? RawPtr() + mLowerFence.mOffset : nullptr;
  }

  Slice GetUpperFence() {
    return Slice(GetUpperFenceKey(), mUpperFence.mLength);
  }

  uint8_t* GetUpperFenceKey() {
    return mUpperFence.mOffset ? RawPtr() + mUpperFence.mOffset : nullptr;
  }

  bool IsUpperFenceInfinity() {
    return !mUpperFence.mOffset;
  }

  bool IsLowerFenceInfinity() {
    return !mLowerFence.mOffset;
  }
};

class BTreeNode : public BTreeNodeHeader {
public:
  //! The slot inside a btree node. Slot records the metadata for the key-value position inside a
  //! page. Common prefix among all keys are removed in a btree node. Slot key-value layout:
  //!  | key without prefix | value |
  struct __attribute__((packed)) Slot {

    //! Data offset of the slot, also the offset of the slot key
    uint16_t mOffset;

    //! Slot key size
    uint16_t mKeySizeWithoutPrefix;

    //! Slot value size
    uint16_t mValSize;

    //! The key header, used for improve key comparation performance
    union {
      HeadType mHead;
      uint8_t mHeadBytes[4];
    };
  };

  //! The slot array, which stores all the key-value positions inside a page.
  Slot mSlot[];

  //! Creates a BTreeNode. Since BTreeNode creations and utilizations are critical, please use
  //! ExclusiveGuardedBufferFrame::InitPayload() or BTreeNode::Init() to construct a BTreeNode on an
  //! existing buffer which has at least BTreeNode::Size() bytes:
  //! 1. ExclusiveGuardedBufferFrame::InitPayload() creates a BTreeNode on the holding BufferFrame.
  //! 2. BTreeNode::Init(): creates a BTreeNode on the providing buffer. The size of the underlying
  //!    buffer to store a BTreeNode can be obtained through BTreeNode::Size()
  BTreeNode(bool isLeaf) : BTreeNodeHeader(isLeaf, BTreeNode::Size()) {
  }

  uint16_t FreeSpace() {
    return mDataOffset - (reinterpret_cast<uint8_t*>(mSlot + mNumSeps) - RawPtr());
  }

  uint16_t FreeSpaceAfterCompaction() {
    return BTreeNode::Size() - (reinterpret_cast<uint8_t*>(mSlot + mNumSeps) - RawPtr()) -
           mSpaceUsed;
  }

  double FillFactorAfterCompaction() {
    return (1 - (FreeSpaceAfterCompaction() * 1.0 / BTreeNode::Size()));
  }

  bool HasEnoughSpaceFor(uint32_t spaceNeeded) {
    return (spaceNeeded <= FreeSpace() || spaceNeeded <= FreeSpaceAfterCompaction());
  }

  // ATTENTION: this method has side effects !
  bool RequestSpaceFor(uint16_t spaceNeeded) {
    if (spaceNeeded <= FreeSpace())
      return true;
    if (spaceNeeded <= FreeSpaceAfterCompaction()) {
      Compactify();
      return true;
    }
    return false;
  }

  Slice KeyWithoutPrefix(uint16_t slotId) {
    return Slice(KeyDataWithoutPrefix(slotId), KeySizeWithoutPrefix(slotId));
  }

  uint8_t* KeyDataWithoutPrefix(uint16_t slotId) {
    return RawPtr() + mSlot[slotId].mOffset;
  }

  uint16_t KeySizeWithoutPrefix(uint16_t slotId) {
    return mSlot[slotId].mKeySizeWithoutPrefix;
  }

  Slice Value(uint16_t slotId) {
    return Slice(ValData(slotId), ValSize(slotId));
  }

  // Each slot is composed of:
  // key (mKeySizeWithoutPrefix), payload (mValSize)
  uint8_t* ValData(uint16_t slotId) {
    return RawPtr() + mSlot[slotId].mOffset + mSlot[slotId].mKeySizeWithoutPrefix;
  }

  uint16_t ValSize(uint16_t slotId) {
    return mSlot[slotId].mValSize;
  }

  Swip* ChildSwipIncludingRightMost(uint16_t slotId) {
    if (slotId == mNumSeps) {
      return &mRightMostChildSwip;
    }

    return reinterpret_cast<Swip*>(ValData(slotId));
  }

  Swip* ChildSwip(uint16_t slotId) {
    LS_DCHECK(slotId < mNumSeps);
    return reinterpret_cast<Swip*>(ValData(slotId));
  }

  uint16_t GetKVConsumedSpace(uint16_t slotId) {
    return sizeof(Slot) + KeySizeWithoutPrefix(slotId) + ValSize(slotId);
  }

  // Attention: the caller has to hold a copy of the existing payload
  void ShortenPayload(uint16_t slotId, uint16_t targetSize) {
    LS_DCHECK(targetSize <= mSlot[slotId].mValSize);
    const uint16_t freeSpace = mSlot[slotId].mValSize - targetSize;
    mSpaceUsed -= freeSpace;
    mSlot[slotId].mValSize = targetSize;
  }

  bool CanExtendPayload(uint16_t slotId, uint16_t targetSize) {
    LS_DCHECK(targetSize > ValSize(slotId),
              "Target size must be larger than current size, "
              "targetSize={}, currentSize={}",
              targetSize, ValSize(slotId));

    const uint16_t extraSpaceNeeded = targetSize - ValSize(slotId);
    return FreeSpaceAfterCompaction() >= extraSpaceNeeded;
  }

  //! Move key-value pair to a new location
  void ExtendPayload(uint16_t slotId, uint16_t targetSize) {
    LS_DCHECK(CanExtendPayload(slotId, targetSize),
              "ExtendPayload failed, not enough space in the current node, "
              "slotId={}, targetSize={}, FreeSpace={}, currentSize={}",
              slotId, targetSize, FreeSpaceAfterCompaction(), ValSize(slotId));
    auto keySizeWithoutPrefix = KeySizeWithoutPrefix(slotId);
    const uint16_t oldTotalSize = keySizeWithoutPrefix + ValSize(slotId);
    const uint16_t newTotalSize = keySizeWithoutPrefix + targetSize;

    // store the keyWithoutPrefix temporarily before moving the payload
    uint8_t copiedKey[keySizeWithoutPrefix];
    std::memcpy(copiedKey, KeyDataWithoutPrefix(slotId), keySizeWithoutPrefix);

    // release the old space occupied by the payload (keyWithoutPrefix + value)
    mSpaceUsed -= oldTotalSize;
    mDataOffset += oldTotalSize;

    mSlot[slotId].mValSize = 0;
    mSlot[slotId].mKeySizeWithoutPrefix = 0;
    if (FreeSpace() < newTotalSize) {
      Compactify();
    }
    LS_DCHECK(FreeSpace() >= newTotalSize);
    mSpaceUsed += newTotalSize;
    mDataOffset -= newTotalSize;
    mSlot[slotId].mOffset = mDataOffset;
    mSlot[slotId].mKeySizeWithoutPrefix = keySizeWithoutPrefix;
    mSlot[slotId].mValSize = targetSize;
    std::memcpy(KeyDataWithoutPrefix(slotId), copiedKey, keySizeWithoutPrefix);
  }

  Slice KeyPrefix() {
    return Slice(GetLowerFenceKey(), mPrefixSize);
  }

  uint8_t* GetPrefix() {
    return GetLowerFenceKey();
  }

  void CopyPrefix(uint8_t* out) {
    memcpy(out, GetLowerFenceKey(), mPrefixSize);
  }

  void CopyKeyWithoutPrefix(uint16_t slotId, uint8_t* dest) {
    auto key = KeyWithoutPrefix(slotId);
    memcpy(dest, key.data(), key.size());
  }

  uint16_t GetFullKeyLen(uint16_t slotId) {
    return mPrefixSize + KeySizeWithoutPrefix(slotId);
  }

  void CopyFullKey(uint16_t slotId, uint8_t* dest) {
    memcpy(dest, GetPrefix(), mPrefixSize);
    auto remaining = KeyWithoutPrefix(slotId);
    memcpy(dest + mPrefixSize, remaining.data(), remaining.size());
  }

  void MakeHint() {
    uint16_t dist = mNumSeps / (sHintCount + 1);
    for (uint16_t i = 0; i < sHintCount; i++)
      mHint[i] = mSlot[dist * (i + 1)].mHead;
  }

  int32_t CompareKeyWithBoundaries(Slice key);

  void SearchHint(HeadType keyHead, uint16_t& lowerOut, uint16_t& upperOut);

  template <bool equality_only = false>
  int16_t LinearSearchWithBias(Slice key, uint16_t startPos, bool higher = true);

  //! Returns the position where the key[pos] (if exists) >= key (not less than the given key):
  //! (2) (2) (1) ->
  //! (2) (2) (1) (0) ->
  //! (2) (2) (1) (0) (0) ->
  //! ...  ->
  //! (2) (2) (2)
  template <bool equalityOnly = false>
  int16_t LowerBound(Slice key, bool* isEqual = nullptr);

  void UpdateHint(uint16_t slotId);

  int16_t InsertDoNotCopyPayload(Slice key, uint16_t valSize, int32_t pos = -1);

  int32_t Insert(Slice key, Slice val);

  uint16_t SpaceNeeded(uint16_t keySize, uint16_t valSize) {
    return SpaceNeeded(keySize, valSize, mPrefixSize);
  }

  bool CanInsert(uint16_t keySize, uint16_t valSize) {
    return HasEnoughSpaceFor(SpaceNeeded(keySize, valSize));
  }

  bool PrepareInsert(uint16_t keySize, uint16_t valSize) {
    return RequestSpaceFor(SpaceNeeded(keySize, valSize));
  }

  void Compactify();

  //! merge right node into this node
  uint32_t MergeSpaceUpperBound(ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedRight);

  uint32_t SpaceUsedBySlot(uint16_t slotId) {
    return sizeof(BTreeNode::Slot) + KeySizeWithoutPrefix(slotId) + ValSize(slotId);
  }

  // NOLINTNEXTLINE
  bool merge(uint16_t slotId, ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedParent,
             ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedRight);

  //! store key/value pair at slotId
  void StoreKeyValue(uint16_t slotId, Slice key, Slice val);

  // ATTENTION: dstSlot then srcSlot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  void CopyKeyValueRange(BTreeNode* dst, uint16_t dstSlot, uint16_t srcSlot, uint16_t count);

  void CopyKeyValue(uint16_t srcSlot, BTreeNode* dst, uint16_t dstSlot);

  void InsertFence(FenceKey& fk, Slice key);

  void SetFences(Slice lowerKey, Slice upperKey);

  void Split(ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedParent,
             ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedNewLeft,
             const BTreeNode::SeparatorInfo& sepInfo);

  uint16_t CommonPrefix(uint16_t aPos, uint16_t bPos);

  SeparatorInfo FindSep();

  Swip& LookupInner(Slice key);

  // Not synchronized or todo section
  bool RemoveSlot(uint16_t slotId);

  bool Remove(Slice key);

  void Reset();

private:
  void generateSeparator(const SeparatorInfo& sepInfo, uint8_t* sepKey) {
    // prefix
    memcpy(sepKey, GetLowerFenceKey(), mPrefixSize);

    if (sepInfo.mTrunc) {
      memcpy(sepKey + mPrefixSize, KeyDataWithoutPrefix(sepInfo.mSlotId + 1),
             sepInfo.mSize - mPrefixSize);
    } else {
      memcpy(sepKey + mPrefixSize, KeyDataWithoutPrefix(sepInfo.mSlotId),
             sepInfo.mSize - mPrefixSize);
    }
  }

  bool shrinkSearchRange(uint16_t& lower, uint16_t& upper, Slice key) {
    auto mid = ((upper - lower) / 2) + lower;
    auto cmp = CmpKeys(key, KeyWithoutPrefix(mid));
    if (cmp < 0) {
      upper = mid;
      return false;
    }

    if (cmp > 0) {
      lower = mid + 1;
      return false;
    }

    lower = mid;
    upper = mid;
    return true;
  }

  bool shrinkSearchRangeWithHead(uint16_t& lower, uint16_t& upper, Slice key, HeadType keyHead) {
    auto mid = ((upper - lower) / 2) + lower;
    auto midHead = mSlot[mid].mHead;
    auto midSize = mSlot[mid].mKeySizeWithoutPrefix;
    if ((keyHead < midHead) || (keyHead == midHead && midSize <= 4 && key.size() < midSize)) {
      upper = mid;
      return false;
    }

    if ((keyHead > midHead) || (keyHead == midHead && midSize <= 4 && key.size() > midSize)) {
      lower = mid + 1;
      return false;
    }

    // now we must have: keyHead == midHead
    if (midSize <= 4 && key.size() == midSize) {
      lower = mid;
      upper = mid;
      return true;
    }

    // now we must have: keyHead == midHead && midSize > 4
    // fallback to the normal compare
    return shrinkSearchRange(lower, upper, key);
  }

public:
  static HeadType Head(Slice key);

  static int32_t CmpKeys(Slice lhs, Slice rhs);

  static uint16_t SpaceNeeded(uint16_t keySize, uint16_t valSize, uint16_t prefixSize) {
    return sizeof(Slot) + (keySize - prefixSize) + valSize;
  }

  template <typename... Args>
  static BTreeNode* Init(void* addr, Args&&... args) {
    return new (addr) BTreeNode(std::forward<Args>(args)...);
  }

  static uint16_t Size() {
    return static_cast<uint16_t>(utils::tlsStore->mStoreOption.mPageSize - sizeof(Page));
  }

  static uint16_t UnderFullSize() {
    return BTreeNode::Size() * 0.6;
  }
};

template <bool equality_only>
int16_t BTreeNode::LinearSearchWithBias(Slice key, uint16_t startPos, bool higher) {
  if (key.size() < mPrefixSize || (bcmp(key.data(), GetLowerFenceKey(), mPrefixSize) != 0)) {
    return -1;
  }

  LS_DCHECK(key.size() >= mPrefixSize && bcmp(key.data(), GetLowerFenceKey(), mPrefixSize) == 0);

  // the compared key has the same prefix
  key.remove_prefix(mPrefixSize);

  if (higher) {
    auto cur = startPos + 1;
    for (; cur < mNumSeps; cur++) {
      if (CmpKeys(key, KeyWithoutPrefix(cur)) == 0) {
        return cur;
      }
      break;
    }
    return equality_only ? -1 : cur;
  }

  auto cur = startPos - 1;
  for (; cur >= 0; cur--) {
    if (CmpKeys(key, KeyWithoutPrefix(cur)) == 0) {
      return cur;
    }
    break;
  }
  return equality_only ? -1 : cur;
}

template <bool equalityOnly>
int16_t BTreeNode::LowerBound(Slice key, bool* isEqual) {
  if (isEqual != nullptr && mIsLeaf) {
    *isEqual = false;
  }

  // compare prefix firstly
  if (equalityOnly) {
    if ((key.size() < mPrefixSize) || (bcmp(key.data(), GetLowerFenceKey(), mPrefixSize) != 0)) {
      return -1;
    }
  } else if (mPrefixSize != 0) {
    Slice keyPrefix(key.data(), std::min<uint16_t>(key.size(), mPrefixSize));
    Slice lowerFencePrefix(GetLowerFenceKey(), mPrefixSize);
    int cmpPrefix = CmpKeys(keyPrefix, lowerFencePrefix);
    if (cmpPrefix < 0) {
      return 0;
    }

    if (cmpPrefix > 0) {
      return mNumSeps;
    }
  }

  // the compared key has the same prefix
  key.remove_prefix(mPrefixSize);
  uint16_t lower = 0;
  uint16_t upper = mNumSeps;
  HeadType keyHead = Head(key);
  SearchHint(keyHead, lower, upper);
  while (lower < upper) {
    bool foundEqual(false);
    if (utils::tlsStore->mStoreOption.mEnableHeadOptimization) {
      foundEqual = shrinkSearchRangeWithHead(lower, upper, key, keyHead);
    } else {
      foundEqual = shrinkSearchRange(lower, upper, key);
    }
    if (foundEqual) {
      if (isEqual != nullptr && mIsLeaf) {
        *isEqual = true;
      }
      return lower;
    }
  }

  return equalityOnly ? -1 : lower;
}

} // namespace leanstore::storage::btree
