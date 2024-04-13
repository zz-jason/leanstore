#pragma once

#include "buffer-manager/BufferFrame.hpp"
#include "buffer-manager/GuardedBufferFrame.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "utils/UserThread.hpp"

#include <glog/logging.h>
#include <rapidjson/document.h>

#include <algorithm>
#include <cstring>

using namespace std;
using namespace leanstore::storage;

namespace leanstore::storage::btree {

class BTreeNode;
using HeadType = uint32_t;

class BTreeNodeHeader {
public:
  static const uint16_t sHintCount = 16;

  struct SeparatorInfo {
    /// The full length of the seperator key.
    uint16_t mSize;

    /// The slot id of the seperator key.
    uint16_t mSlotId;

    // TODO: ???
    bool trunc;

    SeparatorInfo(uint16_t size = 0, uint16_t slotId = 0, bool trunc = false)
        : mSize(size),
          mSlotId(slotId),
          trunc(trunc) {
    }
  };

  struct FenceKey {
    uint16_t offset;
    uint16_t length;
  };

public:
  /// The swip of the right-most child.
  Swip mRightMostChildSwip = nullptr;

  /// The lower fence of the node. Exclusive.
  FenceKey mLowerFence = {0, 0};

  /// The upper fence of the node. Inclusive.
  FenceKey mUpperFence = {0, 0};

  /// The number of seperators. #slots = #seps + 1.
  /// The first mNumSeps children are stored in the payload, while the last
  /// child are stored in upper.
  uint16_t mNumSeps = 0;

  /// Indicates whether this node is leaf node without any child.
  bool mIsLeaf;

  /// Indicates the space used for the node.
  /// @note !!! does not include the header, but includes fences !!!
  uint16_t mSpaceUsed = 0;

  /// Data offset of the current slot in the BTreeNode. The BTreeNode is
  /// organized as follows:
  ///
  ///   | BTreeNodeHeader | info of slot 0..N |  ... | data of slot N..0 |
  ///
  /// It's initialized to the total size of the btree node, reduced and assigned
  /// to each slot when the number of slots is increasing.
  uint16_t mDataOffset;

  uint16_t mPrefixSize = 0;

  uint32_t hint[sHintCount];

  /// Needed for GC
  bool mHasGarbage = false;

public:
  BTreeNodeHeader(bool isLeaf, uint16_t size)
      : mIsLeaf(isLeaf),
        mDataOffset(size) {
  }

  ~BTreeNodeHeader() {
  }

public:
  inline uint8_t* RawPtr() {
    return reinterpret_cast<uint8_t*>(this);
  }

  inline bool isInner() {
    return !mIsLeaf;
  }

  inline Slice GetLowerFence() {
    return Slice(getLowerFenceKey(), mLowerFence.length);
  }

  inline uint8_t* getLowerFenceKey() {
    return mLowerFence.offset ? RawPtr() + mLowerFence.offset : nullptr;
  }

  inline Slice GetUpperFence() {
    return Slice(getUpperFenceKey(), mUpperFence.length);
  }

  inline uint8_t* getUpperFenceKey() {
    return mUpperFence.offset ? RawPtr() + mUpperFence.offset : nullptr;
  }

  inline bool isUpperFenceInfinity() {
    return !mUpperFence.offset;
  }

  inline bool isLowerFenceInfinity() {
    return !mLowerFence.offset;
  }
};

class BTreeNode : public BTreeNodeHeader {
public:
  struct __attribute__((packed)) Slot {
    // Layout:  key wihtout prefix | Payload
    uint16_t offset;
    uint16_t mKeySizeWithoutPrefix;
    uint16_t mValSize;
    union {
      HeadType head;
      uint8_t mHeadBytes[4];
    };
  };

public:
  Slot slot[];

public:
  /// Creates a BTreeNode. Since BTreeNode creations and utilizations are
  /// critical, please use ExclusiveGuardedBufferFrame::InitPayload() or
  /// BTreeNode::Init() to construct a BTreeNode on an existing buffer which has
  /// at least BTreeNode::Size() bytes:
  /// 1. ExclusiveGuardedBufferFrame::InitPayload() creates a BTreeNode on the
  ///    holding BufferFrame.
  /// 2. BTreeNode::Init(): creates a BTreeNode on the providing buffer. The
  ///    size of the underlying buffer to store a BTreeNode can be obtained
  ///    through BTreeNode::Size()
  BTreeNode(bool isLeaf) : BTreeNodeHeader(isLeaf, BTreeNode::Size()) {
  }

public:
  uint16_t freeSpace() {
    return mDataOffset -
           (reinterpret_cast<uint8_t*>(slot + mNumSeps) - RawPtr());
  }

  uint16_t FreeSpaceAfterCompaction() {
    return BTreeNode::Size() -
           (reinterpret_cast<uint8_t*>(slot + mNumSeps) - RawPtr()) -
           mSpaceUsed;
  }

  double fillFactorAfterCompaction() {
    return (1 - (FreeSpaceAfterCompaction() * 1.0 / BTreeNode::Size()));
  }

  bool hasEnoughSpaceFor(uint32_t space_needed) {
    return (space_needed <= freeSpace() ||
            space_needed <= FreeSpaceAfterCompaction());
  }

  // ATTENTION: this method has side effects !
  bool requestSpaceFor(uint16_t spaceNeeded) {
    if (spaceNeeded <= freeSpace())
      return true;
    if (spaceNeeded <= FreeSpaceAfterCompaction()) {
      compactify();
      return true;
    }
    return false;
  }

  inline Slice KeyWithoutPrefix(uint16_t slotId) {
    return Slice(KeyDataWithoutPrefix(slotId), KeySizeWithoutPrefix(slotId));
  }

  inline uint8_t* KeyDataWithoutPrefix(uint16_t slotId) {
    return RawPtr() + slot[slotId].offset;
  }

  inline uint16_t KeySizeWithoutPrefix(uint16_t slotId) {
    return slot[slotId].mKeySizeWithoutPrefix;
  }

  inline Slice Value(uint16_t slotId) {
    return Slice(ValData(slotId), ValSize(slotId));
  }

  // Each slot is composed of:
  // key (mKeySizeWithoutPrefix), payload (mValSize)
  inline uint8_t* ValData(uint16_t slotId) {
    return RawPtr() + slot[slotId].offset + slot[slotId].mKeySizeWithoutPrefix;
  }

  inline uint16_t ValSize(uint16_t slotId) {
    return slot[slotId].mValSize;
  }

  inline Swip* ChildSwipIncludingRightMost(uint16_t slotId) {
    if (slotId == mNumSeps) {
      return &mRightMostChildSwip;
    }

    return reinterpret_cast<Swip*>(ValData(slotId));
  }

  inline Swip* ChildSwip(uint16_t slotId) {
    DCHECK(slotId < mNumSeps);
    return reinterpret_cast<Swip*>(ValData(slotId));
  }

  inline uint16_t getKVConsumedSpace(uint16_t slotId) {
    return sizeof(Slot) + KeySizeWithoutPrefix(slotId) + ValSize(slotId);
  }

  // Attention: the caller has to hold a copy of the existing payload
  inline void shortenPayload(uint16_t slotId, uint16_t targetSize) {
    DCHECK(targetSize <= slot[slotId].mValSize);
    const uint16_t freeSpace = slot[slotId].mValSize - targetSize;
    mSpaceUsed -= freeSpace;
    slot[slotId].mValSize = targetSize;
  }

  inline bool CanExtendPayload(uint16_t slotId, uint16_t targetSize) {
    DCHECK(targetSize > ValSize(slotId))
        << "Target size must be larger than current size"
        << ", targetSize=" << targetSize << ", currentSize=" << ValSize(slotId);

    const uint16_t extraSpaceNeeded = targetSize - ValSize(slotId);
    return FreeSpaceAfterCompaction() >= extraSpaceNeeded;
  }

  // Move key | payload to a new location
  void ExtendPayload(uint16_t slotId, uint16_t targetSize) {
    DCHECK(CanExtendPayload(slotId, targetSize))
        << "ExtendPayload failed, not enough space in the current node"
        << ", slotId=" << slotId << ", targetSize=" << targetSize
        << ", freeSpace=" << FreeSpaceAfterCompaction()
        << ", currentSize=" << ValSize(slotId);
    // const uint16_t extraSpaceNeeded = targetSize - ValSize(slotId);
    // requestSpaceFor(extraSpaceNeeded);

    auto keySizeWithoutPrefix = KeySizeWithoutPrefix(slotId);
    const uint16_t oldTotalSize = keySizeWithoutPrefix + ValSize(slotId);
    const uint16_t newTotalSize = keySizeWithoutPrefix + targetSize;

    // store the keyWithoutPrefix temporarily before moving the payload
    uint8_t copiedKey[keySizeWithoutPrefix];
    std::memcpy(copiedKey, KeyDataWithoutPrefix(slotId), keySizeWithoutPrefix);

    // release the old space occupied by the payload (keyWithoutPrefix + value)
    mSpaceUsed -= oldTotalSize;
    mDataOffset += oldTotalSize;

    slot[slotId].mValSize = 0;
    slot[slotId].mKeySizeWithoutPrefix = 0;
    if (freeSpace() < newTotalSize) {
      compactify();
    }
    DCHECK(freeSpace() >= newTotalSize);
    mSpaceUsed += newTotalSize;
    mDataOffset -= newTotalSize;
    slot[slotId].offset = mDataOffset;
    slot[slotId].mKeySizeWithoutPrefix = keySizeWithoutPrefix;
    slot[slotId].mValSize = targetSize;
    std::memcpy(KeyDataWithoutPrefix(slotId), copiedKey, keySizeWithoutPrefix);
  }

  inline Slice KeyPrefix() {
    return Slice(getLowerFenceKey(), mPrefixSize);
  }

  inline uint8_t* getPrefix() {
    return getLowerFenceKey();
  }

  inline void copyPrefix(uint8_t* out) {
    memcpy(out, getLowerFenceKey(), mPrefixSize);
  }

  inline void copyKeyWithoutPrefix(uint16_t slotId, uint8_t* out_after_prefix) {
    auto key = KeyWithoutPrefix(slotId);
    memcpy(out_after_prefix, key.data(), key.size());
  }

  inline uint16_t getFullKeyLen(uint16_t slotId) {
    return mPrefixSize + KeySizeWithoutPrefix(slotId);
  }

  inline void copyFullKey(uint16_t slotId, uint8_t* out) {
    memcpy(out, getPrefix(), mPrefixSize);
    auto remaining = KeyWithoutPrefix(slotId);
    memcpy(out + mPrefixSize, remaining.data(), remaining.size());
  }

  inline static int32_t CmpKeys(Slice lhs, Slice rhs) {
    auto minLength = min(lhs.size(), rhs.size());
    if (minLength < 4) {
      for (size_t i = 0; i < minLength; ++i) {
        if (lhs[i] != rhs[i]) {
          return lhs[i] < rhs[i] ? -1 : 1;
        }
      }
      return (lhs.size() - rhs.size());
    }

    int c = memcmp(lhs.data(), rhs.data(), minLength);
    if (c != 0) {
      return c;
    }
    return (lhs.size() - rhs.size());
  }

  inline static HeadType head(Slice key) {
    switch (key.size()) {
    case 0: {
      return 0;
    }
    case 1: {
      return static_cast<uint32_t>(key[0]) << 24;
    }
    case 2: {
      const uint16_t bigEndianVal =
          *reinterpret_cast<const uint16_t*>(key.data());
      const uint16_t littleEndianVal = __builtin_bswap16(bigEndianVal);
      return static_cast<uint32_t>(littleEndianVal) << 16;
    }
    case 3: {
      const uint16_t bigEndianVal =
          *reinterpret_cast<const uint16_t*>(key.data());
      const uint16_t littleEndianVal = __builtin_bswap16(bigEndianVal);
      return (static_cast<uint32_t>(littleEndianVal) << 16) |
             (static_cast<uint32_t>(key[2]) << 8);
    }
    default: {
      return __builtin_bswap32(*reinterpret_cast<const uint32_t*>(key.data()));
    }
    }

    return __builtin_bswap32(*reinterpret_cast<const uint32_t*>(key.data()));
  }

  void makeHint();

  int32_t compareKeyWithBoundaries(Slice key);

  void searchHint(HeadType key_head, uint16_t& lower_out, uint16_t& upper_out) {
    if (mNumSeps > sHintCount * 2) {
      if (utils::tlsStore->mStoreOption.mBTreeHints == 2) {
#ifdef __AVX512F__
        const uint16_t dist = mNumSeps / (sHintCount + 1);
        uint16_t pos, pos2;
        __m512i key_head_reg = _mm512_set1_epi32(key_head);
        __m512i chunk = _mm512_loadu_si512(hint);
        __mmask16 compareMask = _mm512_cmpge_epu32_mask(chunk, key_head_reg);
        if (compareMask == 0)
          return;
        pos = __builtin_ctz(compareMask);
        lower_out = pos * dist;
        // -------------------------------------------------------------------------------------
        for (pos2 = pos; pos2 < sHintCount; pos2++) {
          if (hint[pos2] != key_head) {
            break;
          }
        }
        if (pos2 < sHintCount) {
          upper_out = (pos2 + 1) * dist;
        }
#else
        LOG(ERROR) << "Search hint with AVX512 failed: __AVX512F__ not found";
#endif
      } else if (utils::tlsStore->mStoreOption.mBTreeHints == 1) {
        const uint16_t dist = mNumSeps / (sHintCount + 1);
        uint16_t pos, pos2;

        for (pos = 0; pos < sHintCount; pos++) {
          if (hint[pos] >= key_head) {
            break;
          }
        }
        for (pos2 = pos; pos2 < sHintCount; pos2++) {
          if (hint[pos2] != key_head) {
            break;
          }
        }

        lower_out = pos * dist;
        if (pos2 < sHintCount) {
          upper_out = (pos2 + 1) * dist;
        }

        if (mIsLeaf) {
          WorkerCounters::MyCounters().dt_researchy[0][0]++;
          WorkerCounters::MyCounters().dt_researchy[0][1] +=
              pos > 0 || pos2 < sHintCount;
        } else {
          WorkerCounters::MyCounters().dt_researchy[0][2]++;
          WorkerCounters::MyCounters().dt_researchy[0][3] +=
              pos > 0 || pos2 < sHintCount;
        }
      } else {
      }
    }
  }

  template <bool equality_only = false>
  int16_t linearSearchWithBias(Slice key, uint16_t start_pos,
                               bool higher = true) {
    // EXP
    if (key.size() < mPrefixSize ||
        (bcmp(key.data(), getLowerFenceKey(), mPrefixSize) != 0)) {
      return -1;
    }
    DCHECK(key.size() >= mPrefixSize &&
           bcmp(key.data(), getLowerFenceKey(), mPrefixSize) == 0);

    // the compared key has the same prefix
    key.remove_prefix(mPrefixSize);

    if (higher) {
      int32_t cur = start_pos + 1;
      for (; cur < mNumSeps; cur++) {
        int cmp = CmpKeys(key, KeyWithoutPrefix(cur));
        if (cmp == 0) {
          return cur;
        } else {
          break;
        }
      }
      if (equality_only) {
        return -1;
      } else {
        return cur;
      }
    } else {
      int32_t cur = start_pos - 1;
      for (; cur >= 0; cur--) {
        int cmp = CmpKeys(key, KeyWithoutPrefix(cur));
        if (cmp == 0) {
          return cur;
        } else {
          break;
        }
      }
      if (equality_only) {
        return -1;
      } else {
        return cur;
      }
    }
  }

  /**
   * Returns the position where the key[pos] (if exists) >= key (not less than
   * the given key):
   * (2) (2) (1) ->
   * (2) (2) (1) (0) ->
   * (2) (2) (1) (0) (0) ->
   * ...  ->
   * (2) (2) (2)
   */
  template <bool equalityOnly = false>
  int16_t lowerBound(Slice key, bool* isEqual = nullptr) {
    if (isEqual != nullptr && mIsLeaf) {
      *isEqual = false;
    }

    // compare prefix firstly
    if (equalityOnly) {
      if ((key.size() < mPrefixSize) ||
          (bcmp(key.data(), getLowerFenceKey(), mPrefixSize) != 0)) {
        return -1;
      }
    } else if (mPrefixSize != 0) {
      Slice keyPrefix(key.data(), min<uint16_t>(key.size(), mPrefixSize));
      Slice lowerFencePrefix(getLowerFenceKey(), mPrefixSize);
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
    HeadType keyHead = head(key);
    searchHint(keyHead, lower, upper);
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

    if (equalityOnly) {
      return -1;
    }
    return lower;
  }

  void updateHint(uint16_t slotId);

  int16_t insertDoNotCopyPayload(Slice key, uint16_t valSize, int32_t pos = -1);

  int32_t Insert(Slice key, Slice val);

  static uint16_t spaceNeeded(uint16_t keySize, uint16_t valSize,
                              uint16_t prefixLength);

  uint16_t spaceNeeded(uint16_t keySize, uint16_t valSize);

  bool canInsert(uint16_t keySize, uint16_t valSize);

  bool prepareInsert(uint16_t keySize, uint16_t valSize);

  void compactify();

  // merge right node into this node
  uint32_t mergeSpaceUpperBound(
      ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedRight);

  uint32_t spaceUsedBySlot(uint16_t slotId);

  bool merge(uint16_t slotId,
             ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedParent,
             ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedRight);

  // store key/value pair at slotId
  void storeKeyValue(uint16_t slotId, Slice key, Slice val);

  // ATTENTION: dstSlot then srcSlot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  void copyKeyValueRange(BTreeNode* dst, uint16_t dstSlot, uint16_t srcSlot,
                         uint16_t count);
  void copyKeyValue(uint16_t srcSlot, BTreeNode* dst, uint16_t dstSlot);
  void insertFence(FenceKey& fk, Slice key);
  void setFences(Slice lowerKey, Slice upperKey);
  void Split(ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedParent,
             ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedNewLeft,
             const BTreeNode::SeparatorInfo& sepInfo);
  uint16_t commonPrefix(uint16_t aPos, uint16_t bPos);
  SeparatorInfo findSep();
  Swip& lookupInner(Slice key);

  // Not synchronized or todo section
  bool removeSlot(uint16_t slotId);

  bool Remove(Slice key);

  void Reset();

  void ToJson(rapidjson::Value* resultObj,
              rapidjson::Value::AllocatorType& allocator);

private:
  inline void generateSeparator(const SeparatorInfo& sepInfo, uint8_t* sepKey) {
    // prefix
    memcpy(sepKey, getLowerFenceKey(), mPrefixSize);

    if (sepInfo.trunc) {
      memcpy(sepKey + mPrefixSize, KeyDataWithoutPrefix(sepInfo.mSlotId + 1),
             sepInfo.mSize - mPrefixSize);
    } else {
      memcpy(sepKey + mPrefixSize, KeyDataWithoutPrefix(sepInfo.mSlotId),
             sepInfo.mSize - mPrefixSize);
    }
  }

  inline bool shrinkSearchRange(uint16_t& lower, uint16_t& upper, Slice key) {
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

  inline bool shrinkSearchRangeWithHead(uint16_t& lower, uint16_t& upper,
                                        Slice key, HeadType keyHead) {
    auto mid = ((upper - lower) / 2) + lower;
    auto midHead = slot[mid].head;
    auto midSize = slot[mid].mKeySizeWithoutPrefix;
    if ((keyHead < midHead) ||
        (keyHead == midHead && midSize <= 4 && key.size() < midSize)) {
      upper = mid;
      return false;
    }

    if ((keyHead > midHead) ||
        (keyHead == midHead && midSize <= 4 && key.size() > midSize)) {
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
  template <typename... Args>
  inline static BTreeNode* Init(void* addr, Args&&... args) {
    return new (addr) BTreeNode(std::forward<Args>(args)...);
  }

  inline static uint16_t Size() {
    return static_cast<uint16_t>(utils::tlsStore->mStoreOption.mPageSize -
                                 sizeof(Page));
  }

  inline static uint16_t UnderFullSize() {
    return BTreeNode::Size() * 0.6;
  }
};

} // namespace leanstore::storage::btree
