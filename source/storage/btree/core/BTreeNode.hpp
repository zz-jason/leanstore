#pragma once

#include "Exceptions.hpp"
#include "Units.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "storage/buffer-manager/BufferFrame.hpp"
#include "storage/buffer-manager/GuardedBufferFrame.hpp"
#include "storage/buffer-manager/TreeRegistry.hpp"

#include "rapidjson/document.h"

#include <algorithm>
#include <alloca.h>
#include <cassert>
#include <cstring>
#include <fstream>
#include <string>

using namespace std;
using namespace leanstore::storage;

namespace leanstore {
namespace storage {
namespace btree {

struct BTreeNode;
using SwipType = Swip<BTreeNode>;
using HeadType = u32;

static inline u64 swap(u64 x) {
  return __builtin_bswap64(x);
}
static inline u32 swap(u32 x) {
  return __builtin_bswap32(x);
}
static inline u16 swap(u16 x) {
  return __builtin_bswap16(x);
}
static inline u8 swap(u8 x) {
  return x;
}

class BTreeNodeHeader {
public:
  static const u16 sUnderFullSize = EFFECTIVE_PAGE_SIZE * 0.6;
  static const u16 sKWayMergeThreshold = EFFECTIVE_PAGE_SIZE * 0.45;
  static const u16 sHintCount = 16;

  struct SeparatorInfo {
    u16 length;
    u16 slot;
    bool trunc; // TODO: ???
  };

  struct FenceKey {
    u16 offset;
    u16 length;
  };

public:
  /// The swip of the right-most child.
  Swip<BTreeNode> mRightMostChildSwip = nullptr;

  /// The lower fence of the node.
  FenceKey mLowerFence = {0, 0};

  /// The upper fence of the node.
  FenceKey mUpperFence = {0, 0};

  /// The number of seperators. #slots = #seps + 1.
  /// The first mNumSeps children are stored in the payload, while the last
  /// child are stored in upper.
  u16 mNumSeps = 0;

  /// Indicates whether this node is leaf node without any child.
  bool mIsLeaf;

  /// Indicates the space used for the node.
  /// @note !!! does not include the header, but includes fences !!!
  u16 mSpaceUsed = 0;

  u16 mDataOffset = static_cast<u16>(EFFECTIVE_PAGE_SIZE);

  u16 mPrefixSize = 0;

  u32 hint[sHintCount];

  /// Needed for GC
  bool mHasGarbage = false;

public:
  BTreeNodeHeader(bool isLeaf) : mIsLeaf(isLeaf) {
  }

  ~BTreeNodeHeader() {
  }

public:
  inline u8* RawPtr() {
    return reinterpret_cast<u8*>(this);
  }

  inline bool isInner() {
    return !mIsLeaf;
  }

  inline Slice GetLowerFence() {
    return Slice(getLowerFenceKey(), mLowerFence.length);
  }

  inline u8* getLowerFenceKey() {
    return mLowerFence.offset ? RawPtr() + mLowerFence.offset : nullptr;
  }

  inline Slice GetUpperFence() {
    return Slice(getUpperFenceKey(), mUpperFence.length);
  }

  inline u8* getUpperFenceKey() {
    return mUpperFence.offset ? RawPtr() + mUpperFence.offset : nullptr;
  }

  inline bool isUpperFenceInfinity() {
    return !mUpperFence.offset;
  }

  inline bool isLowerFenceInfinity() {
    return !mLowerFence.offset;
  }
};

class BTreeVisitor;

class BTreeNode : public BTreeNodeHeader {
public:
  struct __attribute__((packed)) Slot {
    // Layout:  key wihtout prefix | Payload
    u16 offset;
    u16 mKeySizeWithoutPrefix;
    u16 mValSize;
    union {
      HeadType head;
      u8 mHeadBytes[4];
    };
  };

public:
  // Just to make sizeof(BTreeNode) == EFFECTIVE_PAGE_SIZE
  static constexpr u64 sSlotCapacity =
      (EFFECTIVE_PAGE_SIZE - sizeof(BTreeNodeHeader)) / (sizeof(Slot));

  static constexpr u64 sLeftSpaceToWaste =
      (EFFECTIVE_PAGE_SIZE - sizeof(BTreeNodeHeader)) % (sizeof(Slot));

public:
  Slot slot[sSlotCapacity];

  u8 padding[sLeftSpaceToWaste];

public:
  BTreeNode(bool isLeaf) : BTreeNodeHeader(isLeaf) {
  }

public:
  u16 freeSpace() {
    return mDataOffset - (reinterpret_cast<u8*>(slot + mNumSeps) - RawPtr());
  }

  u16 freeSpaceAfterCompaction() {
    return EFFECTIVE_PAGE_SIZE -
           (reinterpret_cast<u8*>(slot + mNumSeps) - RawPtr()) - mSpaceUsed;
  }

  double fillFactorAfterCompaction() {
    return (1 - (freeSpaceAfterCompaction() * 1.0 / EFFECTIVE_PAGE_SIZE));
  }

  bool hasEnoughSpaceFor(u32 space_needed) {
    return (space_needed <= freeSpace() ||
            space_needed <= freeSpaceAfterCompaction());
  }

  // ATTENTION: this method has side effects !
  bool requestSpaceFor(u16 space_needed) {
    if (space_needed <= freeSpace())
      return true;
    if (space_needed <= freeSpaceAfterCompaction()) {
      compactify();
      return true;
    }
    return false;
  }

  inline Slice KeyWithoutPrefix(u16 slotId) {
    return Slice(KeyDataWithoutPrefix(slotId), KeySizeWithoutPrefix(slotId));
  }

  inline u8* KeyDataWithoutPrefix(u16 slotId) {
    return RawPtr() + slot[slotId].offset;
  }

  inline u16 KeySizeWithoutPrefix(u16 slotId) {
    return slot[slotId].mKeySizeWithoutPrefix;
  }

  inline Slice Value(u16 slotId) {
    return Slice(ValData(slotId), ValSize(slotId));
  }

  // Each slot is composed of:
  // key (mKeySizeWithoutPrefix), payload (mValSize)
  inline u8* ValData(u16 slotId) {
    return RawPtr() + slot[slotId].offset + slot[slotId].mKeySizeWithoutPrefix;
  }

  inline u16 ValSize(u16 slotId) {
    return slot[slotId].mValSize;
  }

  inline SwipType& GetChildIncludingRightMost(u16 slotId) {
    if (slotId == mNumSeps) {
      return mRightMostChildSwip;
    } else {
      return *reinterpret_cast<SwipType*>(ValData(slotId));
    }
  }

  inline SwipType& getChild(u16 slotId) {
    return *reinterpret_cast<SwipType*>(ValData(slotId));
  }

  inline u16 getKVConsumedSpace(u16 slot_id) {
    return sizeof(Slot) + KeySizeWithoutPrefix(slot_id) + ValSize(slot_id);
  }

  // Attention: the caller has to hold a copy of the existing payload
  inline void shortenPayload(u16 slotId, u16 len) {
    assert(len <= slot[slotId].mValSize);
    const u16 freed_space = slot[slotId].mValSize - len;
    mSpaceUsed -= freed_space;
    slot[slotId].mValSize = len;
  }

  inline bool canExtendPayload(u16 slot_id, u16 new_length) {
    assert(new_length > ValSize(slot_id));
    const u16 extra_space_needed = new_length - ValSize(slot_id);
    return freeSpaceAfterCompaction() >= extra_space_needed;
  }

  void extendPayload(u16 slot_id, u16 new_payload_length) {
    // Move key | payload to a new location
    assert(canExtendPayload(slot_id, new_payload_length));
    const u16 extra_space_needed = new_payload_length - ValSize(slot_id);
    requestSpaceFor(extra_space_needed);
    // -------------------------------------------------------------------------------------
    auto keySizeWithoutPrefix = KeySizeWithoutPrefix(slot_id);
    const u16 old_total_length = keySizeWithoutPrefix + ValSize(slot_id);
    const u16 new_total_length = keySizeWithoutPrefix + new_payload_length;
    // Allocate a block that will be freed when the calling function exits.
    u8* key = (u8*)alloca(keySizeWithoutPrefix * sizeof(u8));
    std::memcpy(key, KeyDataWithoutPrefix(slot_id), keySizeWithoutPrefix);
    mSpaceUsed -= old_total_length;
    if (mDataOffset == slot[slot_id].offset && 0) {
      mDataOffset += old_total_length;
    }
    slot[slot_id].mValSize = 0;
    slot[slot_id].mKeySizeWithoutPrefix = 0;
    if (freeSpace() < new_total_length) {
      compactify();
    }
    assert(freeSpace() >= new_total_length);
    mSpaceUsed += new_total_length;
    mDataOffset -= new_total_length;
    slot[slot_id].offset = mDataOffset;
    slot[slot_id].mKeySizeWithoutPrefix = keySizeWithoutPrefix;
    slot[slot_id].mValSize = new_payload_length;
    std::memcpy(KeyDataWithoutPrefix(slot_id), key, keySizeWithoutPrefix);
  }

  inline Slice KeyPrefix() {
    return Slice(getLowerFenceKey(), mPrefixSize);
  }

  inline u8* getPrefix() {
    return getLowerFenceKey();
  }

  inline void copyPrefix(u8* out) {
    memcpy(out, getLowerFenceKey(), mPrefixSize);
  }

  inline void copyKeyWithoutPrefix(u16 slotId, u8* out_after_prefix) {
    auto key = KeyWithoutPrefix(slotId);
    memcpy(out_after_prefix, key.data(), key.size());
  }

  inline u16 getFullKeyLen(u16 slotId) {
    return mPrefixSize + KeySizeWithoutPrefix(slotId);
  }

  inline void copyFullKey(u16 slotId, u8* out) {
    memcpy(out, getPrefix(), mPrefixSize);
    auto remaining = KeyWithoutPrefix(slotId);
    memcpy(out + mPrefixSize, remaining.data(), remaining.size());
  }

  static inline s32 CmpKeys(Slice lhs, Slice rhs) {
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

  static inline HeadType head(Slice key) {
    switch (key.size()) {
    case 0: {
      return 0;
    }
    case 1: {
      return static_cast<u32>(key[0]) << 24;
    }
    case 2: {
      const u16 bigEndianVal = *reinterpret_cast<const u16*>(key.data());
      const u16 littleEndianVal = __builtin_bswap16(bigEndianVal);
      return static_cast<u32>(littleEndianVal) << 16;
    }
    case 3: {
      const u16 bigEndianVal = *reinterpret_cast<const u16*>(key.data());
      const u16 littleEndianVal = __builtin_bswap16(bigEndianVal);
      return (static_cast<u32>(littleEndianVal) << 16) |
             (static_cast<u32>(key[2]) << 8);
    }
    default: {
      return __builtin_bswap32(*reinterpret_cast<const u32*>(key.data()));
    }
    }

    return __builtin_bswap32(*reinterpret_cast<const u32*>(key.data()));
  }

  void makeHint();

  s32 compareKeyWithBoundaries(Slice key);

  void searchHint(HeadType key_head, u16& lower_out, u16& upper_out) {
    if (mNumSeps > sHintCount * 2) {
      if (FLAGS_btree_hints == 2) {
#ifdef __AVX512F__
        const u16 dist = mNumSeps / (sHintCount + 1);
        u16 pos, pos2;
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
        throw;
#endif
      } else if (FLAGS_btree_hints == 1) {
        const u16 dist = mNumSeps / (sHintCount + 1);
        u16 pos, pos2;

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
          WorkerCounters::myCounters().dt_researchy[0][0]++;
          WorkerCounters::myCounters().dt_researchy[0][1] +=
              pos > 0 || pos2 < sHintCount;
        } else {
          WorkerCounters::myCounters().dt_researchy[0][2]++;
          WorkerCounters::myCounters().dt_researchy[0][3] +=
              pos > 0 || pos2 < sHintCount;
        }
      } else {
      }
    }
  }

  template <bool equality_only = false>
  s16 linearSearchWithBias(Slice key, u16 start_pos, bool higher = true) {
    throw;
    // EXP
    if (key.size() < mPrefixSize ||
        (bcmp(key.data(), getLowerFenceKey(), mPrefixSize) != 0)) {
      return -1;
    }
    assert((key.size() >= mPrefixSize) &&
           (bcmp(key.data(), getLowerFenceKey(), mPrefixSize) == 0));

    // the compared key has the same prefix
    key.remove_prefix(mPrefixSize);

    if (higher) {
      s32 cur = start_pos + 1;
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
      s32 cur = start_pos - 1;
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
  s16 lowerBound(Slice key, bool* isEqual = nullptr) {
    if (isEqual != nullptr && mIsLeaf) {
      *isEqual = false;
    }

    // compare prefix firstly
    if (equalityOnly) {
      if ((key.size() < mPrefixSize) ||
          (bcmp(key.data(), getLowerFenceKey(), mPrefixSize) != 0)) {
        return -1;
      }
    } else {
      Slice keyPrefix(key.data(), min<u16>(key.size(), mPrefixSize));
      Slice lowerFencePrefix(getLowerFenceKey(), mPrefixSize);
      int cmpPrefix = CmpKeys(keyPrefix, lowerFencePrefix);
      if (cmpPrefix < 0) {
        return 0;
      } else if (cmpPrefix > 0) {
        return mNumSeps;
      }
    }

    // the compared key has the same prefix
    key.remove_prefix(mPrefixSize);
    u16 lower = 0;
    u16 upper = mNumSeps;
    HeadType keyHead = head(key);
    searchHint(keyHead, lower, upper);
    while (lower < upper) {
      bool foundEqual(false);
      if (FLAGS_btree_heads) {
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

  void updateHint(u16 slotId);

  s16 insertDoNotCopyPayload(Slice key, u16 valSize, s32 pos = -1);

  s32 insert(Slice key, Slice val);
  static u16 spaceNeeded(u16 keyLength, u16 valSize, u16 prefixLength);
  u16 spaceNeeded(u16 key_length, u16 valSize);
  bool canInsert(u16 key_length, u16 valSize);
  bool prepareInsert(u16 keyLength, u16 valSize);

  void compactify();

  // merge right node into this node
  u32 mergeSpaceUpperBound(
      ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedRight);
  u32 spaceUsedBySlot(u16 slot_id);

  bool merge(u16 slotId, ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedParent,
             ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedRight);

  // store key/value pair at slotId
  void storeKeyValue(u16 slotId, Slice key, Slice val);

  // ATTENTION: dstSlot then srcSlot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  void copyKeyValueRange(BTreeNode* dst, u16 dstSlot, u16 srcSlot, u16 count);
  void copyKeyValue(u16 srcSlot, BTreeNode* dst, u16 dstSlot);
  void insertFence(FenceKey& fk, Slice key);
  void setFences(Slice lowerKey, Slice upperKey);
  void split(ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedParent,
             ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedNewNode,
             u16 sepSlot, u8* sepKey, u16 sepLength);
  u16 commonPrefix(u16 aPos, u16 bPos);
  SeparatorInfo findSep();
  void getSep(u8* sepKeyOut, SeparatorInfo info);
  Swip<BTreeNode>& lookupInner(Slice key);

  // Not synchronized or todo section
  bool removeSlot(u16 slotId);
  bool remove(Slice key);
  void reset();

  void Accept(BTreeVisitor* visitor);

  void ToJSON(rapidjson::Value* resultObj,
              rapidjson::Value::AllocatorType& allocator);

private:
  inline bool shrinkSearchRange(u16& lower, u16& upper, Slice key) {
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

  inline bool shrinkSearchRangeWithHead(u16& lower, u16& upper, Slice key,
                                        HeadType keyHead) {
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
};

static_assert(sizeof(BTreeNode) == EFFECTIVE_PAGE_SIZE,
              "BTreeNode must be equal to one page");

} // namespace btree
} // namespace storage
} // namespace leanstore
