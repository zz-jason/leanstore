#include "leanstore/btree/core/BTreeNode.hpp"

#include "leanstore/Exceptions.hpp"
#include "leanstore/Slice.hpp"
#include "leanstore/buffer-manager/GuardedBufferFrame.hpp"
#include "leanstore/utils/Defer.hpp"
#include "leanstore/utils/Log.hpp"

#include <algorithm>
#include <cstdint>

namespace leanstore::storage::btree {

void BTreeNode::UpdateHint(uint16_t slotId) {
  uint16_t dist = mNumSlots / (sHintCount + 1);
  uint16_t begin = 0;
  if ((mNumSlots > sHintCount * 2 + 1) && (((mNumSlots - 1) / (sHintCount + 1)) == dist) &&
      ((slotId / dist) > 1))
    begin = (slotId / dist) - 1;
  for (uint16_t i = begin; i < sHintCount; i++)
    mHint[i] = mSlot[dist * (i + 1)].mHead;
  for (uint16_t i = 0; i < sHintCount; i++)
    assert(mHint[i] == mSlot[dist * (i + 1)].mHead);
}

void BTreeNode::SearchHint(HeadType keyHead, uint16_t& lowerOut, uint16_t& upperOut) {
  if (mNumSlots > sHintCount * 2) {
    if (utils::tlsStore->mStoreOption->mBTreeHints == 2) {
#ifdef __AVX512F__
      const uint16_t dist = mNumSlots / (sHintCount + 1);
      uint16_t pos, pos2;
      __m512i key_head_reg = _mm512_set1_epi32(keyHead);
      __m512i chunk = _mm512_loadu_si512(hint);
      __mmask16 compareMask = _mm512_cmpge_epu32_mask(chunk, key_head_reg);
      if (compareMask == 0)
        return;
      pos = __builtin_ctz(compareMask);
      lowerOut = pos * dist;
      // -------------------------------------------------------------------------------------
      for (pos2 = pos; pos2 < sHintCount; pos2++) {
        if (mHint[pos2] != keyHead) {
          break;
        }
      }
      if (pos2 < sHintCount) {
        upperOut = (pos2 + 1) * dist;
      }
#else
      Log::Error("Search hint with AVX512 failed: __AVX512F__ not found");
#endif
    } else if (utils::tlsStore->mStoreOption->mBTreeHints == 1) {
      const uint16_t dist = mNumSlots / (sHintCount + 1);
      uint16_t pos, pos2;

      for (pos = 0; pos < sHintCount; pos++) {
        if (mHint[pos] >= keyHead) {
          break;
        }
      }
      for (pos2 = pos; pos2 < sHintCount; pos2++) {
        if (mHint[pos2] != keyHead) {
          break;
        }
      }

      lowerOut = pos * dist;
      if (pos2 < sHintCount) {
        upperOut = (pos2 + 1) * dist;
      }
    } else {
    }
  }
}

int16_t BTreeNode::InsertDoNotCopyPayload(Slice key, uint16_t valSize, int32_t pos) {
  LS_DCHECK(CanInsert(key.size(), valSize));
  PrepareInsert(key.size(), valSize);

  // calculate taret slotId for insertion
  int32_t slotId = (pos == -1) ? LowerBound<false>(key) : pos;

  // 1. move mSlot[slotId..mNumSlots] to mSlot[slotId+1..mNumSlots+1]
  memmove(mSlot + slotId + 1, mSlot + slotId, sizeof(BTreeNodeSlot) * (mNumSlots - slotId));

  // remove common key prefix
  key.remove_prefix(mPrefixSize);

  //
  mSlot[slotId].mHead = Head(key);
  mSlot[slotId].mKeySizeWithoutPrefix = key.size();
  mSlot[slotId].mValSize = valSize;
  auto totalKeyValSize = key.size() + valSize;
  advanceDataOffset(totalKeyValSize);
  mSlot[slotId].mOffset = mDataOffset;
  memcpy(KeyDataWithoutPrefix(slotId), key.data(), key.size());

  mNumSlots++;
  UpdateHint(slotId);
  return slotId;
}

int32_t BTreeNode::Insert(Slice key, Slice val) {
  DEBUG_BLOCK() {
    assert(CanInsert(key.size(), val.size()));
    int32_t exactPos = LowerBound<true>(key);
    static_cast<void>(exactPos);
    assert(exactPos == -1); // assert for duplicates
  }

  PrepareInsert(key.size(), val.size());
  int32_t slotId = LowerBound<false>(key);
  memmove(mSlot + slotId + 1, mSlot + slotId, sizeof(BTreeNodeSlot) * (mNumSlots - slotId));
  StoreKeyValue(slotId, key, val);
  mNumSlots++;
  UpdateHint(slotId);
  return slotId;

  DEBUG_BLOCK() {
    int32_t exactPos = LowerBound<true>(key);
    static_cast<void>(exactPos);
    // assert for duplicates
    assert(exactPos == slotId);
  }
}

void BTreeNode::Compactify() {
  uint16_t spaceAfterCompaction [[maybe_unused]] = 0;
  DEBUG_BLOCK() {
    spaceAfterCompaction = FreeSpaceAfterCompaction();
  }
  SCOPED_DEFER(DEBUG_BLOCK() { LS_DCHECK(spaceAfterCompaction == FreeSpace()); });

  // generate a temp node to store the compacted data
  auto tmpNodeBuf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
  auto* tmp = BTreeNode::New(tmpNodeBuf->get(), mIsLeaf, GetLowerFence(), GetUpperFence());

  // copy the keys and values
  CopyKeyValueRange(tmp, 0, 0, mNumSlots);

  // copy the right most child
  tmp->mRightMostChildSwip = mRightMostChildSwip;

  // copy back
  memcpy(reinterpret_cast<char*>(this), tmp, BTreeNode::Size());
  MakeHint();
}

uint32_t BTreeNode::MergeSpaceUpperBound(ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedRight) {
  LS_DCHECK(xGuardedRight->mIsLeaf);

  auto tmpNodeBuf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
  auto* tmp =
      BTreeNode::New(tmpNodeBuf->get(), true, GetLowerFence(), xGuardedRight->GetUpperFence());

  uint32_t leftGrow = (mPrefixSize - tmp->mPrefixSize) * mNumSlots;
  uint32_t rightGrow = (xGuardedRight->mPrefixSize - tmp->mPrefixSize) * xGuardedRight->mNumSlots;
  uint32_t spaceUpperBound =
      mSpaceUsed + xGuardedRight->mSpaceUsed +
      (reinterpret_cast<uint8_t*>(mSlot + mNumSlots + xGuardedRight->mNumSlots) - NodeBegin()) +
      leftGrow + rightGrow;
  return spaceUpperBound;
}

// right survives, this gets reclaimed left(this) into right
bool BTreeNode::merge(uint16_t slotId, ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedParent,
                      ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedRight) {
  if (mIsLeaf) {
    assert(xGuardedRight->mIsLeaf);
    assert(xGuardedParent->IsInner());

    auto tmpNodeBuf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
    auto* tmp =
        BTreeNode::New(tmpNodeBuf->get(), true, GetLowerFence(), xGuardedRight->GetUpperFence());
    uint16_t leftGrow = (mPrefixSize - tmp->mPrefixSize) * mNumSlots;
    uint16_t rightGrow = (xGuardedRight->mPrefixSize - tmp->mPrefixSize) * xGuardedRight->mNumSlots;
    uint16_t spaceUpperBound =
        mSpaceUsed + xGuardedRight->mSpaceUsed +
        (reinterpret_cast<uint8_t*>(mSlot + mNumSlots + xGuardedRight->mNumSlots) - NodeBegin()) +
        leftGrow + rightGrow;
    if (spaceUpperBound > BTreeNode::Size()) {
      return false;
    }
    CopyKeyValueRange(tmp, 0, 0, mNumSlots);
    xGuardedRight->CopyKeyValueRange(tmp, mNumSlots, 0, xGuardedRight->mNumSlots);
    xGuardedParent->RemoveSlot(slotId);

    xGuardedRight->mHasGarbage |= mHasGarbage;

    memcpy(xGuardedRight.GetPagePayloadPtr(), tmp, BTreeNode::Size());
    xGuardedRight->MakeHint();
    return true;
  }

  // Inner node
  LS_DCHECK(!xGuardedRight->mIsLeaf);
  LS_DCHECK(xGuardedParent->IsInner());

  auto tmpNodeBuf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
  auto* tmp =
      BTreeNode::New(tmpNodeBuf->get(), mIsLeaf, GetLowerFence(), xGuardedRight->GetUpperFence());
  uint16_t leftGrow = (mPrefixSize - tmp->mPrefixSize) * mNumSlots;
  uint16_t rightGrow = (xGuardedRight->mPrefixSize - tmp->mPrefixSize) * xGuardedRight->mNumSlots;
  uint16_t extraKeyLength = xGuardedParent->GetFullKeyLen(slotId);
  uint16_t spaceUpperBound =
      mSpaceUsed + xGuardedRight->mSpaceUsed +
      (reinterpret_cast<uint8_t*>(mSlot + mNumSlots + xGuardedRight->mNumSlots) - NodeBegin()) +
      leftGrow + rightGrow + SpaceNeeded(extraKeyLength, sizeof(Swip), tmp->mPrefixSize);
  if (spaceUpperBound > BTreeNode::Size())
    return false;
  CopyKeyValueRange(tmp, 0, 0, mNumSlots);
  // Allocate in the stack, freed when the calling function exits.
  auto extraKey = utils::JumpScopedArray<uint8_t>(extraKeyLength);
  xGuardedParent->CopyFullKey(slotId, extraKey->get());
  tmp->StoreKeyValue(mNumSlots, Slice(extraKey->get(), extraKeyLength),
                     Slice(reinterpret_cast<uint8_t*>(&mRightMostChildSwip), sizeof(Swip)));
  tmp->mNumSlots++;
  xGuardedRight->CopyKeyValueRange(tmp, tmp->mNumSlots, 0, xGuardedRight->mNumSlots);
  xGuardedParent->RemoveSlot(slotId);
  tmp->mRightMostChildSwip = xGuardedRight->mRightMostChildSwip;
  tmp->MakeHint();
  memcpy(xGuardedRight.GetPagePayloadPtr(), tmp, BTreeNode::Size());
  return true;
}

void BTreeNode::StoreKeyValue(uint16_t slotId, Slice key, Slice val) {
  // Head
  key.remove_prefix(mPrefixSize);
  mSlot[slotId].mHead = Head(key);
  mSlot[slotId].mKeySizeWithoutPrefix = key.size();
  mSlot[slotId].mValSize = val.size();

  // Value
  advanceDataOffset(key.size() + val.size());
  mSlot[slotId].mOffset = mDataOffset;
  memcpy(KeyDataWithoutPrefix(slotId), key.data(), key.size());
  memcpy(ValData(slotId), val.data(), val.size());
}

void BTreeNode::CopyKeyValueRange(BTreeNode* dst, uint16_t dstSlot, uint16_t srcSlot,
                                  uint16_t count) {
  if (mPrefixSize == dst->mPrefixSize) {
    // copy slot array
    memcpy(dst->mSlot + dstSlot, mSlot + srcSlot, sizeof(BTreeNodeSlot) * count);

    for (auto i = 0u; i < count; i++) {
      // consolidate the offset of each slot
      uint32_t kvSize = KeySizeWithoutPrefix(srcSlot + i) + ValSize(srcSlot + i);
      dst->advanceDataOffset(kvSize);
      dst->mSlot[dstSlot + i].mOffset = dst->mDataOffset;

      // copy the key value pair
      memcpy(dst->NodeBegin() + dst->mDataOffset, NodeBegin() + mSlot[srcSlot + i].mOffset, kvSize);
    }
  } else {
    for (uint16_t i = 0; i < count; i++)
      CopyKeyValue(srcSlot + i, dst, dstSlot + i);
  }
  dst->mNumSlots += count;
}

void BTreeNode::CopyKeyValue(uint16_t srcSlot, BTreeNode* dst, uint16_t dstSlot) {
  uint16_t fullLength = GetFullKeyLen(srcSlot);
  auto keyBuf = utils::JumpScopedArray<uint8_t>(fullLength);
  auto* key = keyBuf->get();
  CopyFullKey(srcSlot, key);
  dst->StoreKeyValue(dstSlot, Slice(key, fullLength), Value(srcSlot));
}

void BTreeNode::InsertFence(BTreeNodeHeader::FenceKey& fk, Slice key) {
  if (!key.data()) {
    return;
  }
  assert(FreeSpace() >= key.size());

  advanceDataOffset(key.size());
  fk.mOffset = mDataOffset;
  fk.mSize = key.size();
  memcpy(NodeBegin() + mDataOffset, key.data(), key.size());
}

uint16_t BTreeNode::CommonPrefix(uint16_t slotA, uint16_t slotB) {
  if (mNumSlots == 0) {
    // Do not prefix compress if only one tuple is in to
    // avoid corner cases (e.g., SI Version)
    return 0;
  }

  // TODO: the following two checks work only in single threaded
  //   assert(aPos < mNumSlots);
  //   assert(bPos < mNumSlots);
  uint32_t limit = std::min(mSlot[slotA].mKeySizeWithoutPrefix, mSlot[slotB].mKeySizeWithoutPrefix);
  uint8_t *a = KeyDataWithoutPrefix(slotA), *b = KeyDataWithoutPrefix(slotB);
  uint32_t i;
  for (i = 0; i < limit; i++)
    if (a[i] != b[i])
      break;
  return i;
}

BTreeNode::SeparatorInfo BTreeNode::FindSep() {
  LS_DCHECK(mNumSlots > 1);

  // Inner nodes are split in the middle
  if (IsInner()) {
    uint16_t slotId = mNumSlots / 2;
    return SeparatorInfo{GetFullKeyLen(slotId), slotId, false};
  }

  // Find good separator slot
  uint16_t bestPrefixLength, bestSlot;
  if (mNumSlots > 16) {
    uint16_t lower = (mNumSlots / 2) - (mNumSlots / 16);
    uint16_t upper = (mNumSlots / 2);

    bestPrefixLength = CommonPrefix(lower, 0);
    bestSlot = lower;

    if (bestPrefixLength != CommonPrefix(upper - 1, 0))
      for (bestSlot = lower + 1;
           (bestSlot < upper) && (CommonPrefix(bestSlot, 0) == bestPrefixLength); bestSlot++)
        ;
  } else {
    bestSlot = (mNumSlots - 1) / 2;
    // bestPrefixLength = CommonPrefix(bestSlot, 0);
  }

  // Try to truncate separator
  uint16_t common = CommonPrefix(bestSlot, bestSlot + 1);
  if ((bestSlot + 1 < mNumSlots) && (mSlot[bestSlot].mKeySizeWithoutPrefix > common) &&
      (mSlot[bestSlot + 1].mKeySizeWithoutPrefix > (common + 1)))
    return SeparatorInfo{static_cast<uint16_t>(mPrefixSize + common + 1), bestSlot, true};

  return SeparatorInfo{GetFullKeyLen(bestSlot), bestSlot, false};
}

int32_t BTreeNode::CompareKeyWithBoundaries(Slice key) {
  // Lower Bound exclusive, upper bound inclusive
  if (mLowerFence.mOffset) {
    int cmp = CmpKeys(key, GetLowerFence());
    if (!(cmp > 0))
      return 1; // Key lower or equal LF
  }

  if (mUpperFence.mOffset) {
    int cmp = CmpKeys(key, GetUpperFence());
    if (!(cmp <= 0))
      return -1; // Key higher than UF
  }
  return 0;
}

Swip& BTreeNode::LookupInner(Slice key) {
  int32_t slotId = LowerBound<false>(key);
  if (slotId == mNumSlots) {
    LS_DCHECK(!mRightMostChildSwip.IsEmpty());
    return mRightMostChildSwip;
  }
  auto* childSwip = ChildSwip(slotId);
  LS_DCHECK(!childSwip->IsEmpty(), "childSwip is empty, slotId={}", slotId);
  return *childSwip;
}

//! xGuardedParent           xGuardedParent
//!      |                      |       |
//!     this           xGuardedNewLeft this
//!
void BTreeNode::Split(ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedParent,
                      ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedNewLeft,
                      const BTreeNode::SeparatorInfo& sepInfo) {
  LS_DCHECK(xGuardedParent->CanInsert(sepInfo.mSize, sizeof(Swip)));

  // generate separator key
  uint8_t sepKey[sepInfo.mSize];
  generateSeparator(sepInfo, sepKey);
  Slice seperator{sepKey, sepInfo.mSize};

  xGuardedNewLeft->setFences(GetLowerFence(), seperator);

  uint8_t tmpRightBuf[BTreeNode::Size()];
  auto* tmpRight = BTreeNode::New(tmpRightBuf, mIsLeaf, seperator, GetUpperFence());

  // insert (seperator, xGuardedNewLeft) into xGuardedParent
  auto swip = xGuardedNewLeft.swip();
  xGuardedParent->Insert(seperator, Slice(reinterpret_cast<uint8_t*>(&swip), sizeof(Swip)));

  if (mIsLeaf) {
    // move slot 0..sepInfo.mSlotId to xGuardedNewLeft
    CopyKeyValueRange(xGuardedNewLeft.GetPagePayload(), 0, 0, sepInfo.mSlotId + 1);

    // move slot sepInfo.mSlotId+1..mNumSlots to tmpRight
    CopyKeyValueRange(tmpRight, 0, xGuardedNewLeft->mNumSlots,
                      mNumSlots - xGuardedNewLeft->mNumSlots);
    tmpRight->mHasGarbage = mHasGarbage;
    xGuardedNewLeft->mHasGarbage = mHasGarbage;
  } else {
    CopyKeyValueRange(xGuardedNewLeft.GetPagePayload(), 0, 0, sepInfo.mSlotId);
    CopyKeyValueRange(tmpRight, 0, xGuardedNewLeft->mNumSlots + 1,
                      mNumSlots - xGuardedNewLeft->mNumSlots - 1);
    xGuardedNewLeft->mRightMostChildSwip = *ChildSwip(xGuardedNewLeft->mNumSlots);
    tmpRight->mRightMostChildSwip = mRightMostChildSwip;
  }
  xGuardedNewLeft->MakeHint();
  tmpRight->MakeHint();
  memcpy(reinterpret_cast<char*>(this), tmpRight, BTreeNode::Size());
}

bool BTreeNode::RemoveSlot(uint16_t slotId) {
  mSpaceUsed -= KeySizeWithoutPrefix(slotId) + ValSize(slotId);
  memmove(mSlot + slotId, mSlot + slotId + 1, sizeof(BTreeNodeSlot) * (mNumSlots - slotId - 1));
  mNumSlots--;
  MakeHint();
  return true;
}

bool BTreeNode::Remove(Slice key) {
  int slotId = LowerBound<true>(key);
  if (slotId == -1) {
    // key not found
    return false;
  }
  return RemoveSlot(slotId);
}

void BTreeNode::Reset() {
  mSpaceUsed = mUpperFence.mSize + mLowerFence.mSize;
  mDataOffset = BTreeNode::Size() - mSpaceUsed;
  mNumSlots = 0;
}

int32_t BTreeNode::CmpKeys(Slice lhs, Slice rhs) {
  auto minLength = std::min(lhs.size(), rhs.size());
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

HeadType BTreeNode::Head(Slice key) {
  switch (key.size()) {
  case 0: {
    return 0;
  }
  case 1: {
    return static_cast<uint32_t>(key[0]) << 24;
  }
  case 2: {
    const uint16_t bigEndianVal = *reinterpret_cast<const uint16_t*>(key.data());
    const uint16_t littleEndianVal = __builtin_bswap16(bigEndianVal);
    return static_cast<uint32_t>(littleEndianVal) << 16;
  }
  case 3: {
    const uint16_t bigEndianVal = *reinterpret_cast<const uint16_t*>(key.data());
    const uint16_t littleEndianVal = __builtin_bswap16(bigEndianVal);
    return (static_cast<uint32_t>(littleEndianVal) << 16) | (static_cast<uint32_t>(key[2]) << 8);
  }
  default: {
    return __builtin_bswap32(*reinterpret_cast<const uint32_t*>(key.data()));
  }
  }

  return __builtin_bswap32(*reinterpret_cast<const uint32_t*>(key.data()));
}

} // namespace leanstore::storage::btree
