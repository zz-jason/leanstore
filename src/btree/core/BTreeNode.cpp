#include "BTreeNode.hpp"

#include "buffer-manager/GuardedBufferFrame.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "utils/JsonUtil.hpp"
#include "utils/Log.hpp"

#include <algorithm>

namespace leanstore::storage::btree {

void BTreeNode::UpdateHint(uint16_t slotId) {
  uint16_t dist = mNumSeps / (sHintCount + 1);
  uint16_t begin = 0;
  if ((mNumSeps > sHintCount * 2 + 1) && (((mNumSeps - 1) / (sHintCount + 1)) == dist) &&
      ((slotId / dist) > 1))
    begin = (slotId / dist) - 1;
  for (uint16_t i = begin; i < sHintCount; i++)
    mHint[i] = mSlot[dist * (i + 1)].mHead;
  for (uint16_t i = 0; i < sHintCount; i++)
    assert(mHint[i] == mSlot[dist * (i + 1)].mHead);
}

void BTreeNode::SearchHint(HeadType keyHead, uint16_t& lowerOut, uint16_t& upperOut) {
  if (mNumSeps > sHintCount * 2) {
    if (utils::tlsStore->mStoreOption.mBTreeHints == 2) {
#ifdef __AVX512F__
      const uint16_t dist = mNumSeps / (sHintCount + 1);
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
    } else if (utils::tlsStore->mStoreOption.mBTreeHints == 1) {
      const uint16_t dist = mNumSeps / (sHintCount + 1);
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

      if (mIsLeaf) {
        WorkerCounters::MyCounters().dt_researchy[0][0]++;
        WorkerCounters::MyCounters().dt_researchy[0][1] += pos > 0 || pos2 < sHintCount;
      } else {
        WorkerCounters::MyCounters().dt_researchy[0][2]++;
        WorkerCounters::MyCounters().dt_researchy[0][3] += pos > 0 || pos2 < sHintCount;
      }
    } else {
    }
  }
}

int16_t BTreeNode::InsertDoNotCopyPayload(Slice key, uint16_t valSize, int32_t pos) {
  LS_DCHECK(CanInsert(key.size(), valSize));
  PrepareInsert(key.size(), valSize);

  int32_t slotId = (pos == -1) ? LowerBound<false>(key) : pos;
  memmove(mSlot + slotId + 1, mSlot + slotId, sizeof(Slot) * (mNumSeps - slotId));

  // StoreKeyValue
  key.remove_prefix(mPrefixSize);

  mSlot[slotId].mHead = Head(key);
  mSlot[slotId].mKeySizeWithoutPrefix = key.size();
  mSlot[slotId].mValSize = valSize;
  const uint16_t space = key.size() + valSize;
  mDataOffset -= space;
  mSpaceUsed += space;
  mSlot[slotId].mOffset = mDataOffset;
  memcpy(KeyDataWithoutPrefix(slotId), key.data(), key.size());

  mNumSeps++;
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
  memmove(mSlot + slotId + 1, mSlot + slotId, sizeof(Slot) * (mNumSeps - slotId));
  StoreKeyValue(slotId, key, val);
  mNumSeps++;
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
  uint16_t should = FreeSpaceAfterCompaction();
  static_cast<void>(should);

  auto tmpNodeBuf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
  auto* tmp = BTreeNode::Init(tmpNodeBuf->get(), mIsLeaf);

  tmp->SetFences(GetLowerFence(), GetUpperFence());
  CopyKeyValueRange(tmp, 0, 0, mNumSeps);
  tmp->mRightMostChildSwip = mRightMostChildSwip;
  memcpy(reinterpret_cast<char*>(this), tmp, BTreeNode::Size());
  MakeHint();
  assert(FreeSpace() == should);
}

uint32_t BTreeNode::MergeSpaceUpperBound(ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedRight) {
  LS_DCHECK(xGuardedRight->mIsLeaf);

  auto tmpNodeBuf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
  auto* tmp = BTreeNode::Init(tmpNodeBuf->get(), true);

  tmp->SetFences(GetLowerFence(), xGuardedRight->GetUpperFence());
  uint32_t leftGrow = (mPrefixSize - tmp->mPrefixSize) * mNumSeps;
  uint32_t rightGrow = (xGuardedRight->mPrefixSize - tmp->mPrefixSize) * xGuardedRight->mNumSeps;
  uint32_t spaceUpperBound =
      mSpaceUsed + xGuardedRight->mSpaceUsed +
      (reinterpret_cast<uint8_t*>(mSlot + mNumSeps + xGuardedRight->mNumSeps) - RawPtr()) +
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
    auto* tmp = BTreeNode::Init(tmpNodeBuf->get(), true);

    tmp->SetFences(GetLowerFence(), xGuardedRight->GetUpperFence());
    uint16_t leftGrow = (mPrefixSize - tmp->mPrefixSize) * mNumSeps;
    uint16_t rightGrow = (xGuardedRight->mPrefixSize - tmp->mPrefixSize) * xGuardedRight->mNumSeps;
    uint16_t spaceUpperBound =
        mSpaceUsed + xGuardedRight->mSpaceUsed +
        (reinterpret_cast<uint8_t*>(mSlot + mNumSeps + xGuardedRight->mNumSeps) - RawPtr()) +
        leftGrow + rightGrow;
    if (spaceUpperBound > BTreeNode::Size()) {
      return false;
    }
    CopyKeyValueRange(tmp, 0, 0, mNumSeps);
    xGuardedRight->CopyKeyValueRange(tmp, mNumSeps, 0, xGuardedRight->mNumSeps);
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
  auto* tmp = BTreeNode::Init(tmpNodeBuf->get(), mIsLeaf);

  tmp->SetFences(GetLowerFence(), xGuardedRight->GetUpperFence());
  uint16_t leftGrow = (mPrefixSize - tmp->mPrefixSize) * mNumSeps;
  uint16_t rightGrow = (xGuardedRight->mPrefixSize - tmp->mPrefixSize) * xGuardedRight->mNumSeps;
  uint16_t extraKeyLength = xGuardedParent->GetFullKeyLen(slotId);
  uint16_t spaceUpperBound =
      mSpaceUsed + xGuardedRight->mSpaceUsed +
      (reinterpret_cast<uint8_t*>(mSlot + mNumSeps + xGuardedRight->mNumSeps) - RawPtr()) +
      leftGrow + rightGrow + SpaceNeeded(extraKeyLength, sizeof(Swip), tmp->mPrefixSize);
  if (spaceUpperBound > BTreeNode::Size())
    return false;
  CopyKeyValueRange(tmp, 0, 0, mNumSeps);
  // Allocate in the stack, freed when the calling function exits.
  auto extraKey = utils::JumpScopedArray<uint8_t>(extraKeyLength);
  xGuardedParent->CopyFullKey(slotId, extraKey->get());
  tmp->StoreKeyValue(mNumSeps, Slice(extraKey->get(), extraKeyLength),
                     Slice(reinterpret_cast<uint8_t*>(&mRightMostChildSwip), sizeof(Swip)));
  tmp->mNumSeps++;
  xGuardedRight->CopyKeyValueRange(tmp, tmp->mNumSeps, 0, xGuardedRight->mNumSeps);
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
  const uint16_t space = key.size() + val.size();
  mDataOffset -= space;
  mSpaceUsed += space;
  mSlot[slotId].mOffset = mDataOffset;
  memcpy(KeyDataWithoutPrefix(slotId), key.data(), key.size());
  memcpy(ValData(slotId), val.data(), val.size());
  assert(RawPtr() + mDataOffset >= reinterpret_cast<uint8_t*>(mSlot + mNumSeps));
}

// ATTENTION: dstSlot then srcSlot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
void BTreeNode::CopyKeyValueRange(BTreeNode* dst, uint16_t dstSlot, uint16_t srcSlot,
                                  uint16_t count) {
  if (mPrefixSize == dst->mPrefixSize) {
    // Fast path
    memcpy(dst->mSlot + dstSlot, mSlot + srcSlot, sizeof(Slot) * count);
    DEBUG_BLOCK() {
      uint32_t totalSpaceUsed [[maybe_unused]] = mUpperFence.mLength + mLowerFence.mLength;
      for (uint16_t i = 0; i < this->mNumSeps; i++) {
        totalSpaceUsed += KeySizeWithoutPrefix(i) + ValSize(i);
      }
      assert(totalSpaceUsed == this->mSpaceUsed);
    }
    for (uint16_t i = 0; i < count; i++) {
      uint32_t kvSize = KeySizeWithoutPrefix(srcSlot + i) + ValSize(srcSlot + i);
      dst->mDataOffset -= kvSize;
      dst->mSpaceUsed += kvSize;
      dst->mSlot[dstSlot + i].mOffset = dst->mDataOffset;
      DEBUG_BLOCK() {
        [[maybe_unused]] int64_t offBy = reinterpret_cast<uint8_t*>(dst->mSlot + dstSlot + count) -
                                         (dst->RawPtr() + dst->mDataOffset);
        assert(offBy <= 0);
      }
      memcpy(dst->RawPtr() + dst->mDataOffset, RawPtr() + mSlot[srcSlot + i].mOffset, kvSize);
    }
  } else {
    for (uint16_t i = 0; i < count; i++)
      CopyKeyValue(srcSlot + i, dst, dstSlot + i);
  }
  dst->mNumSeps += count;
  assert((dst->RawPtr() + dst->mDataOffset) >=
         reinterpret_cast<uint8_t*>(dst->mSlot + dst->mNumSeps));
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

  mDataOffset -= key.size();
  mSpaceUsed += key.size();
  fk.mOffset = mDataOffset;
  fk.mLength = key.size();
  memcpy(RawPtr() + mDataOffset, key.data(), key.size());
}

void BTreeNode::SetFences(Slice lowerKey, Slice upperKey) {
  InsertFence(mLowerFence, lowerKey);
  InsertFence(mUpperFence, upperKey);
  LS_DCHECK(GetLowerFenceKey() == nullptr || GetUpperFenceKey() == nullptr ||
            *GetLowerFenceKey() <= *GetUpperFenceKey());

  // prefix compression
  for (mPrefixSize = 0; (mPrefixSize < min(lowerKey.size(), upperKey.size())) &&
                        (lowerKey[mPrefixSize] == upperKey[mPrefixSize]);
       mPrefixSize++)
    ;
}

uint16_t BTreeNode::CommonPrefix(uint16_t slotA, uint16_t slotB) {
  if (mNumSeps == 0) {
    // Do not prefix compress if only one tuple is in to
    // avoid corner cases (e.g., SI Version)
    return 0;
  }

  // TODO: the folowing two checks work only in single threaded
  //   assert(aPos < mNumSeps);
  //   assert(bPos < mNumSeps);
  uint32_t limit = min(mSlot[slotA].mKeySizeWithoutPrefix, mSlot[slotB].mKeySizeWithoutPrefix);
  uint8_t *a = KeyDataWithoutPrefix(slotA), *b = KeyDataWithoutPrefix(slotB);
  uint32_t i;
  for (i = 0; i < limit; i++)
    if (a[i] != b[i])
      break;
  return i;
}

BTreeNode::SeparatorInfo BTreeNode::FindSep() {
  LS_DCHECK(mNumSeps > 1);

  // Inner nodes are split in the middle
  if (IsInner()) {
    uint16_t slotId = mNumSeps / 2;
    return SeparatorInfo{GetFullKeyLen(slotId), slotId, false};
  }

  // Find good separator slot
  uint16_t bestPrefixLength, bestSlot;
  if (mNumSeps > 16) {
    uint16_t lower = (mNumSeps / 2) - (mNumSeps / 16);
    uint16_t upper = (mNumSeps / 2);

    bestPrefixLength = CommonPrefix(lower, 0);
    bestSlot = lower;

    if (bestPrefixLength != CommonPrefix(upper - 1, 0))
      for (bestSlot = lower + 1;
           (bestSlot < upper) && (CommonPrefix(bestSlot, 0) == bestPrefixLength); bestSlot++)
        ;
  } else {
    bestSlot = (mNumSeps - 1) / 2;
    // bestPrefixLength = CommonPrefix(bestSlot, 0);
  }

  // Try to truncate separator
  uint16_t common = CommonPrefix(bestSlot, bestSlot + 1);
  if ((bestSlot + 1 < mNumSeps) && (mSlot[bestSlot].mKeySizeWithoutPrefix > common) &&
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
  if (slotId == mNumSeps) {
    LS_DCHECK(!mRightMostChildSwip.IsEmpty());
    return mRightMostChildSwip;
  }
  auto* childSwip = ChildSwip(slotId);
  LS_DCHECK(!childSwip->IsEmpty(), "childSwip is empty, slotId={}", slotId);
  return *childSwip;
}

//! This = right
//! PRE: current, xGuardedParent and xGuardedLeft are x locked
//! assert(sepSlot > 0);
//! TODO: really ?
void BTreeNode::Split(ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedParent,
                      ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedNewLeft,
                      const BTreeNode::SeparatorInfo& sepInfo) {
  LS_DCHECK(xGuardedParent->CanInsert(sepInfo.mSize, sizeof(Swip)));

  // generate separator key
  uint8_t sepKey[sepInfo.mSize];
  generateSeparator(sepInfo, sepKey);

  xGuardedNewLeft->SetFences(GetLowerFence(), Slice(sepKey, sepInfo.mSize));

  uint8_t tmpRightBuf[BTreeNode::Size()];
  auto* tmpRight = BTreeNode::Init(tmpRightBuf, mIsLeaf);

  tmpRight->SetFences(Slice(sepKey, sepInfo.mSize), GetUpperFence());
  auto swip = xGuardedNewLeft.swip();
  xGuardedParent->Insert(Slice(sepKey, sepInfo.mSize),
                         Slice(reinterpret_cast<uint8_t*>(&swip), sizeof(Swip)));
  if (mIsLeaf) {
    CopyKeyValueRange(xGuardedNewLeft.GetPagePayload(), 0, 0, sepInfo.mSlotId + 1);
    CopyKeyValueRange(tmpRight, 0, xGuardedNewLeft->mNumSeps, mNumSeps - xGuardedNewLeft->mNumSeps);
    tmpRight->mHasGarbage = mHasGarbage;
    xGuardedNewLeft->mHasGarbage = mHasGarbage;
  } else {
    CopyKeyValueRange(xGuardedNewLeft.GetPagePayload(), 0, 0, sepInfo.mSlotId);
    CopyKeyValueRange(tmpRight, 0, xGuardedNewLeft->mNumSeps + 1,
                      mNumSeps - xGuardedNewLeft->mNumSeps - 1);
    xGuardedNewLeft->mRightMostChildSwip = *ChildSwip(xGuardedNewLeft->mNumSeps);
    tmpRight->mRightMostChildSwip = mRightMostChildSwip;
  }
  xGuardedNewLeft->MakeHint();
  tmpRight->MakeHint();
  memcpy(reinterpret_cast<char*>(this), tmpRight, BTreeNode::Size());
}

bool BTreeNode::RemoveSlot(uint16_t slotId) {
  mSpaceUsed -= KeySizeWithoutPrefix(slotId) + ValSize(slotId);
  memmove(mSlot + slotId, mSlot + slotId + 1, sizeof(Slot) * (mNumSeps - slotId - 1));
  mNumSeps--;
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
  mSpaceUsed = mUpperFence.mLength + mLowerFence.mLength;
  mDataOffset = BTreeNode::Size() - mSpaceUsed;
  mNumSeps = 0;
}

using leanstore::utils::AddMemberToJson;

void BTreeNode::ToJson(rapidjson::Value* resultObj, rapidjson::Value::AllocatorType& allocator) {
  LS_DCHECK(resultObj->IsObject());

  auto lowerFence = GetLowerFence();
  if (lowerFence.size() == 0) {
    AddMemberToJson(resultObj, allocator, "mLowerFence", "-inf");
  } else {
    AddMemberToJson(resultObj, allocator, "mLowerFence", lowerFence);
  }

  auto upperFence = GetUpperFence();
  if (upperFence.size() == 0) {
    AddMemberToJson(resultObj, allocator, "mUpperFence", "+inf");
  } else {
    AddMemberToJson(resultObj, allocator, "mUpperFence", upperFence);
  }

  AddMemberToJson(resultObj, allocator, "mNumSeps", mNumSeps);
  AddMemberToJson(resultObj, allocator, "mIsLeaf", mIsLeaf);
  AddMemberToJson(resultObj, allocator, "mSpaceUsed", mSpaceUsed);
  AddMemberToJson(resultObj, allocator, "mDataOffset", mDataOffset);
  AddMemberToJson(resultObj, allocator, "mPrefixSize", mPrefixSize);

  // hints
  {
    rapidjson::Value memberArray(rapidjson::kArrayType);
    for (auto i = 0; i < sHintCount; ++i) {
      rapidjson::Value hintJson;
      hintJson.SetUint64(mHint[i]);
      memberArray.PushBack(hintJson, allocator);
    }
    resultObj->AddMember("mHints", memberArray, allocator);
  }

  AddMemberToJson(resultObj, allocator, "mHasGarbage", mHasGarbage);

  // slots
  {
    rapidjson::Value memberArray(rapidjson::kArrayType);
    for (auto i = 0; i < mNumSeps; ++i) {
      rapidjson::Value arrayElement(rapidjson::kObjectType);
      AddMemberToJson(&arrayElement, allocator, "mOffset", static_cast<uint64_t>(mSlot[i].mOffset));
      AddMemberToJson(&arrayElement, allocator, "mKeyLen",
                      static_cast<uint64_t>(mSlot[i].mKeySizeWithoutPrefix));
      AddMemberToJson(&arrayElement, allocator, "mKey", KeyWithoutPrefix(i));
      AddMemberToJson(&arrayElement, allocator, "mPayloadLen",
                      static_cast<uint64_t>(mSlot[i].mValSize));
      AddMemberToJson(&arrayElement, allocator, "mHead", static_cast<uint64_t>(mSlot[i].mHead));
      memberArray.PushBack(arrayElement, allocator);
    }
    resultObj->AddMember("mSlots", memberArray, allocator);
  }
}

int32_t BTreeNode::CmpKeys(Slice lhs, Slice rhs) {
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
