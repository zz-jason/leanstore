#include "BTreeNode.hpp"

#include "buffer-manager/GuardedBufferFrame.hpp"
#include "utils/JsonUtil.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

namespace leanstore::storage::btree {

void BTreeNode::makeHint() {
  uint16_t dist = mNumSeps / (sHintCount + 1);
  for (uint16_t i = 0; i < sHintCount; i++)
    hint[i] = slot[dist * (i + 1)].head;
}

void BTreeNode::updateHint(uint16_t slotId) {
  uint16_t dist = mNumSeps / (sHintCount + 1);
  uint16_t begin = 0;
  if ((mNumSeps > sHintCount * 2 + 1) &&
      (((mNumSeps - 1) / (sHintCount + 1)) == dist) && ((slotId / dist) > 1))
    begin = (slotId / dist) - 1;
  for (uint16_t i = begin; i < sHintCount; i++)
    hint[i] = slot[dist * (i + 1)].head;
  for (uint16_t i = 0; i < sHintCount; i++)
    assert(hint[i] == slot[dist * (i + 1)].head);
}

uint16_t BTreeNode::spaceNeeded(uint16_t keySize, uint16_t valSize,
                                uint16_t prefixSize) {
  return sizeof(Slot) + (keySize - prefixSize) + valSize;
}

uint16_t BTreeNode::spaceNeeded(uint16_t keySize, uint16_t valSize) {
  return spaceNeeded(keySize, valSize, mPrefixSize);
}

bool BTreeNode::canInsert(uint16_t keySize, uint16_t valSize) {
  const uint16_t numSpaceNeeded = spaceNeeded(keySize, valSize);
  if (!hasEnoughSpaceFor(numSpaceNeeded)) {
    return false; // no space, insert fails
  }
  return true;
}

bool BTreeNode::prepareInsert(uint16_t keySize, uint16_t valSize) {
  const uint16_t numSpaceNeeded = spaceNeeded(keySize, valSize);
  if (!requestSpaceFor(numSpaceNeeded)) {
    return false;
  }
  return true;
}

int16_t BTreeNode::insertDoNotCopyPayload(Slice key, uint16_t valSize,
                                          int32_t pos) {
  DCHECK(canInsert(key.size(), valSize));
  prepareInsert(key.size(), valSize);

  int32_t slotId = (pos == -1) ? lowerBound<false>(key) : pos;
  memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (mNumSeps - slotId));

  // StoreKeyValue
  key.remove_prefix(mPrefixSize);

  slot[slotId].head = head(key);
  slot[slotId].mKeySizeWithoutPrefix = key.size();
  slot[slotId].mValSize = valSize;
  const uint16_t space = key.size() + valSize;
  mDataOffset -= space;
  mSpaceUsed += space;
  slot[slotId].offset = mDataOffset;
  memcpy(KeyDataWithoutPrefix(slotId), key.data(), key.size());

  mNumSeps++;
  updateHint(slotId);
  return slotId;
}

int32_t BTreeNode::Insert(Slice key, Slice val) {
  DEBUG_BLOCK() {
    assert(canInsert(key.size(), val.size()));
    int32_t exactPos = lowerBound<true>(key);
    static_cast<void>(exactPos);
    assert(exactPos == -1); // assert for duplicates
  }

  prepareInsert(key.size(), val.size());
  int32_t slotId = lowerBound<false>(key);
  memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (mNumSeps - slotId));
  storeKeyValue(slotId, key, val);
  mNumSeps++;
  updateHint(slotId);
  return slotId;

  DEBUG_BLOCK() {
    int32_t exactPos = lowerBound<true>(key);
    static_cast<void>(exactPos);
    // assert for duplicates
    assert(exactPos == slotId);
  }
}

void BTreeNode::compactify() {
  uint16_t should = FreeSpaceAfterCompaction();
  static_cast<void>(should);

  auto tmpNodeBuf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
  auto* tmp = BTreeNode::Init(tmpNodeBuf->get(), mIsLeaf);

  tmp->setFences(GetLowerFence(), GetUpperFence());
  copyKeyValueRange(tmp, 0, 0, mNumSeps);
  tmp->mRightMostChildSwip = mRightMostChildSwip;
  memcpy(reinterpret_cast<char*>(this), tmp, BTreeNode::Size());
  makeHint();
  assert(freeSpace() == should);
}

uint32_t BTreeNode::mergeSpaceUpperBound(
    ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedRight) {
  DCHECK(xGuardedRight->mIsLeaf);

  auto tmpNodeBuf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
  auto* tmp = BTreeNode::Init(tmpNodeBuf->get(), true);

  tmp->setFences(GetLowerFence(), xGuardedRight->GetUpperFence());
  uint32_t leftGrow = (mPrefixSize - tmp->mPrefixSize) * mNumSeps;
  uint32_t rightGrow =
      (xGuardedRight->mPrefixSize - tmp->mPrefixSize) * xGuardedRight->mNumSeps;
  uint32_t spaceUpperBound =
      mSpaceUsed + xGuardedRight->mSpaceUsed +
      (reinterpret_cast<uint8_t*>(slot + mNumSeps + xGuardedRight->mNumSeps) -
       RawPtr()) +
      leftGrow + rightGrow;
  return spaceUpperBound;
}

uint32_t BTreeNode::spaceUsedBySlot(uint16_t slotId) {
  return sizeof(BTreeNode::Slot) + KeySizeWithoutPrefix(slotId) +
         ValSize(slotId);
}

// right survives, this gets reclaimed
// left(this) into right
bool BTreeNode::merge(uint16_t slotId,
                      ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedParent,
                      ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedRight) {
  if (mIsLeaf) {
    assert(xGuardedRight->mIsLeaf);
    assert(xGuardedParent->isInner());

    auto tmpNodeBuf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
    auto* tmp = BTreeNode::Init(tmpNodeBuf->get(), true);

    tmp->setFences(GetLowerFence(), xGuardedRight->GetUpperFence());
    uint16_t leftGrow = (mPrefixSize - tmp->mPrefixSize) * mNumSeps;
    uint16_t rightGrow = (xGuardedRight->mPrefixSize - tmp->mPrefixSize) *
                         xGuardedRight->mNumSeps;
    uint16_t spaceUpperBound =
        mSpaceUsed + xGuardedRight->mSpaceUsed +
        (reinterpret_cast<uint8_t*>(slot + mNumSeps + xGuardedRight->mNumSeps) -
         RawPtr()) +
        leftGrow + rightGrow;
    if (spaceUpperBound > BTreeNode::Size()) {
      return false;
    }
    copyKeyValueRange(tmp, 0, 0, mNumSeps);
    xGuardedRight->copyKeyValueRange(tmp, mNumSeps, 0, xGuardedRight->mNumSeps);
    xGuardedParent->removeSlot(slotId);

    xGuardedRight->mHasGarbage |= mHasGarbage;

    memcpy(xGuardedRight.GetPagePayloadPtr(), tmp, BTreeNode::Size());
    xGuardedRight->makeHint();
    return true;
  }

  // Inner node
  DCHECK(!xGuardedRight->mIsLeaf);
  DCHECK(xGuardedParent->isInner());

  auto tmpNodeBuf = utils::JumpScopedArray<uint8_t>(BTreeNode::Size());
  auto* tmp = BTreeNode::Init(tmpNodeBuf->get(), mIsLeaf);

  tmp->setFences(GetLowerFence(), xGuardedRight->GetUpperFence());
  uint16_t leftGrow = (mPrefixSize - tmp->mPrefixSize) * mNumSeps;
  uint16_t rightGrow =
      (xGuardedRight->mPrefixSize - tmp->mPrefixSize) * xGuardedRight->mNumSeps;
  uint16_t extraKeyLength = xGuardedParent->getFullKeyLen(slotId);
  uint16_t spaceUpperBound =
      mSpaceUsed + xGuardedRight->mSpaceUsed +
      (reinterpret_cast<uint8_t*>(slot + mNumSeps + xGuardedRight->mNumSeps) -
       RawPtr()) +
      leftGrow + rightGrow +
      spaceNeeded(extraKeyLength, sizeof(Swip), tmp->mPrefixSize);
  if (spaceUpperBound > BTreeNode::Size())
    return false;
  copyKeyValueRange(tmp, 0, 0, mNumSeps);
  // Allocate in the stack, freed when the calling function exits.
  auto extraKey = utils::JumpScopedArray<uint8_t>(extraKeyLength);
  xGuardedParent->copyFullKey(slotId, extraKey->get());
  tmp->storeKeyValue(
      mNumSeps, Slice(extraKey->get(), extraKeyLength),
      Slice(reinterpret_cast<uint8_t*>(&mRightMostChildSwip), sizeof(Swip)));
  tmp->mNumSeps++;
  xGuardedRight->copyKeyValueRange(tmp, tmp->mNumSeps, 0,
                                   xGuardedRight->mNumSeps);
  xGuardedParent->removeSlot(slotId);
  tmp->mRightMostChildSwip = xGuardedRight->mRightMostChildSwip;
  tmp->makeHint();
  memcpy(xGuardedRight.GetPagePayloadPtr(), tmp, BTreeNode::Size());
  return true;
}

void BTreeNode::storeKeyValue(uint16_t slotId, Slice key, Slice val) {
  // Head
  key.remove_prefix(mPrefixSize);
  slot[slotId].head = head(key);
  slot[slotId].mKeySizeWithoutPrefix = key.size();
  slot[slotId].mValSize = val.size();

  // Value
  const uint16_t space = key.size() + val.size();
  mDataOffset -= space;
  mSpaceUsed += space;
  slot[slotId].offset = mDataOffset;
  memcpy(KeyDataWithoutPrefix(slotId), key.data(), key.size());
  memcpy(ValData(slotId), val.data(), val.size());
  assert(RawPtr() + mDataOffset >= reinterpret_cast<uint8_t*>(slot + mNumSeps));
}

// ATTENTION: dstSlot then srcSlot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
void BTreeNode::copyKeyValueRange(BTreeNode* dst, uint16_t dstSlot,
                                  uint16_t srcSlot, uint16_t count) {
  if (mPrefixSize == dst->mPrefixSize) {
    // Fast path
    memcpy(dst->slot + dstSlot, slot + srcSlot, sizeof(Slot) * count);
    DEBUG_BLOCK() {
      uint32_t totalSpaceUsed = mUpperFence.length + mLowerFence.length;
      for (uint16_t i = 0; i < this->mNumSeps; i++) {
        totalSpaceUsed += KeySizeWithoutPrefix(i) + ValSize(i);
      }
      assert(totalSpaceUsed == this->mSpaceUsed);
    }
    for (uint16_t i = 0; i < count; i++) {
      uint32_t kvSize =
          KeySizeWithoutPrefix(srcSlot + i) + ValSize(srcSlot + i);
      dst->mDataOffset -= kvSize;
      dst->mSpaceUsed += kvSize;
      dst->slot[dstSlot + i].offset = dst->mDataOffset;
      DEBUG_BLOCK() {
        [[maybe_unused]] int64_t offBy =
            reinterpret_cast<uint8_t*>(dst->slot + dstSlot + count) -
            (dst->RawPtr() + dst->mDataOffset);
        assert(offBy <= 0);
      }
      memcpy(dst->RawPtr() + dst->mDataOffset,
             RawPtr() + slot[srcSlot + i].offset, kvSize);
    }
  } else {
    for (uint16_t i = 0; i < count; i++)
      copyKeyValue(srcSlot + i, dst, dstSlot + i);
  }
  dst->mNumSeps += count;
  assert((dst->RawPtr() + dst->mDataOffset) >=
         reinterpret_cast<uint8_t*>(dst->slot + dst->mNumSeps));
}

void BTreeNode::copyKeyValue(uint16_t srcSlot, BTreeNode* dst,
                             uint16_t dstSlot) {
  uint16_t fullLength = getFullKeyLen(srcSlot);
  auto keyBuf = utils::JumpScopedArray<uint8_t>(fullLength);
  auto* key = keyBuf->get();
  copyFullKey(srcSlot, key);
  dst->storeKeyValue(dstSlot, Slice(key, fullLength), Value(srcSlot));
}

void BTreeNode::insertFence(BTreeNodeHeader::FenceKey& fk, Slice key) {
  if (!key.data()) {
    return;
  }
  assert(freeSpace() >= key.size());

  mDataOffset -= key.size();
  mSpaceUsed += key.size();
  fk.offset = mDataOffset;
  fk.length = key.size();
  memcpy(RawPtr() + mDataOffset, key.data(), key.size());
}

void BTreeNode::setFences(Slice lowerKey, Slice upperKey) {
  insertFence(mLowerFence, lowerKey);
  insertFence(mUpperFence, upperKey);
  DCHECK(getLowerFenceKey() == nullptr || getUpperFenceKey() == nullptr ||
         *getLowerFenceKey() <= *getUpperFenceKey());

  if (FLAGS_btree_prefix_compression) {
    for (mPrefixSize = 0;
         (mPrefixSize < min(lowerKey.size(), upperKey.size())) &&
         (lowerKey[mPrefixSize] == upperKey[mPrefixSize]);
         mPrefixSize++)
      ;
  } else {
    mPrefixSize = 0;
  }
}

uint16_t BTreeNode::commonPrefix(uint16_t slotA, uint16_t slotB) {
  if (mNumSeps == 0) {
    // Do not prefix compress if only one tuple is in to
    // avoid corner cases (e.g., SI Version)
    return 0;
  }

  // TODO: the folowing two checks work only in single threaded
  //   assert(aPos < mNumSeps);
  //   assert(bPos < mNumSeps);
  uint32_t limit =
      min(slot[slotA].mKeySizeWithoutPrefix, slot[slotB].mKeySizeWithoutPrefix);
  uint8_t *a = KeyDataWithoutPrefix(slotA), *b = KeyDataWithoutPrefix(slotB);
  uint32_t i;
  for (i = 0; i < limit; i++)
    if (a[i] != b[i])
      break;
  return i;
}

BTreeNode::SeparatorInfo BTreeNode::findSep() {
  DCHECK(mNumSeps > 1);

  // Inner nodes are split in the middle
  if (isInner()) {
    uint16_t slotId = mNumSeps / 2;
    return SeparatorInfo{getFullKeyLen(slotId), slotId, false};
  }

  // Find good separator slot
  uint16_t bestPrefixLength, bestSlot;
  if (mNumSeps > 16) {
    uint16_t lower = (mNumSeps / 2) - (mNumSeps / 16);
    uint16_t upper = (mNumSeps / 2);

    bestPrefixLength = commonPrefix(lower, 0);
    bestSlot = lower;

    if (bestPrefixLength != commonPrefix(upper - 1, 0))
      for (bestSlot = lower + 1;
           (bestSlot < upper) &&
           (commonPrefix(bestSlot, 0) == bestPrefixLength);
           bestSlot++)
        ;
  } else {
    bestSlot = (mNumSeps - 1) / 2;
    // bestPrefixLength = commonPrefix(bestSlot, 0);
  }

  // Try to truncate separator
  uint16_t common = commonPrefix(bestSlot, bestSlot + 1);
  if ((bestSlot + 1 < mNumSeps) &&
      (slot[bestSlot].mKeySizeWithoutPrefix > common) &&
      (slot[bestSlot + 1].mKeySizeWithoutPrefix > (common + 1)))
    return SeparatorInfo{static_cast<uint16_t>(mPrefixSize + common + 1),
                         bestSlot, true};

  return SeparatorInfo{getFullKeyLen(bestSlot), bestSlot, false};
}

int32_t BTreeNode::compareKeyWithBoundaries(Slice key) {
  // Lower Bound exclusive, upper bound inclusive
  if (mLowerFence.offset) {
    int cmp = CmpKeys(key, GetLowerFence());
    if (!(cmp > 0))
      return 1; // Key lower or equal LF
  }

  if (mUpperFence.offset) {
    int cmp = CmpKeys(key, GetUpperFence());
    if (!(cmp <= 0))
      return -1; // Key higher than UF
  }
  return 0;
}

Swip& BTreeNode::lookupInner(Slice key) {
  int32_t slotId = lowerBound<false>(key);
  if (slotId == mNumSeps) {
    return mRightMostChildSwip;
  }
  return *ChildSwip(slotId);
}

/// This = right
/// PRE: current, xGuardedParent and xGuardedLeft are x locked
/// assert(sepSlot > 0);
/// TODO: really ?
void BTreeNode::Split(ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedParent,
                      ExclusiveGuardedBufferFrame<BTreeNode>& xGuardedNewLeft,
                      const BTreeNode::SeparatorInfo& sepInfo) {
  DCHECK(xGuardedParent->canInsert(sepInfo.mSize, sizeof(Swip)));

  // generate separator key
  uint8_t sepKey[sepInfo.mSize];
  generateSeparator(sepInfo, sepKey);

  xGuardedNewLeft->setFences(GetLowerFence(), Slice(sepKey, sepInfo.mSize));

  uint8_t tmpRightBuf[BTreeNode::Size()];
  auto* tmpRight = BTreeNode::Init(tmpRightBuf, mIsLeaf);

  tmpRight->setFences(Slice(sepKey, sepInfo.mSize), GetUpperFence());
  auto swip = xGuardedNewLeft.swip();
  xGuardedParent->Insert(
      Slice(sepKey, sepInfo.mSize),
      Slice(reinterpret_cast<uint8_t*>(&swip), sizeof(Swip)));
  if (mIsLeaf) {
    copyKeyValueRange(xGuardedNewLeft.GetPagePayload(), 0, 0,
                      sepInfo.mSlotId + 1);
    copyKeyValueRange(tmpRight, 0, xGuardedNewLeft->mNumSeps,
                      mNumSeps - xGuardedNewLeft->mNumSeps);
    tmpRight->mHasGarbage = mHasGarbage;
    xGuardedNewLeft->mHasGarbage = mHasGarbage;
  } else {
    copyKeyValueRange(xGuardedNewLeft.GetPagePayload(), 0, 0, sepInfo.mSlotId);
    copyKeyValueRange(tmpRight, 0, xGuardedNewLeft->mNumSeps + 1,
                      mNumSeps - xGuardedNewLeft->mNumSeps - 1);
    xGuardedNewLeft->mRightMostChildSwip =
        *ChildSwip(xGuardedNewLeft->mNumSeps);
    tmpRight->mRightMostChildSwip = mRightMostChildSwip;
  }
  xGuardedNewLeft->makeHint();
  tmpRight->makeHint();
  memcpy(reinterpret_cast<char*>(this), tmpRight, BTreeNode::Size());
}

bool BTreeNode::removeSlot(uint16_t slotId) {
  mSpaceUsed -= KeySizeWithoutPrefix(slotId) + ValSize(slotId);
  memmove(slot + slotId, slot + slotId + 1,
          sizeof(Slot) * (mNumSeps - slotId - 1));
  mNumSeps--;
  makeHint();
  return true;
}

bool BTreeNode::Remove(Slice key) {
  int slotId = lowerBound<true>(key);
  if (slotId == -1) {
    // key not found
    return false;
  }
  return removeSlot(slotId);
}

void BTreeNode::Reset() {
  mSpaceUsed = mUpperFence.length + mLowerFence.length;
  mDataOffset = BTreeNode::Size() - mSpaceUsed;
  mNumSeps = 0;
}

using leanstore::utils::AddMemberToJson;

void BTreeNode::ToJson(rapidjson::Value* resultObj,
                       rapidjson::Value::AllocatorType& allocator) {
  DCHECK(resultObj->IsObject());

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
      hintJson.SetUint64(hint[i]);
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
      AddMemberToJson(&arrayElement, allocator, "mOffset",
                      static_cast<uint64_t>(slot[i].offset));
      AddMemberToJson(&arrayElement, allocator, "mKeyLen",
                      static_cast<uint64_t>(slot[i].mKeySizeWithoutPrefix));
      AddMemberToJson(&arrayElement, allocator, "mKey", KeyWithoutPrefix(i));
      AddMemberToJson(&arrayElement, allocator, "mPayloadLen",
                      static_cast<uint64_t>(slot[i].mValSize));
      AddMemberToJson(&arrayElement, allocator, "mHead",
                      static_cast<uint64_t>(slot[i].head));
      memberArray.PushBack(arrayElement, allocator);
    }
    resultObj->AddMember("mSlots", memberArray, allocator);
  }
}

} // namespace leanstore::storage::btree
