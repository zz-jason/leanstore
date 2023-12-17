#include "BTreeNode.hpp"

#include "storage/btree/core/BTreeVisitor.hpp"
#include "sync-primitives/GuardedBufferFrame.hpp"
#include "utils/JsonUtil.hpp"

#include <gflags/gflags.h>

#include <alloca.h>

namespace leanstore {
namespace storage {
namespace btree {

void BTreeNode::makeHint() {
  u16 dist = mNumSeps / (sHintCount + 1);
  for (u16 i = 0; i < sHintCount; i++)
    hint[i] = slot[dist * (i + 1)].head;
}

void BTreeNode::updateHint(u16 slotId) {
  u16 dist = mNumSeps / (sHintCount + 1);
  u16 begin = 0;
  if ((mNumSeps > sHintCount * 2 + 1) &&
      (((mNumSeps - 1) / (sHintCount + 1)) == dist) && ((slotId / dist) > 1))
    begin = (slotId / dist) - 1;
  for (u16 i = begin; i < sHintCount; i++)
    hint[i] = slot[dist * (i + 1)].head;
  for (u16 i = 0; i < sHintCount; i++)
    assert(hint[i] == slot[dist * (i + 1)].head);
}

u16 BTreeNode::spaceNeeded(u16 keySize, u16 valSize, u16 prefix_len) {
  return sizeof(Slot) + (keySize - prefix_len) + valSize;
}

u16 BTreeNode::spaceNeeded(u16 keySize, u16 valSize) {
  return spaceNeeded(keySize, valSize, mPrefixSize);
}

bool BTreeNode::canInsert(u16 keySize, u16 valSize) {
  const u16 space_needed = spaceNeeded(keySize, valSize);
  if (!hasEnoughSpaceFor(space_needed))
    return false; // no space, insert fails
  else
    return true;
}

bool BTreeNode::prepareInsert(u16 keySize, u16 valSize) {
  const u16 space_needed = spaceNeeded(keySize, valSize);
  if (!requestSpaceFor(space_needed))
    return false; // no space, insert fails
  else
    return true;
}

s16 BTreeNode::insertDoNotCopyPayload(Slice key, u16 valSize, s32 pos) {
  assert(canInsert(key.size(), valSize));
  prepareInsert(key.size(), valSize);

  s32 slotId = (pos == -1) ? lowerBound<false>(key) : pos;
  memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (mNumSeps - slotId));

  // StoreKeyValue
  key.remove_prefix(mPrefixSize);

  slot[slotId].head = head(key);
  slot[slotId].mKeySizeWithoutPrefix = key.size();
  slot[slotId].mValSize = valSize;
  const u16 space = key.size() + valSize;
  mDataOffset -= space;
  mSpaceUsed += space;
  slot[slotId].offset = mDataOffset;
  memcpy(KeyDataWithoutPrefix(slotId), key.data(), key.size());

  mNumSeps++;
  updateHint(slotId);
  return slotId;
}

s32 BTreeNode::insert(Slice key, Slice val) {
  DEBUG_BLOCK() {
    assert(canInsert(key.size(), val.size()));
    s32 exact_pos = lowerBound<true>(key);
    static_cast<void>(exact_pos);
    assert(exact_pos == -1); // assert for duplicates
  }

  prepareInsert(key.size(), val.size());
  s32 slotId = lowerBound<false>(key);
  memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (mNumSeps - slotId));
  storeKeyValue(slotId, key, val);
  mNumSeps++;
  updateHint(slotId);
  return slotId;

  DEBUG_BLOCK() {
    s32 exact_pos = lowerBound<true>(key);
    static_cast<void>(exact_pos);
    // assert for duplicates
    assert(exact_pos == slotId);
  }
}

void BTreeNode::compactify() {
  u16 should = freeSpaceAfterCompaction();
  static_cast<void>(should);
  BTreeNode tmp(mIsLeaf);
  tmp.setFences(GetLowerFence(), GetUpperFence());
  copyKeyValueRange(&tmp, 0, 0, mNumSeps);
  tmp.mRightMostChildSwip = mRightMostChildSwip;
  memcpy(reinterpret_cast<char*>(this), &tmp, sizeof(BTreeNode));
  makeHint();
  assert(freeSpace() == should);
}

u32 BTreeNode::mergeSpaceUpperBound(ExclusivePageGuard<BTreeNode>& right) {
  assert(right->mIsLeaf);
  BTreeNode tmp(true);
  tmp.setFences(GetLowerFence(), right->GetUpperFence());
  u32 leftGrow = (mPrefixSize - tmp.mPrefixSize) * mNumSeps;
  u32 rightGrow = (right->mPrefixSize - tmp.mPrefixSize) * right->mNumSeps;
  u32 spaceUpperBound =
      mSpaceUsed + right->mSpaceUsed +
      (reinterpret_cast<u8*>(slot + mNumSeps + right->mNumSeps) - RawPtr()) +
      leftGrow + rightGrow;
  return spaceUpperBound;
}

u32 BTreeNode::spaceUsedBySlot(u16 s_i) {
  return sizeof(BTreeNode::Slot) + KeySizeWithoutPrefix(s_i) + ValSize(s_i);
}
// -------------------------------------------------------------------------------------
// right survives, this gets reclaimed
// left(this) into right
bool BTreeNode::merge(u16 slotId, ExclusivePageGuard<BTreeNode>& parent,
                      ExclusivePageGuard<BTreeNode>& right) {
  if (mIsLeaf) {
    assert(right->mIsLeaf);
    assert(parent->isInner());
    BTreeNode tmp(mIsLeaf);
    tmp.setFences(GetLowerFence(), right->GetUpperFence());
    u16 leftGrow = (mPrefixSize - tmp.mPrefixSize) * mNumSeps;
    u16 rightGrow = (right->mPrefixSize - tmp.mPrefixSize) * right->mNumSeps;
    u16 spaceUpperBound =
        mSpaceUsed + right->mSpaceUsed +
        (reinterpret_cast<u8*>(slot + mNumSeps + right->mNumSeps) - RawPtr()) +
        leftGrow + rightGrow;
    if (spaceUpperBound > EFFECTIVE_PAGE_SIZE) {
      return false;
    }
    copyKeyValueRange(&tmp, 0, 0, mNumSeps);
    right->copyKeyValueRange(&tmp, mNumSeps, 0, right->mNumSeps);
    parent->removeSlot(slotId);
    // -------------------------------------------------------------------------------------
    right->mHasGarbage |= mHasGarbage;
    // -------------------------------------------------------------------------------------
    memcpy(reinterpret_cast<u8*>(right.PageData()), &tmp, sizeof(BTreeNode));
    right->makeHint();
    return true;
  } else { // Inner node
    assert(!right->mIsLeaf);
    assert(parent->isInner());
    BTreeNode tmp(mIsLeaf);
    tmp.setFences(GetLowerFence(), right->GetUpperFence());
    u16 leftGrow = (mPrefixSize - tmp.mPrefixSize) * mNumSeps;
    u16 rightGrow = (right->mPrefixSize - tmp.mPrefixSize) * right->mNumSeps;
    u16 extraKeyLength = parent->getFullKeyLen(slotId);
    u16 spaceUpperBound =
        mSpaceUsed + right->mSpaceUsed +
        (reinterpret_cast<u8*>(slot + mNumSeps + right->mNumSeps) - RawPtr()) +
        leftGrow + rightGrow +
        spaceNeeded(extraKeyLength, sizeof(SwipType), tmp.mPrefixSize);
    if (spaceUpperBound > EFFECTIVE_PAGE_SIZE)
      return false;
    copyKeyValueRange(&tmp, 0, 0, mNumSeps);
    // Allocate in the stack, freed when the calling function exits.
    u8* extraKey = (u8*)alloca(extraKeyLength * sizeof(u8));
    parent->copyFullKey(slotId, extraKey);
    tmp.storeKeyValue(
        mNumSeps, Slice(extraKey, extraKeyLength),
        Slice(reinterpret_cast<u8*>(&mRightMostChildSwip), sizeof(SwipType)));
    tmp.mNumSeps++;
    right->copyKeyValueRange(&tmp, tmp.mNumSeps, 0, right->mNumSeps);
    parent->removeSlot(slotId);
    tmp.mRightMostChildSwip = right->mRightMostChildSwip;
    tmp.makeHint();
    memcpy(reinterpret_cast<u8*>(right.PageData()), &tmp, sizeof(BTreeNode));
    return true;
  }
}

void BTreeNode::storeKeyValue(u16 slotId, Slice key, Slice val) {
  // Head
  key.remove_prefix(mPrefixSize);
  slot[slotId].head = head(key);
  slot[slotId].mKeySizeWithoutPrefix = key.size();
  slot[slotId].mValSize = val.size();

  // Value
  const u16 space = key.size() + val.size();
  mDataOffset -= space;
  mSpaceUsed += space;
  slot[slotId].offset = mDataOffset;
  memcpy(KeyDataWithoutPrefix(slotId), key.data(), key.size());
  memcpy(ValData(slotId), val.data(), val.size());
  assert(RawPtr() + mDataOffset >= reinterpret_cast<u8*>(slot + mNumSeps));
}

// ATTENTION: dstSlot then srcSlot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
void BTreeNode::copyKeyValueRange(BTreeNode* dst, u16 dstSlot, u16 srcSlot,
                                  u16 count) {
  if (mPrefixSize == dst->mPrefixSize) {
    // Fast path
    memcpy(dst->slot + dstSlot, slot + srcSlot, sizeof(Slot) * count);
    DEBUG_BLOCK() {
      u32 total_space_used = mUpperFence.length + mLowerFence.length;
      for (u16 i = 0; i < this->mNumSeps; i++) {
        total_space_used += KeySizeWithoutPrefix(i) + ValSize(i);
      }
      assert(total_space_used == this->mSpaceUsed);
    }
    for (u16 i = 0; i < count; i++) {
      u32 kv_size = KeySizeWithoutPrefix(srcSlot + i) + ValSize(srcSlot + i);
      dst->mDataOffset -= kv_size;
      dst->mSpaceUsed += kv_size;
      dst->slot[dstSlot + i].offset = dst->mDataOffset;
      DEBUG_BLOCK() {
        [[maybe_unused]] s64 off_by =
            reinterpret_cast<u8*>(dst->slot + dstSlot + count) -
            (dst->RawPtr() + dst->mDataOffset);
        assert(off_by <= 0);
      }
      memcpy(dst->RawPtr() + dst->mDataOffset,
             RawPtr() + slot[srcSlot + i].offset, kv_size);
    }
  } else {
    for (u16 i = 0; i < count; i++)
      copyKeyValue(srcSlot + i, dst, dstSlot + i);
  }
  dst->mNumSeps += count;
  assert((dst->RawPtr() + dst->mDataOffset) >=
         reinterpret_cast<u8*>(dst->slot + dst->mNumSeps));
}

void BTreeNode::copyKeyValue(u16 srcSlot, BTreeNode* dst, u16 dstSlot) {
  u16 fullLength = getFullKeyLen(srcSlot);
  auto key = (u8*)alloca(fullLength * sizeof(u8));
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

u16 BTreeNode::commonPrefix(u16 slotA, u16 slotB) {
  if (mNumSeps == 0) {
    // Do not prefix compress if only one tuple is in to
    // avoid corner cases (e.g., SI Version)
    return 0;
  } else {
    // TODO: the folowing two checks work only in single threaded
    //   assert(aPos < mNumSeps);
    //   assert(bPos < mNumSeps);
    u32 limit = min(slot[slotA].mKeySizeWithoutPrefix,
                    slot[slotB].mKeySizeWithoutPrefix);
    u8 *a = KeyDataWithoutPrefix(slotA), *b = KeyDataWithoutPrefix(slotB);
    u32 i;
    for (i = 0; i < limit; i++)
      if (a[i] != b[i])
        break;
    return i;
  }
}

BTreeNode::SeparatorInfo BTreeNode::findSep() {
  assert(mNumSeps > 1);
  if (isInner()) {
    // Inner nodes are split in the middle
    u16 slotId = mNumSeps / 2;
    return SeparatorInfo{
        static_cast<u16>(mPrefixSize + slot[slotId].mKeySizeWithoutPrefix),
        slotId, false};
  }

  // Find good separator slot
  u16 bestPrefixLength, bestSlot;
  if (mNumSeps > 16) {
    u16 lower = (mNumSeps / 2) - (mNumSeps / 16);
    u16 upper = (mNumSeps / 2);

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
  u16 common = commonPrefix(bestSlot, bestSlot + 1);
  if ((bestSlot + 1 < mNumSeps) &&
      (slot[bestSlot].mKeySizeWithoutPrefix > common) &&
      (slot[bestSlot + 1].mKeySizeWithoutPrefix > (common + 1)))
    return SeparatorInfo{static_cast<u16>(mPrefixSize + common + 1), bestSlot,
                         true};

  return SeparatorInfo{
      static_cast<u16>(mPrefixSize + slot[bestSlot].mKeySizeWithoutPrefix),
      bestSlot, false};
}

void BTreeNode::getSep(u8* sepKeyOut, BTreeNodeHeader::SeparatorInfo info) {
  memcpy(sepKeyOut, getLowerFenceKey(), mPrefixSize);
  if (info.trunc) {
    memcpy(sepKeyOut + mPrefixSize, KeyDataWithoutPrefix(info.slot + 1),
           info.length - mPrefixSize);
  } else {
    memcpy(sepKeyOut + mPrefixSize, KeyDataWithoutPrefix(info.slot),
           info.length - mPrefixSize);
  }
}

s32 BTreeNode::compareKeyWithBoundaries(Slice key) {
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

Swip<BTreeNode>& BTreeNode::lookupInner(Slice key) {
  s32 slotId = lowerBound<false>(key);
  if (slotId == mNumSeps) {
    return mRightMostChildSwip;
  }
  return getChild(slotId);
}

// This = right
void BTreeNode::split(ExclusivePageGuard<BTreeNode>& guardedParent,
                      ExclusivePageGuard<BTreeNode>& guardedLeft, u16 sepSlot,
                      u8* sepKey, u16 sepLength) {
  // PRE: current, guardedParent and guardedLeft are x locked
  // assert(sepSlot > 0); TODO: really ?
  assert(sepSlot < (EFFECTIVE_PAGE_SIZE / sizeof(SwipType)));
  assert(guardedParent->canInsert(sepLength, sizeof(SwipType)));

  guardedLeft->setFences(GetLowerFence(), Slice(sepKey, sepLength));
  BTreeNode tmp(mIsLeaf);
  BTreeNode* nodeRight = &tmp;
  nodeRight->setFences(Slice(sepKey, sepLength), GetUpperFence());
  auto swip = guardedLeft.swip();
  guardedParent->insert(Slice(sepKey, sepLength),
                        Slice(reinterpret_cast<u8*>(&swip), sizeof(SwipType)));
  if (mIsLeaf) {
    copyKeyValueRange(guardedLeft.PageData(), 0, 0, sepSlot + 1);
    copyKeyValueRange(nodeRight, 0, guardedLeft->mNumSeps,
                      mNumSeps - guardedLeft->mNumSeps);
    nodeRight->mHasGarbage = mHasGarbage;
    guardedLeft->mHasGarbage = mHasGarbage;
  } else {
    copyKeyValueRange(guardedLeft.PageData(), 0, 0, sepSlot);
    copyKeyValueRange(nodeRight, 0, guardedLeft->mNumSeps + 1,
                      mNumSeps - guardedLeft->mNumSeps - 1);
    guardedLeft->mRightMostChildSwip = getChild(guardedLeft->mNumSeps);
    nodeRight->mRightMostChildSwip = mRightMostChildSwip;
  }
  guardedLeft->makeHint();
  nodeRight->makeHint();
  memcpy(reinterpret_cast<char*>(this), nodeRight, sizeof(BTreeNode));
}

bool BTreeNode::removeSlot(u16 slotId) {
  mSpaceUsed -= KeySizeWithoutPrefix(slotId) + ValSize(slotId);
  memmove(slot + slotId, slot + slotId + 1,
          sizeof(Slot) * (mNumSeps - slotId - 1));
  mNumSeps--;
  makeHint();
  return true;
}

bool BTreeNode::remove(Slice key) {
  int slotId = lowerBound<true>(key);
  if (slotId == -1) {
    // key not found
    return false;
  }
  return removeSlot(slotId);
}

void BTreeNode::reset() {
  mSpaceUsed = mUpperFence.length + mLowerFence.length;
  mDataOffset = EFFECTIVE_PAGE_SIZE - mSpaceUsed;
  mNumSeps = 0;
}

void BTreeNode::Accept(BTreeVisitor* visitor) {
  visitor->Visit(this);
}

using leanstore::utils::AddMemberToJson;

void BTreeNode::ToJSON(rapidjson::Value* resultObj,
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
                      static_cast<u64>(slot[i].offset));
      AddMemberToJson(&arrayElement, allocator, "mKeyLen",
                      static_cast<u64>(slot[i].mKeySizeWithoutPrefix));
      AddMemberToJson(&arrayElement, allocator, "mPayloadLen",
                      static_cast<u64>(slot[i].mValSize));
      AddMemberToJson(&arrayElement, allocator, "mHead",
                      static_cast<u64>(slot[i].head));
      memberArray.PushBack(arrayElement, allocator);
    }
    resultObj->AddMember("mSlots", memberArray, allocator);
  }
}

} // namespace btree
} // namespace storage
} // namespace leanstore
