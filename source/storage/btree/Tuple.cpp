#include "Tuple.hpp"

#include "TransactionKV.hpp"
#include "concurrency-recovery/Worker.hpp"
#include "storage/btree/BasicKV.hpp"
#include "storage/btree/ChainedTuple.hpp"
#include "storage/btree/core/BTreeNode.hpp"
#include "utils/Misc.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <unordered_map>

using namespace std;
using namespace leanstore::storage;
using OpCode = leanstore::OpCode;

namespace std {

template <> class hash<leanstore::UpdateSlotInfo> {
public:
  size_t operator()(const leanstore::UpdateSlotInfo& slot) const {
    size_t result = (slot.mSize << 16) | slot.mOffset;
    return result;
  }
};

} // namespace std

namespace leanstore::storage::btree {

static_assert(sizeof(FatTuple) >= sizeof(ChainedTuple));

// -----------------------------------------------------------------------------
// Tuple
// -----------------------------------------------------------------------------

static u64 MaxFatTupleLength() {
  return BTreeNode::Size() - 1000;
}

bool Tuple::ToFat(BTreeExclusiveIterator& xIter) {
  utils::Timer timer(CRCounters::MyCounters().cc_ms_fat_tuple_conversion);

  // Process the chain tuple
  MutableSlice mutRawVal = xIter.MutableVal();
  auto& chainedTuple = *ChainedTuple::From(mutRawVal.Data());
  DCHECK(chainedTuple.IsWriteLocked());
  DCHECK(chainedTuple.mFormat == TupleFormat::kChained);

  auto tmpBufSize = MaxFatTupleLength();
  auto tmpBuf = utils::JumpScopedArray<u8>(tmpBufSize);
  u32 payloadSize = tmpBufSize - sizeof(FatTuple);
  u32 valSize = mutRawVal.Size() - sizeof(ChainedTuple);
  auto* fatTuple =
      new (tmpBuf->get()) FatTuple(payloadSize, valSize, chainedTuple);

  auto prevWorkerId = chainedTuple.mWorkerId;
  auto prevTxId = chainedTuple.mTxId;
  auto prevCommandId = chainedTuple.mCommandId;

  // TODO: check for mPayloadSize overflow
  bool abortConversion = false;
  u16 numDeltasToReplace = 0;
  while (!abortConversion) {
    if (cr::Worker::My().cc.VisibleForAll(prevTxId)) {
      // No need to convert versions that are visible for all to the FatTuple,
      // these old version can be GCed. Pruning versions space might get delayed
      break;
    }

    if (!cr::Worker::My().cc.GetVersion(
            prevWorkerId, prevTxId, prevCommandId, [&](const u8* version, u64) {
              numDeltasToReplace++;
              const auto& chainedDelta = *UpdateVersion::From(version);
              DCHECK(chainedDelta.mType == VersionType::kUpdate);
              DCHECK(chainedDelta.mIsDelta);

              auto& updateDesc = *UpdateDesc::From(chainedDelta.mPayload);
              auto sizeOfDescAndDelta = updateDesc.SizeWithDelta();
              auto deltaSize = sizeof(FatTupleDelta) + sizeOfDescAndDelta;

              if (!fatTuple->HasSpaceFor(updateDesc)) {
                fatTuple->GarbageCollection();
                if (!fatTuple->HasSpaceFor(updateDesc)) {
                  abortConversion = true;
                  return;
                }
              }

              // Add a FatTupleDelta
              fatTuple->NewDelta(deltaSize, chainedDelta.mWorkerId,
                                 chainedDelta.mTxId, chainedDelta.mCommandId,
                                 reinterpret_cast<const u8*>(&updateDesc),
                                 sizeOfDescAndDelta);
              prevWorkerId = chainedDelta.mWorkerId;
              prevTxId = chainedDelta.mTxId;
              prevCommandId = chainedDelta.mCommandId;
            })) {
      // no more old versions
      break;
    }
  }

  if (abortConversion) {
    chainedTuple.WriteUnlock();
    return false;
  }

  if (numDeltasToReplace <= TransactionKV::ConvertToFatTupleThreshold()) {
    chainedTuple.WriteUnlock();
    return false;
  }

  // Reverse the deltas to make follow the O2N order.
  // NOTE: Deltas are added when visiting the chainedTuple in the N2O order.
  fatTuple->ReverseDeltas();
  fatTuple->GarbageCollection();

  if (fatTuple->mPayloadSize > MaxFatTupleLength()) {
    chainedTuple.WriteUnlock();
    return false;
  }

  DCHECK(fatTuple->mPayloadCapacity >= fatTuple->mPayloadSize);

  // Finalize the new FatTuple
  // TODO: corner cases, more careful about space usage
  const u16 fatTupleSize = sizeof(FatTuple) + fatTuple->mPayloadCapacity;
  if (xIter.value().size() < fatTupleSize) {
    auto succeed = xIter.ExtendPayload(fatTupleSize);
    LOG_IF(FATAL, !succeed)
        << "Failed to extend current value buffer to fit the FatTuple"
        << ", fatTupleSize=" << fatTupleSize
        << ", current value buffer size=" << xIter.value().size();
  } else {
    xIter.ShortenWithoutCompaction(fatTupleSize);
  }

  // Copy the FatTuple back to the underlying value buffer.
  std::memcpy(mutRawVal.Data(), tmpBuf->get(), fatTupleSize);
  xIter.MarkAsDirty();
  return true;
}

// -----------------------------------------------------------------------------
// FatTuple
// -----------------------------------------------------------------------------

FatTuple::FatTuple(u32 payloadSize, u32 valSize,
                   const ChainedTuple& chainedTuple)
    : Tuple(TupleFormat::kFat, chainedTuple.mWorkerId, chainedTuple.mTxId),
      mValSize(valSize),
      mPayloadCapacity(payloadSize),
      mPayloadSize(valSize),
      mDataOffset(payloadSize) {
  std::memcpy(mPayload, chainedTuple.mPayload, mValSize);
  mCommandId = chainedTuple.mCommandId;
}

void FatTuple::UndoLastUpdate() {
  ENSURE(mNumDeltas >= 1);
  auto& delta = getDelta(mNumDeltas - 1);
  mWorkerId = delta.mWorkerId;
  mTxId = delta.mTxId;
  mCommandId = delta.mCommandId;
  mNumDeltas -= 1;
  const u32 totalDeltaSize = delta.TotalSize();
  mDataOffset += totalDeltaSize;
  mPayloadSize -= totalDeltaSize + sizeof(u16);
  auto& updateDesc = delta.GetUpdateDesc();
  auto* xorData = delta.GetDeltaPtr();
  BasicKV::CopyToValue(updateDesc, xorData, GetValPtr());
}

// Attention: we have to disable garbage collection if the latest delta was from
// us and not committed yet! Otherwise we would crash during undo although the
// end result is the same if the transaction would commit (overwrite)
void FatTuple::GarbageCollection() {
  if (mNumDeltas == 0) {
    return;
  }
  utils::Timer timer(CRCounters::MyCounters().cc_ms_gc);

  auto appendDelta = [](FatTuple& fatTuple, u8* delta, u16 deltaSize) {
    assert(fatTuple.mPayloadCapacity >=
           (fatTuple.mPayloadSize + deltaSize + sizeof(u16)));
    const u16 i = fatTuple.mNumDeltas++;
    fatTuple.mPayloadSize += deltaSize + sizeof(u16);
    fatTuple.mDataOffset -= deltaSize;
    fatTuple.getDeltaOffsets()[i] = fatTuple.mDataOffset;
    std::memcpy(fatTuple.mPayload + fatTuple.mDataOffset, delta, deltaSize);
  };

  // Delete for all visible deltas, atm using cheap visibility check
  if (cr::Worker::My().cc.VisibleForAll(mTxId)) {
    mNumDeltas = 0;
    mDataOffset = mPayloadCapacity;
    mPayloadSize = mValSize;
    return; // Done
  }

  u16 deltasVisibleForAll = 0;
  for (s32 i = mNumDeltas - 1; i >= 1; i--) {
    auto& delta = getDelta(i);
    if (cr::Worker::My().cc.VisibleForAll(delta.mTxId)) {
      deltasVisibleForAll = i - 1;
      break;
    }
  }

  const TXID local_oldest_oltp = cr::Worker::My().sOldestActiveShortTx.load();
  const TXID local_newest_olap = cr::Worker::My().sNetestActiveLongTx.load();
  if (deltasVisibleForAll == 0 && local_newest_olap > local_oldest_oltp) {
    return; // Nothing to do here
  }

  auto bin_search = [&](TXID upper_bound) {
    s64 l = deltasVisibleForAll;
    s64 r = mNumDeltas - 1;
    s64 m = -1;
    while (l <= r) {
      m = l + (r - l) / 2;
      TXID it = getDelta(m).mTxId;
      if (it == upper_bound) {
        return m;
      } else if (it < upper_bound) {
        l = m + 1;
      } else {
        r = m - 1;
      }
    }
    return l;
  };

  // Identify tuples we should merge --> [zone_begin, zone_end)
  s32 deltas_to_merge_counter = 0;
  s32 zone_begin = -1, zone_end = -1; // [zone_begin, zone_end)
  s32 res = bin_search(local_newest_olap);
  if (res < mNumDeltas) {
    zone_begin = res;
    res = bin_search(local_oldest_oltp);
    if (res - 2 > zone_begin) { // 1 is enough but 2 is an easy fix for
                                // res=mNumDeltas case
      zone_end = res - 2;
      deltas_to_merge_counter = zone_end - zone_begin;
    }
  }
  if (deltasVisibleForAll == 0 && deltas_to_merge_counter <= 0) {
    return;
  }

  auto bufferSize = mPayloadCapacity + sizeof(FatTuple);
  auto buffer = utils::JumpScopedArray<u8>(bufferSize);
  auto& newFatTuple = *new (buffer->get()) FatTuple(mPayloadCapacity);
  newFatTuple.mWorkerId = mWorkerId;
  newFatTuple.mTxId = mTxId;
  newFatTuple.mCommandId = mCommandId;
  newFatTuple.mPayloadSize += mValSize;
  newFatTuple.mValSize = mValSize;
  std::memcpy(newFatTuple.mPayload, mPayload, mValSize); // Copy value

  if (deltas_to_merge_counter <= 0) {
    for (u32 i = deltasVisibleForAll; i < mNumDeltas; i++) {
      auto& delta = getDelta(i);
      appendDelta(newFatTuple, reinterpret_cast<u8*>(&delta),
                  delta.TotalSize());
    }
  } else {
    for (s32 i = deltasVisibleForAll; i < zone_begin; i++) {
      auto& delta = getDelta(i);
      appendDelta(newFatTuple, reinterpret_cast<u8*>(&delta),
                  delta.TotalSize());
    }

    // TODO: Optimize
    // Merge from newest to oldest, i.e., from end to beginning
    std::unordered_map<UpdateSlotInfo, std::basic_string<u8>> diffSlotsMap;
    for (s32 i = zone_end - 1; i >= zone_begin; i--) {
      auto& deltaId = getDelta(i);
      auto& updateDesc = deltaId.GetUpdateDesc();
      auto* deltaPtr = deltaId.GetDeltaPtr();
      for (s16 i = 0; i < updateDesc.mNumSlots; i++) {
        diffSlotsMap[updateDesc.mUpdateSlots[i]] =
            std::basic_string<u8>(deltaPtr, updateDesc.mUpdateSlots[i].mSize);
        deltaPtr += updateDesc.mUpdateSlots[i].mSize;
      }
    }
    u32 totalNewDeltaSize = sizeof(FatTupleDelta) + sizeof(UpdateDesc) +
                            (sizeof(UpdateSlotInfo) * diffSlotsMap.size());
    for (auto& slot_itr : diffSlotsMap) {
      totalNewDeltaSize += slot_itr.second.size();
    }
    auto& mergeDelta = newFatTuple.NewDelta(totalNewDeltaSize);
    mergeDelta = getDelta(zone_begin);
    auto& updateDesc = mergeDelta.GetUpdateDesc();
    updateDesc.mNumSlots = diffSlotsMap.size();
    auto* mergeDeltaPtr = mergeDelta.GetDeltaPtr();
    u32 s_i = 0;
    for (auto& slot_itr : diffSlotsMap) {
      updateDesc.mUpdateSlots[s_i++] = slot_itr.first;
      std::memcpy(mergeDeltaPtr, slot_itr.second.c_str(),
                  slot_itr.second.size());
      mergeDeltaPtr += slot_itr.second.size();
    }

    for (u32 i = zone_end; i < mNumDeltas; i++) {
      auto& delta = getDelta(i);
      appendDelta(newFatTuple, reinterpret_cast<u8*>(&delta),
                  delta.TotalSize());
    }
  }

  std::memcpy(this, buffer->get(), bufferSize);
  DCHECK(mPayloadCapacity >= mPayloadSize);

  DEBUG_BLOCK() {
    u32 spaceUsed = mValSize;
    for (u32 i = 0; i < mNumDeltas; i++) {
      spaceUsed += sizeof(u16) + getDelta(i).TotalSize();
    }
    DCHECK(mPayloadSize == spaceUsed);
  }
}

bool FatTuple::HasSpaceFor(const UpdateDesc& updateDesc) {
  const u32 spaceNeeded =
      updateDesc.SizeWithDelta() + sizeof(u16) + sizeof(FatTupleDelta);
  return (mDataOffset - (mValSize + (mNumDeltas * sizeof(u16)))) >= spaceNeeded;
}

template <typename... Args>
FatTupleDelta& FatTuple::NewDelta(u32 totalDeltaSize, Args&&... args) {
  DCHECK((mPayloadCapacity - mPayloadSize) >= (totalDeltaSize + sizeof(u16)));
  mPayloadSize += totalDeltaSize + sizeof(u16);
  mDataOffset -= totalDeltaSize;
  const u32 deltaId = mNumDeltas++;
  getDeltaOffsets()[deltaId] = mDataOffset;
  return *new (&getDelta(deltaId)) FatTupleDelta(std::forward<Args>(args)...);
}

// Caller should take care of WAL
void FatTuple::Append(UpdateDesc& updateDesc) {
  const u32 totalDeltaSize = updateDesc.SizeWithDelta() + sizeof(FatTupleDelta);
  auto& newDelta = NewDelta(totalDeltaSize);
  newDelta.mWorkerId = this->mWorkerId;
  newDelta.mTxId = this->mTxId;
  newDelta.mCommandId = this->mCommandId;
  std::memcpy(newDelta.mPayload, &updateDesc, updateDesc.Size());
  auto* deltaPtr = newDelta.mPayload + updateDesc.Size();
  BasicKV::CopyToBuffer(updateDesc, this->GetValPtr(), deltaPtr);
}

std::tuple<OpCode, u16> FatTuple::GetVisibleTuple(
    ValCallback valCallback) const {

  // Latest version is visible
  if (cr::Worker::My().cc.VisibleForMe(mWorkerId, mTxId)) {
    valCallback(GetValue());
    return {OpCode::kOK, 1};
  }

  DCHECK(cr::ActiveTx().IsLongRunning());

  if (mNumDeltas > 0) {
    auto copiedVal = utils::JumpScopedArray<u8>(mValSize);
    std::memcpy(copiedVal->get(), GetValPtr(), mValSize);
    // we have to apply the diffs
    u16 numVisitedVersions = 2;
    for (int i = mNumDeltas - 1; i >= 0; i--) {
      const auto& delta = getDelta(i);
      const auto& updateDesc = delta.GetUpdateDesc();
      auto* xorData = delta.GetDeltaPtr();
      BasicKV::CopyToValue(updateDesc, xorData, copiedVal->get());
      if (cr::Worker::My().cc.VisibleForMe(delta.mWorkerId, delta.mTxId)) {
        valCallback(Slice(copiedVal->get(), mValSize));
        return {OpCode::kOK, numVisitedVersions};
      }

      numVisitedVersions++;
    }

    return {OpCode::kNotFound, numVisitedVersions};
  }

  return {OpCode::kNotFound, 1};
}

void FatTuple::resize(const u32 newSize) {
  auto tmpPageSize = newSize + sizeof(FatTuple);
  auto tmpPage = utils::JumpScopedArray<u8>(tmpPageSize);
  auto& newFatTuple = *new (tmpPage->get()) FatTuple(newSize);
  newFatTuple.mWorkerId = mWorkerId;
  newFatTuple.mTxId = mTxId;
  newFatTuple.mCommandId = mCommandId;
  newFatTuple.mPayloadSize += mValSize;
  newFatTuple.mValSize = mValSize;
  std::memcpy(newFatTuple.mPayload, mPayload, mValSize); // Copy value
  auto appendDelta = [](FatTuple& fatTuple, u8* delta, u16 deltaSize) {
    DCHECK(fatTuple.mPayloadCapacity >=
           (fatTuple.mPayloadSize + deltaSize + sizeof(u16)));
    const u16 i = fatTuple.mNumDeltas++;
    fatTuple.mPayloadSize += deltaSize + sizeof(u16);
    fatTuple.mDataOffset -= deltaSize;
    fatTuple.getDeltaOffsets()[i] = fatTuple.mDataOffset;
    std::memcpy(fatTuple.mPayload + fatTuple.mDataOffset, delta, deltaSize);
  };
  for (u64 i = 0; i < mNumDeltas; i++) {
    appendDelta(newFatTuple, reinterpret_cast<u8*>(&getDelta(i)),
                getDelta(i).TotalSize());
  }
  std::memcpy(this, tmpPage->get(), tmpPageSize);
  DCHECK(mPayloadCapacity >= mPayloadSize);
}

void FatTuple::ConvertToChained(TREEID treeId) {
  auto prevWorkerId = mWorkerId;
  auto prevTxId = mTxId;
  auto prevCommandId = mCommandId;
  for (s64 i = mNumDeltas - 1; i >= 0; i--) {
    auto& delta = getDelta(i);
    auto& updateDesc = delta.GetUpdateDesc();
    auto sizeOfDescAndDelta = updateDesc.SizeWithDelta();
    auto versionSize = sizeOfDescAndDelta + sizeof(UpdateVersion);
    cr::Worker::My().cc.mHistoryTree->PutVersion(
        prevWorkerId, prevTxId, prevCommandId, treeId, false, versionSize,
        [&](u8* versionBuf) {
          new (versionBuf) UpdateVersion(delta, sizeOfDescAndDelta);
        },
        false);
    prevWorkerId = delta.mWorkerId;
    prevTxId = delta.mTxId;
    prevCommandId = delta.mCommandId;
  }

  new (this) ChainedTuple(*this);

  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().cc_fat_tuple_decompose[treeId]++;
  }
}

// TODO: Implement inserts after remove cases

} // namespace leanstore::storage::btree
