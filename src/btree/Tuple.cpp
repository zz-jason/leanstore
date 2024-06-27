#include "leanstore/btree/Tuple.hpp"

#include "leanstore/btree/BasicKV.hpp"
#include "leanstore/btree/ChainedTuple.hpp"
#include "leanstore/btree/TransactionKV.hpp"
#include "leanstore/btree/core/BTreeNode.hpp"
#include "leanstore/concurrency/CRManager.hpp"
#include "leanstore/concurrency/Worker.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/Misc.hpp"

#include <unordered_map>

using namespace std;
using namespace leanstore::storage;
using OpCode = leanstore::OpCode;

namespace std {

template <>
class hash<leanstore::UpdateSlotInfo> {
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

static uint64_t MaxFatTupleLength() {
  return BTreeNode::Size() - 1000;
}

bool Tuple::ToFat(BTreePessimisticExclusiveIterator& xIter) {
  utils::Timer timer(CRCounters::MyCounters().cc_ms_fat_tuple_conversion);

  // Process the chain tuple
  MutableSlice mutRawVal = xIter.MutableVal();
  auto& chainedTuple = *ChainedTuple::From(mutRawVal.Data());
  LS_DCHECK(chainedTuple.IsWriteLocked());
  LS_DCHECK(chainedTuple.mFormat == TupleFormat::kChained);

  auto tmpBufSize = MaxFatTupleLength();
  auto tmpBuf = utils::JumpScopedArray<uint8_t>(tmpBufSize);
  uint32_t payloadSize = tmpBufSize - sizeof(FatTuple);
  uint32_t valSize = mutRawVal.Size() - sizeof(ChainedTuple);
  auto* fatTuple = new (tmpBuf->get()) FatTuple(payloadSize, valSize, chainedTuple);

  auto newerWorkerId = chainedTuple.mWorkerId;
  auto newerTxId = chainedTuple.mTxId;
  auto newerCommandId = chainedTuple.mCommandId;

  // TODO: check for mPayloadSize overflow
  bool abortConversion = false;
  uint16_t numDeltasToReplace = 0;
  while (!abortConversion) {
    if (cr::Worker::My().mCc.VisibleForAll(newerTxId)) {
      // No need to convert versions that are visible for all to the FatTuple,
      // these old version can be GCed. Pruning versions space might get delayed
      break;
    }

    if (!cr::Worker::My().mCc.GetVersion(
            newerWorkerId, newerTxId, newerCommandId, [&](const uint8_t* version, uint64_t) {
              numDeltasToReplace++;
              const auto& chainedDelta = *UpdateVersion::From(version);
              LS_DCHECK(chainedDelta.mType == VersionType::kUpdate);
              LS_DCHECK(chainedDelta.mIsDelta);

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
              fatTuple->NewDelta(deltaSize, chainedDelta.mWorkerId, chainedDelta.mTxId,
                                 chainedDelta.mCommandId,
                                 reinterpret_cast<const uint8_t*>(&updateDesc), sizeOfDescAndDelta);
              newerWorkerId = chainedDelta.mWorkerId;
              newerTxId = chainedDelta.mTxId;
              newerCommandId = chainedDelta.mCommandId;
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

  LS_DCHECK(fatTuple->mPayloadCapacity >= fatTuple->mPayloadSize);

  // Finalize the new FatTuple
  // TODO: corner cases, more careful about space usage
  const uint16_t fatTupleSize = sizeof(FatTuple) + fatTuple->mPayloadCapacity;
  if (xIter.value().size() < fatTupleSize) {
    auto succeed = xIter.ExtendPayload(fatTupleSize);
    if (!succeed) {
      Log::Fatal("Failed to extend current value buffer to fit the FatTuple, "
                 "fatTupleSize={}, current value buffer size={}",
                 fatTupleSize, xIter.value().size());
    }
  } else {
    xIter.ShortenWithoutCompaction(fatTupleSize);
  }

  // Copy the FatTuple back to the underlying value buffer.
  std::memcpy(mutRawVal.Data(), tmpBuf->get(), fatTupleSize);
  return true;
}

// -----------------------------------------------------------------------------
// FatTuple
// -----------------------------------------------------------------------------

FatTuple::FatTuple(uint32_t payloadSize, uint32_t valSize, const ChainedTuple& chainedTuple)
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
  const uint32_t totalDeltaSize = delta.TotalSize();
  mDataOffset += totalDeltaSize;
  mPayloadSize -= totalDeltaSize + sizeof(uint16_t);
  auto& updateDesc = delta.GetUpdateDesc();
  auto* xorData = delta.GetDeltaPtr();
  BasicKV::CopyToValue(updateDesc, xorData, GetValPtr());
}

// NOLINTBEGIN

// Attention: we have to disable garbage collection if the latest delta was from
// us and not committed yet! Otherwise we would crash during undo although the
// end result is the same if the transaction would commit (overwrite)
void FatTuple::GarbageCollection() {
  if (mNumDeltas == 0) {
    return;
  }
  utils::Timer timer(CRCounters::MyCounters().cc_ms_gc);

  auto appendDelta = [](FatTuple& fatTuple, uint8_t* delta, uint16_t deltaSize) {
    assert(fatTuple.mPayloadCapacity >= (fatTuple.mPayloadSize + deltaSize + sizeof(uint16_t)));
    const uint16_t i = fatTuple.mNumDeltas++;
    fatTuple.mPayloadSize += deltaSize + sizeof(uint16_t);
    fatTuple.mDataOffset -= deltaSize;
    fatTuple.getDeltaOffsets()[i] = fatTuple.mDataOffset;
    std::memcpy(fatTuple.mPayload + fatTuple.mDataOffset, delta, deltaSize);
  };

  // Delete for all visible deltas, atm using cheap visibility check
  if (cr::Worker::My().mCc.VisibleForAll(mTxId)) {
    mNumDeltas = 0;
    mDataOffset = mPayloadCapacity;
    mPayloadSize = mValSize;
    return; // Done
  }

  uint16_t deltasVisibleForAll = 0;
  for (int32_t i = mNumDeltas - 1; i >= 1; i--) {
    auto& delta = getDelta(i);
    if (cr::Worker::My().mCc.VisibleForAll(delta.mTxId)) {
      deltasVisibleForAll = i - 1;
      break;
    }
  }

  const TXID local_oldest_oltp =
      cr::Worker::My().mStore->mCRManager->mGlobalWmkInfo.mOldestActiveShortTx.load();
  const TXID local_newest_olap =
      cr::Worker::My().mStore->mCRManager->mGlobalWmkInfo.mNewestActiveLongTx.load();
  if (deltasVisibleForAll == 0 && local_newest_olap > local_oldest_oltp) {
    return; // Nothing to do here
  }

  auto bin_search = [&](TXID upper_bound) {
    int64_t l = deltasVisibleForAll;
    int64_t r = mNumDeltas - 1;
    int64_t m = -1;
    while (l <= r) {
      m = l + (r - l) / 2;
      TXID it = getDelta(m).mTxId;
      if (it == upper_bound) {
        return m;
      }

      if (it < upper_bound) {
        l = m + 1;
      } else {
        r = m - 1;
      }
    }
    return l;
  };

  // Identify tuples we should merge --> [zone_begin, zone_end)
  int32_t deltas_to_merge_counter = 0;
  int32_t zone_begin = -1, zone_end = -1; // [zone_begin, zone_end)
  int32_t res = bin_search(local_newest_olap);
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
  auto buffer = utils::JumpScopedArray<uint8_t>(bufferSize);
  auto& newFatTuple = *new (buffer->get()) FatTuple(mPayloadCapacity);
  newFatTuple.mWorkerId = mWorkerId;
  newFatTuple.mTxId = mTxId;
  newFatTuple.mCommandId = mCommandId;
  newFatTuple.mPayloadSize += mValSize;
  newFatTuple.mValSize = mValSize;
  std::memcpy(newFatTuple.mPayload, mPayload, mValSize); // Copy value

  if (deltas_to_merge_counter <= 0) {
    for (uint32_t i = deltasVisibleForAll; i < mNumDeltas; i++) {
      auto& delta = getDelta(i);
      appendDelta(newFatTuple, reinterpret_cast<uint8_t*>(&delta), delta.TotalSize());
    }
  } else {
    for (int32_t i = deltasVisibleForAll; i < zone_begin; i++) {
      auto& delta = getDelta(i);
      appendDelta(newFatTuple, reinterpret_cast<uint8_t*>(&delta), delta.TotalSize());
    }

    // TODO: Optimize
    // Merge from newest to oldest, i.e., from end to beginning
    std::unordered_map<UpdateSlotInfo, std::basic_string<uint8_t>> diffSlotsMap;
    for (int32_t i = zone_end - 1; i >= zone_begin; i--) {
      auto& deltaId = getDelta(i);
      auto& updateDesc = deltaId.GetUpdateDesc();
      auto* deltaPtr = deltaId.GetDeltaPtr();
      for (int16_t i = 0; i < updateDesc.mNumSlots; i++) {
        diffSlotsMap[updateDesc.mUpdateSlots[i]] =
            std::basic_string<uint8_t>(deltaPtr, updateDesc.mUpdateSlots[i].mSize);
        deltaPtr += updateDesc.mUpdateSlots[i].mSize;
      }
    }
    uint32_t totalNewDeltaSize =
        sizeof(FatTupleDelta) + sizeof(UpdateDesc) + (sizeof(UpdateSlotInfo) * diffSlotsMap.size());
    for (auto& slot_itr : diffSlotsMap) {
      totalNewDeltaSize += slot_itr.second.size();
    }
    auto& mergeDelta = newFatTuple.NewDelta(totalNewDeltaSize);
    mergeDelta = getDelta(zone_begin);
    auto& updateDesc = mergeDelta.GetUpdateDesc();
    updateDesc.mNumSlots = diffSlotsMap.size();
    auto* mergeDeltaPtr = mergeDelta.GetDeltaPtr();
    uint32_t s_i = 0;
    for (auto& slot_itr : diffSlotsMap) {
      updateDesc.mUpdateSlots[s_i++] = slot_itr.first;
      std::memcpy(mergeDeltaPtr, slot_itr.second.c_str(), slot_itr.second.size());
      mergeDeltaPtr += slot_itr.second.size();
    }

    for (uint32_t i = zone_end; i < mNumDeltas; i++) {
      auto& delta = getDelta(i);
      appendDelta(newFatTuple, reinterpret_cast<uint8_t*>(&delta), delta.TotalSize());
    }
  }

  std::memcpy(this, buffer->get(), bufferSize);
  LS_DCHECK(mPayloadCapacity >= mPayloadSize);

  DEBUG_BLOCK() {
    uint32_t spaceUsed [[maybe_unused]] = mValSize;
    for (uint32_t i = 0; i < mNumDeltas; i++) {
      spaceUsed += sizeof(uint16_t) + getDelta(i).TotalSize();
    }
    LS_DCHECK(mPayloadSize == spaceUsed);
  }
}

// NOLINTEND

bool FatTuple::HasSpaceFor(const UpdateDesc& updateDesc) {
  const uint32_t SpaceNeeded =
      updateDesc.SizeWithDelta() + sizeof(uint16_t) + sizeof(FatTupleDelta);
  return (mDataOffset - (mValSize + (mNumDeltas * sizeof(uint16_t)))) >= SpaceNeeded;
}

template <typename... Args>
FatTupleDelta& FatTuple::NewDelta(uint32_t totalDeltaSize, Args&&... args) {
  LS_DCHECK((mPayloadCapacity - mPayloadSize) >= (totalDeltaSize + sizeof(uint16_t)));
  mPayloadSize += totalDeltaSize + sizeof(uint16_t);
  mDataOffset -= totalDeltaSize;
  const uint32_t deltaId = mNumDeltas++;
  getDeltaOffsets()[deltaId] = mDataOffset;
  return *new (&getDelta(deltaId)) FatTupleDelta(std::forward<Args>(args)...);
}

// Caller should take care of WAL
void FatTuple::Append(UpdateDesc& updateDesc) {
  const uint32_t totalDeltaSize = updateDesc.SizeWithDelta() + sizeof(FatTupleDelta);
  auto& newDelta = NewDelta(totalDeltaSize);
  newDelta.mWorkerId = this->mWorkerId;
  newDelta.mTxId = this->mTxId;
  newDelta.mCommandId = this->mCommandId;
  std::memcpy(newDelta.mPayload, &updateDesc, updateDesc.Size());
  auto* deltaPtr = newDelta.mPayload + updateDesc.Size();
  BasicKV::CopyToBuffer(updateDesc, this->GetValPtr(), deltaPtr);
}

std::tuple<OpCode, uint16_t> FatTuple::GetVisibleTuple(ValCallback valCallback) const {

  // Latest version is visible
  if (cr::Worker::My().mCc.VisibleForMe(mWorkerId, mTxId)) {
    valCallback(GetValue());
    return {OpCode::kOK, 1};
  }

  LS_DCHECK(cr::ActiveTx().IsLongRunning());

  if (mNumDeltas > 0) {
    auto copiedVal = utils::JumpScopedArray<uint8_t>(mValSize);
    std::memcpy(copiedVal->get(), GetValPtr(), mValSize);
    // we have to apply the diffs
    uint16_t numVisitedVersions = 2;
    for (int i = mNumDeltas - 1; i >= 0; i--) {
      const auto& delta = getDelta(i);
      const auto& updateDesc = delta.GetUpdateDesc();
      auto* xorData = delta.GetDeltaPtr();
      BasicKV::CopyToValue(updateDesc, xorData, copiedVal->get());
      if (cr::Worker::My().mCc.VisibleForMe(delta.mWorkerId, delta.mTxId)) {
        valCallback(Slice(copiedVal->get(), mValSize));
        return {OpCode::kOK, numVisitedVersions};
      }

      numVisitedVersions++;
    }

    return {OpCode::kNotFound, numVisitedVersions};
  }

  return {OpCode::kNotFound, 1};
}

void FatTuple::resize(const uint32_t newSize) {
  auto tmpPageSize = newSize + sizeof(FatTuple);
  auto tmpPage = utils::JumpScopedArray<uint8_t>(tmpPageSize);
  auto& newFatTuple = *new (tmpPage->get()) FatTuple(newSize);
  newFatTuple.mWorkerId = mWorkerId;
  newFatTuple.mTxId = mTxId;
  newFatTuple.mCommandId = mCommandId;
  newFatTuple.mPayloadSize += mValSize;
  newFatTuple.mValSize = mValSize;
  std::memcpy(newFatTuple.mPayload, mPayload, mValSize); // Copy value
  auto appendDelta = [](FatTuple& fatTuple, uint8_t* delta, uint16_t deltaSize) {
    LS_DCHECK(fatTuple.mPayloadCapacity >= (fatTuple.mPayloadSize + deltaSize + sizeof(uint16_t)));
    const uint16_t i = fatTuple.mNumDeltas++;
    fatTuple.mPayloadSize += deltaSize + sizeof(uint16_t);
    fatTuple.mDataOffset -= deltaSize;
    fatTuple.getDeltaOffsets()[i] = fatTuple.mDataOffset;
    std::memcpy(fatTuple.mPayload + fatTuple.mDataOffset, delta, deltaSize);
  };
  for (uint64_t i = 0; i < mNumDeltas; i++) {
    appendDelta(newFatTuple, reinterpret_cast<uint8_t*>(&getDelta(i)), getDelta(i).TotalSize());
  }
  std::memcpy(this, tmpPage->get(), tmpPageSize);
  LS_DCHECK(mPayloadCapacity >= mPayloadSize);
}

void FatTuple::ConvertToChained(TREEID treeId) {
  auto prevWorkerId = mWorkerId;
  auto prevTxId = mTxId;
  auto prevCommandId = mCommandId;
  for (int64_t i = mNumDeltas - 1; i >= 0; i--) {
    auto& delta = getDelta(i);
    auto& updateDesc = delta.GetUpdateDesc();
    auto sizeOfDescAndDelta = updateDesc.SizeWithDelta();
    auto versionSize = sizeOfDescAndDelta + sizeof(UpdateVersion);
    cr::Worker::My()
        .mCc.Other(prevWorkerId)
        .mHistoryStorage.PutVersion(
            prevTxId, prevCommandId, treeId, false, versionSize,
            [&](uint8_t* versionBuf) { new (versionBuf) UpdateVersion(delta, sizeOfDescAndDelta); },
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
