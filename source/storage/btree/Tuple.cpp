#include "BTreeVI.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "storage/btree/core/BTreeNode.hpp"
#include "utils/Misc.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <map>
#include <set>
#include <unordered_map>

#include <signal.h>

using namespace std;
using namespace leanstore::storage;
using OP_RESULT = leanstore::OP_RESULT;

namespace std {

template <> class hash<leanstore::UpdateDiffSlot> {
public:
  size_t operator()(const leanstore::UpdateDiffSlot& slot) const {
    size_t result = (slot.length << 16) | slot.offset;
    return result;
  }
};

} // namespace std

namespace leanstore::storage::btree {

// -----------------------------------------------------------------------------
// Tuple
// -----------------------------------------------------------------------------

static u64 maxFatTupleLength() {
  return BTreeNode::Size() - 1000;
}

bool Tuple::ToFat(BTreeExclusiveIterator& xIter) {
  utils::Timer timer(CRCounters::myCounters().cc_ms_fat_tuple_conversion);

  // Process the chain tuple
  MutableSlice rawVal = xIter.MutableVal();
  auto& chainedTuple = *ChainedTuple::From(rawVal.data());
  DCHECK(chainedTuple.IsWriteLocked());
  DCHECK(chainedTuple.mFormat == TupleFormat::CHAINED);

  auto tmpBufSize = maxFatTupleLength();
  auto tmpBuf = utils::JumpScopedArray<u8>(tmpBufSize);
  u32 payloadSize = tmpBufSize - sizeof(FatTuple);
  u32 valSize = rawVal.length() - sizeof(ChainedTuple);
  auto fatTuple =
      new (tmpBuf->get()) FatTuple(payloadSize, valSize, chainedTuple);

  auto prevWorkerId = chainedTuple.mWorkerId;
  auto prevTxId = chainedTuple.mTxId;
  auto prevCommandId = chainedTuple.mCommandId;

  // TODO: check for mPayloadSize overflow
  bool abortConversion = false;
  u16 numDeltasToReplace = 0;
  while (!abortConversion) {
    if (cr::Worker::my().cc.isVisibleForAll(prevTxId)) {
      // No need to convert versions that are visible for all to the FatTuple,
      // these old version can be GCed. Pruning versions space might get delayed
      break;
    }

    if (!cr::Worker::my().cc.retrieveVersion(
            prevWorkerId, prevTxId, prevCommandId, [&](const u8* version, u64) {
              numDeltasToReplace++;
              const auto& chainedDelta = *UpdateVersion::From(version);
              DCHECK(chainedDelta.type == Version::TYPE::UPDATE);
              DCHECK(chainedDelta.mIsDelta);

              const auto& updateDesc = *UpdateDesc::From(chainedDelta.payload);
              const u32 deltaPayloadSize = updateDesc.TotalSize();
              const u32 deltaSize = sizeof(FatTupleDelta) + deltaPayloadSize;

              if (!fatTuple->hasSpaceFor(updateDesc)) {
                fatTuple->garbageCollection();
                if (!fatTuple->hasSpaceFor(updateDesc)) {
                  abortConversion = true;
                  return;
                }
              }

              // Add a FatTupleDelta
              fatTuple->NewDelta(deltaSize, chainedDelta.mWorkerId,
                                 chainedDelta.mTxId, chainedDelta.mCommandId,
                                 reinterpret_cast<const u8*>(&updateDesc),
                                 deltaPayloadSize);
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

  if (numDeltasToReplace <= BTreeVI::convertToFatTupleThreshold()) {
    chainedTuple.WriteUnlock();
    return false;
  }

  // Reverse the deltas to make follow the O2N order.
  // NOTE: Deltas are added when visiting the chainedTuple in the N2O order.
  fatTuple->ReverseDeltas();
  fatTuple->garbageCollection();

  if (fatTuple->mPayloadSize > maxFatTupleLength()) {
    chainedTuple.WriteUnlock();
    return false;
  }

  DCHECK(fatTuple->mPayloadCapacity >= fatTuple->mPayloadSize);

  // Finalize the new FatTuple
  // TODO: corner cases, more careful about space usage
  const u16 fatTupleSize = sizeof(FatTuple) + fatTuple->mPayloadCapacity;
  if (xIter.value().size() < fatTupleSize) {
    auto succeed = xIter.extendPayload(fatTupleSize);
    LOG_IF(FATAL, !succeed)
        << "Failed to extend current value buffer to fit the FatTuple"
        << ", fatTupleSize=" << fatTupleSize
        << ", current value buffer size=" << xIter.value().size();
  } else {
    xIter.shorten(fatTupleSize);
  }

  // Copy the FatTuple back to the underlying value buffer.
  std::memcpy(rawVal.data(), tmpBuf->get(), fatTupleSize);
  xIter.MarkAsDirty();
  return true;
}

// -----------------------------------------------------------------------------
// FatTuple
// -----------------------------------------------------------------------------

FatTuple::FatTuple(u32 payloadSize, u32 valSize,
                   const ChainedTuple& chainedTuple)
    : Tuple(TupleFormat::FAT, chainedTuple.mWorkerId, chainedTuple.mTxId),
      mValSize(valSize), mPayloadCapacity(payloadSize), mPayloadSize(valSize),
      mDataOffset(payloadSize) {
  std::memcpy(payload, chainedTuple.payload, mValSize);
  mCommandId = chainedTuple.mCommandId;
}

void FatTuple::undoLastUpdate() {
  ENSURE(mNumDeltas >= 1);
  auto& delta = getDelta(mNumDeltas - 1);
  mWorkerId = delta.mWorkerId;
  mTxId = delta.mTxId;
  mCommandId = delta.mCommandId;
  mNumDeltas -= 1;
  const u32 totalDeltaSize = delta.TotalSize();
  mDataOffset += totalDeltaSize;
  mPayloadSize -= totalDeltaSize + sizeof(u16);
  delta.getDescriptor().ApplyDiff(GetValPtr(),
                                  delta.payload + delta.getDescriptor().size());
}

// Attention: we have to disable garbage collection if the latest delta was from
// us and not committed yet! Otherwise we would crash during undo although the
// end result is the same if the transaction would commit (overwrite)
void FatTuple::garbageCollection() {
  if (mNumDeltas == 0) {
    return;
  }
  utils::Timer timer(CRCounters::myCounters().cc_ms_gc);

  auto append_ll = [](FatTuple& fatTuple, u8* delta, u16 delta_length) {
    assert(fatTuple.mPayloadCapacity >=
           (fatTuple.mPayloadSize + delta_length + sizeof(u16)));
    const u16 i = fatTuple.mNumDeltas++;
    fatTuple.mPayloadSize += delta_length + sizeof(u16);
    fatTuple.mDataOffset -= delta_length;
    fatTuple.getDeltaOffsets()[i] = fatTuple.mDataOffset;
    std::memcpy(fatTuple.payload + fatTuple.mDataOffset, delta, delta_length);
  };

  // Delete for all visible deltas, atm using cheap visibility check
  if (cr::Worker::my().cc.isVisibleForAll(mTxId)) {
    mNumDeltas = 0;
    mDataOffset = mPayloadCapacity;
    mPayloadSize = mValSize;
    return; // Done
  }

  u16 deltas_visible_by_all_counter = 0;
  for (s32 i = mNumDeltas - 1; i >= 1; i--) {
    auto& delta = getDelta(i);
    if (cr::Worker::my().cc.isVisibleForAll(delta.mTxId)) {
      deltas_visible_by_all_counter = i - 1;
      break;
    }
  }

  const TXID local_oldest_oltp = cr::Worker::my().sOldestOltpStartTx.load();
  const TXID local_newest_olap = cr::Worker::my().sNewestOlapStartTx.load();
  if (deltas_visible_by_all_counter == 0 &&
      local_newest_olap > local_oldest_oltp) {
    return; // Nothing to do here
  }

  auto bin_search = [&](TXID upper_bound) {
    s64 l = deltas_visible_by_all_counter;
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
  if (deltas_visible_by_all_counter == 0 && deltas_to_merge_counter <= 0) {
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
  std::memcpy(newFatTuple.payload, payload, mValSize); // Copy value

  if (deltas_to_merge_counter <= 0) {
    for (u32 i = deltas_visible_by_all_counter; i < mNumDeltas; i++) {
      auto& delta = getDelta(i);
      append_ll(newFatTuple, reinterpret_cast<u8*>(&delta), delta.TotalSize());
    }
  } else {
    for (s32 i = deltas_visible_by_all_counter; i < zone_begin; i++) {
      auto& delta = getDelta(i);
      append_ll(newFatTuple, reinterpret_cast<u8*>(&delta), delta.TotalSize());
    }

    // TODO: Optimize
    // Merge from newest to oldest, i.e., from end of array into beginning
    if (FLAGS_tmp5 && 0) {
      // Hack
      auto& delta = getDelta(zone_begin);
      append_ll(newFatTuple, reinterpret_cast<u8*>(&delta), delta.TotalSize());
    } else {
      using Slot = UpdateDiffSlot;
      std::unordered_map<Slot, std::basic_string<u8>> diffSlotsMap;
      for (s32 i = zone_end - 1; i >= zone_begin; i--) {
        auto& deltaId = getDelta(i);
        auto& descriptor_i = deltaId.getDescriptor();
        u8* delta_diff_ptr = deltaId.payload + descriptor_i.size();
        for (s16 s_i = 0; s_i < descriptor_i.mNumSlots; s_i++) {
          diffSlotsMap[descriptor_i.mDiffSlots[s_i]] = std::basic_string<u8>(
              delta_diff_ptr, descriptor_i.mDiffSlots[s_i].length);
          delta_diff_ptr += descriptor_i.mDiffSlots[s_i].length;
        }
      }
      u32 totalNewDeltaSize = sizeof(FatTupleDelta) + sizeof(UpdateDesc) +
                              (sizeof(UpdateDiffSlot) * diffSlotsMap.size());
      for (auto& slot_itr : diffSlotsMap) {
        totalNewDeltaSize += slot_itr.second.size();
      }
      auto& mergeDelta = newFatTuple.NewDelta(totalNewDeltaSize);
      mergeDelta = getDelta(zone_begin);
      UpdateDesc& merge_descriptor = mergeDelta.getDescriptor();
      merge_descriptor.mNumSlots = diffSlotsMap.size();
      u8* merge_diff_ptr = mergeDelta.payload + merge_descriptor.size();
      u32 s_i = 0;
      for (auto& slot_itr : diffSlotsMap) {
        merge_descriptor.mDiffSlots[s_i++] = slot_itr.first;
        std::memcpy(merge_diff_ptr, slot_itr.second.c_str(),
                    slot_itr.second.size());
        merge_diff_ptr += slot_itr.second.size();
      }
    }

    for (u32 i = zone_end; i < mNumDeltas; i++) {
      auto& delta = getDelta(i);
      append_ll(newFatTuple, reinterpret_cast<u8*>(&delta), delta.TotalSize());
    }
  }

  std::memcpy(this, buffer->get(), bufferSize);
  DCHECK(mPayloadCapacity >= mPayloadSize);

  DEBUG_BLOCK() {
    u32 should_used_space = mValSize;
    for (u32 i = 0; i < mNumDeltas; i++) {
      should_used_space += sizeof(u16) + getDelta(i).TotalSize();
    }
    DCHECK(mPayloadSize == should_used_space);
  }
}

bool FatTuple::hasSpaceFor(const UpdateDesc& updateDesc) {
  const u32 needed_space =
      updateDesc.TotalSize() + sizeof(u16) + sizeof(FatTupleDelta);
  return (mDataOffset - (mValSize + (mNumDeltas * sizeof(u16)))) >=
         needed_space;
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
void FatTuple::append(UpdateDesc& updateDesc) {
  const u32 totalDeltaSize = updateDesc.TotalSize() + sizeof(FatTupleDelta);
  auto& newDelta = NewDelta(totalDeltaSize);
  newDelta.mWorkerId = this->mWorkerId;
  newDelta.mTxId = this->mTxId;
  newDelta.mCommandId = this->mCommandId;
  std::memcpy(newDelta.payload, &updateDesc, updateDesc.size());
  auto diffDest = newDelta.payload + updateDesc.size();
  updateDesc.CopySlots(diffDest, this->GetValPtr());
}

bool FatTuple::update(BTreeExclusiveIterator& xIter, Slice key,
                      MutValCallback updateCallBack, UpdateDesc& updateDesc) {
  utils::Timer timer(CRCounters::myCounters().cc_ms_fat_tuple);
  while (true) {
    auto fatTuple = reinterpret_cast<FatTuple*>(xIter.MutableVal().data());
    DCHECK(fatTuple->IsWriteLocked())
        << "Tuple should be write locked before update";

    if (fatTuple->hasSpaceFor(updateDesc)) {
      // WAL
      auto deltaSize = updateDesc.TotalSize();
      auto prevWorkerId = fatTuple->mWorkerId;
      auto prevTxId = fatTuple->mTxId;
      auto prevCommandId = fatTuple->mCommandId;
      auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WALUpdateSSIP>(
          key.size() + deltaSize, key, updateDesc, deltaSize, prevWorkerId,
          prevTxId, prevCommandId);
      auto diffDest = walHandler->payload + key.size() + updateDesc.size();
      auto diffSrc = fatTuple->GetValPtr();
      updateDesc.CopySlots(diffDest, diffSrc);
      updateDesc.XORSlots(diffDest, diffSrc);
      walHandler.SubmitWal();

      // Update the value in-place
      fatTuple->append(updateDesc);
      fatTuple->mWorkerId = cr::Worker::my().mWorkerId;
      fatTuple->mTxId = cr::activeTX().startTS();
      fatTuple->mCommandId = cr::Worker::my().mCommandId++;
      updateCallBack(fatTuple->GetMutableValue());
      DCHECK(fatTuple->mPayloadCapacity >= fatTuple->mPayloadSize);
      return true;
    }

    fatTuple->garbageCollection();
    if (fatTuple->hasSpaceFor(updateDesc)) {
      continue;
    }

    // Not enough space, convert FatTuple to ChainedTuple
    auto chainedTupleSize = fatTuple->mValSize + sizeof(ChainedTuple);
    DCHECK(chainedTupleSize < xIter.value().length());
    fatTuple->convertToChained(xIter.mBTree.mTreeId);
    xIter.shorten(chainedTupleSize);
    return false;
  }
}

std::tuple<OP_RESULT, u16> FatTuple::GetVisibleTuple(
    ValCallback valCallback) const {

  // Latest version is visible
  if (cr::Worker::my().cc.VisibleForMe(mWorkerId, mTxId)) {
    valCallback(GetValue());
    return {OP_RESULT::OK, 1};
  }

  DCHECK(!cr::activeTX().isOLTP());

  if (mNumDeltas > 0) {
    auto materializedValue = utils::JumpScopedArray<u8>(mValSize);
    std::memcpy(materializedValue->get(), GetValPtr(), mValSize);
    // we have to apply the diffs
    u16 numVisitedVersions = 2;
    for (int i = mNumDeltas - 1; i >= 0; i--) {
      const auto& delta = getDelta(i);
      const auto& desc = delta.getDescriptor();
      desc.ApplyDiff(materializedValue->get(), delta.payload + desc.size());
      if (cr::Worker::my().cc.VisibleForMe(delta.mWorkerId, delta.mTxId)) {
        valCallback(Slice(materializedValue->get(), mValSize));
        return {OP_RESULT::OK, numVisitedVersions};
      }

      numVisitedVersions++;
    }

    return {OP_RESULT::NOT_FOUND, numVisitedVersions};
  }

  return {OP_RESULT::NOT_FOUND, 1};
}

void FatTuple::resize(const u32 newLength) {
  auto tmpPageSize = newLength + sizeof(FatTuple);
  auto tmpPage = utils::JumpScopedArray<u8>(tmpPageSize);
  auto& newFatTuple = *new (tmpPage->get()) FatTuple(newLength);
  newFatTuple.mWorkerId = mWorkerId;
  newFatTuple.mTxId = mTxId;
  newFatTuple.mCommandId = mCommandId;
  newFatTuple.mPayloadSize += mValSize;
  newFatTuple.mValSize = mValSize;
  std::memcpy(newFatTuple.payload, payload, mValSize); // Copy value
  auto append_ll = [](FatTuple& fatTuple, u8* delta, u16 delta_length) {
    DCHECK(fatTuple.mPayloadCapacity >=
           (fatTuple.mPayloadSize + delta_length + sizeof(u16)));
    const u16 i = fatTuple.mNumDeltas++;
    fatTuple.mPayloadSize += delta_length + sizeof(u16);
    fatTuple.mDataOffset -= delta_length;
    fatTuple.getDeltaOffsets()[i] = fatTuple.mDataOffset;
    std::memcpy(fatTuple.payload + fatTuple.mDataOffset, delta, delta_length);
  };
  for (u64 i = 0; i < mNumDeltas; i++) {
    append_ll(newFatTuple, reinterpret_cast<u8*>(&getDelta(i)),
              getDelta(i).TotalSize());
  }
  std::memcpy(this, tmpPage->get(), tmpPageSize);
  DCHECK(mPayloadCapacity >= mPayloadSize);
}

void FatTuple::convertToChained(TREEID treeId) {
  auto prevWorkerId = mWorkerId;
  auto prevTxId = mTxId;
  auto prevCommandId = mCommandId;
  for (s64 i = mNumDeltas - 1; i >= 0; i--) {
    auto& delta = getDelta(i);
    auto deltaPayloadSize = delta.getDescriptor().TotalSize();
    auto versionSize = deltaPayloadSize + sizeof(UpdateVersion);
    cr::Worker::my().cc.mHistoryTree->insertVersion(
        prevWorkerId, prevTxId, prevCommandId, treeId, false, versionSize,
        [&](u8* versionBuf) {
          new (versionBuf) UpdateVersion(delta, deltaPayloadSize);
        },
        false);
    prevWorkerId = delta.mWorkerId;
    prevTxId = delta.mTxId;
    prevCommandId = delta.mCommandId;
  }

  new (this) ChainedTuple(*this);

  COUNTERS_BLOCK() {
    WorkerCounters::myCounters().cc_fat_tuple_decompose[treeId]++;
  }
}

// TODO: Implement inserts after remove cases
std::tuple<OP_RESULT, u16> ChainedTuple::GetVisibleTuple(
    Slice payload, ValCallback callback) const {
  if (cr::Worker::my().cc.VisibleForMe(mWorkerId, mTxId, false)) {
    if (mIsRemoved) {
      return {OP_RESULT::NOT_FOUND, 1};
    }

    auto valSize = payload.length() - sizeof(ChainedTuple);
    callback(GetValue(valSize));
    return {OP_RESULT::OK, 1};
  }

  if (isFinal()) {
    JUMPMU_RETURN{OP_RESULT::NOT_FOUND, 1};
  }

  // Head is not visible
  u16 valueSize = payload.length() - sizeof(ChainedTuple);
  auto valueBuf = std::make_unique<u8[]>(valueSize);
  std::memcpy(valueBuf.get(), this->payload, valueSize);

  WORKERID prevWorkerId = mWorkerId;
  TXID prevTxId = mTxId;
  COMMANDID prevCommandId = mCommandId;

  u16 numVisitedVersions = 1;
  while (true) {
    bool found = cr::Worker::my().cc.retrieveVersion(
        prevWorkerId, prevTxId, prevCommandId,
        [&](const u8* versionBuf, u64 versionSize) {
          const auto& version = *reinterpret_cast<const Version*>(versionBuf);
          if (version.type == Version::TYPE::UPDATE) {
            const auto& updateVersion = *UpdateVersion::From(versionBuf);
            if (updateVersion.mIsDelta) {
              // Apply delta
              const auto& updateDesc = *UpdateDesc::From(updateVersion.payload);
              auto diffSrc = updateVersion.payload + updateDesc.size();
              updateDesc.ApplyDiff(valueBuf.get(), diffSrc);
            } else {
              valueSize = versionSize - sizeof(UpdateVersion);
              valueBuf = std::make_unique<u8[]>(valueSize);
              std::memcpy(valueBuf.get(), updateVersion.payload, valueSize);
            }
          } else if (version.type == Version::TYPE::REMOVE) {
            const auto& removeVersion = *RemoveVersion::From(versionBuf);
            valueSize = removeVersion.mValSize;
            valueBuf = std::make_unique<u8[]>(valueSize);
            std::memcpy(valueBuf.get(), removeVersion.payload, valueSize);
          } else {
            UNREACHABLE();
          }

          prevWorkerId = version.mWorkerId;
          prevTxId = version.mTxId;
          prevCommandId = version.mCommandId;
        });
    if (!found) {
      cerr << std::find(cr::Worker::my().cc.local_workers_start_ts.get(),
                        cr::Worker::my().cc.local_workers_start_ts.get() +
                            cr::Worker::my().mNumAllWorkers,
                        prevTxId) -
                  cr::Worker::my().cc.local_workers_start_ts.get()
           << endl;
      cerr << "tls = " << cr::Worker::my().mActiveTx.startTS() << endl;
      RAISE_WHEN(true);
      RAISE_WHEN(prevCommandId != INVALID_COMMANDID);
      return {OP_RESULT::NOT_FOUND, numVisitedVersions};
    }
    if (cr::Worker::my().cc.VisibleForMe(prevWorkerId, prevTxId, false)) {
      callback(Slice(valueBuf.get(), valueSize));
      return {OP_RESULT::OK, numVisitedVersions};
    }
    numVisitedVersions++;
  }
  return {OP_RESULT::NOT_FOUND, numVisitedVersions};
}

void ChainedTuple::Update(BTreeExclusiveIterator& xIter, Slice key,
                          MutValCallback updateCallBack,
                          UpdateDesc& updateDesc) {
  auto deltaPayloadSize = updateDesc.TotalSize();
  const u64 versionSize = deltaPayloadSize + sizeof(UpdateVersion);
  COMMANDID commandId = INVALID_COMMANDID;

  // Move the newest tuple to the history version tree.
  auto treeId = xIter.mBTree.mTreeId;
  commandId = cr::Worker::my().cc.insertVersion(
      treeId, false, versionSize, [&](u8* versionBuf) {
        auto& updateVersion =
            *new (versionBuf) UpdateVersion(mWorkerId, mTxId, mCommandId, true);
        std::memcpy(updateVersion.payload, &updateDesc, updateDesc.size());
        auto dest = updateVersion.payload + updateDesc.size();
        updateDesc.CopySlots(dest, payload);
      });
  COUNTERS_BLOCK() {
    WorkerCounters::myCounters().cc_update_versions_created[treeId]++;
  }

  // WAL
  if (xIter.mBTree.config.mEnableWal) {
    auto prevWorkerId = mWorkerId;
    auto prevTxId = mTxId;
    auto prevCommandId = mCommandId;
    auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WALUpdateSSIP>(
        key.size() + deltaPayloadSize, key, updateDesc, deltaPayloadSize,
        prevWorkerId, prevTxId, prevCommandId);
    auto dest = walHandler->payload + key.size() + updateDesc.size();
    updateDesc.CopySlots(dest, payload);
    updateDesc.XORSlots(dest, payload);
    walHandler.SubmitWal();
  }

  // Update
  MutableSlice rawVal = xIter.MutableVal();
  DCHECK(updateDesc.TotalSize() - updateDesc.size() ==
         rawVal.length() - sizeof(ChainedTuple));
  updateCallBack(MutableSlice(payload, rawVal.length() - sizeof(ChainedTuple)));
  mWorkerId = cr::Worker::my().mWorkerId;
  mTxId = cr::activeTX().startTS();
  mCommandId = commandId;

  WriteUnlock();
  xIter.MarkAsDirty();
  xIter.UpdateContentionStats();
}

} // namespace leanstore::storage::btree
