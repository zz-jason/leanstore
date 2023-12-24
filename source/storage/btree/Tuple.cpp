#include "BTreeVI.hpp"
#include "concurrency-recovery/CRMG.hpp"
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

void FatTuple::undoLastUpdate() {
  ENSURE(mNumDeltas >= 1);
  auto& delta = getDelta(mNumDeltas - 1);
  mWorkerId = delta.mWorkerId;
  mTxId = delta.mTxId;
  mCommandId = delta.mCommandId;
  mNumDeltas -= 1;
  const u32 totalDeltaSize = delta.TotalSize();
  mDataOffset += totalDeltaSize;
  used_space -= totalDeltaSize + sizeof(u16);
  BTreeLL::applyDiff(delta.getDescriptor(), GetValPtr(),
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
    assert(fatTuple.total_space >=
           (fatTuple.used_space + delta_length + sizeof(u16)));
    const u16 i = fatTuple.mNumDeltas++;
    fatTuple.used_space += delta_length + sizeof(u16);
    fatTuple.mDataOffset -= delta_length;
    fatTuple.getDeltaOffsets()[i] = fatTuple.mDataOffset;
    std::memcpy(fatTuple.payload + fatTuple.mDataOffset, delta, delta_length);
  };

  // Delete for all visible deltas, atm using cheap visibility check
  if (cr::Worker::my().cc.isVisibleForAll(mWorkerId, mTxId)) {
    mNumDeltas = 0;
    mDataOffset = total_space;
    used_space = mValSize;
    return; // Done
  }

  u16 deltas_visible_by_all_counter = 0;
  for (s32 i = mNumDeltas - 1; i >= 1; i--) {
    auto& delta = getDelta(i);
    if (cr::Worker::my().cc.isVisibleForAll(delta.mWorkerId, delta.mTxId)) {
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

  auto bufferSize = total_space + sizeof(FatTuple);
  auto buffer = utils::ArrayOnStack<u8>(bufferSize);
  auto& newFatTuple = *new (buffer) FatTuple(total_space);
  newFatTuple.mWorkerId = mWorkerId;
  newFatTuple.mTxId = mTxId;
  newFatTuple.mCommandId = mCommandId;
  newFatTuple.used_space += mValSize;
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
        for (s16 s_i = 0; s_i < descriptor_i.count; s_i++) {
          diffSlotsMap[descriptor_i.mDiffSlots[s_i]] = std::basic_string<u8>(
              delta_diff_ptr, descriptor_i.mDiffSlots[s_i].length);
          delta_diff_ptr += descriptor_i.mDiffSlots[s_i].length;
        }
      }
      u32 totalNewDeltaSize = sizeof(Delta) +
                              sizeof(UpdateSameSizeInPlaceDescriptor) +
                              (sizeof(UpdateDiffSlot) * diffSlotsMap.size());
      for (auto& slot_itr : diffSlotsMap) {
        totalNewDeltaSize += slot_itr.second.size();
      }
      auto& mergeDelta = newFatTuple.allocateDelta(totalNewDeltaSize);
      mergeDelta = getDelta(zone_begin);
      UpdateSameSizeInPlaceDescriptor& merge_descriptor =
          mergeDelta.getDescriptor();
      merge_descriptor.count = diffSlotsMap.size();
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

  std::memcpy(this, buffer, bufferSize);
  assert(total_space >= used_space);

  DEBUG_BLOCK() {
    u32 should_used_space = mValSize;
    for (u32 i = 0; i < mNumDeltas; i++) {
      should_used_space += sizeof(u16) + getDelta(i).TotalSize();
    }
    assert(used_space == should_used_space);
  }
}

bool FatTuple::hasSpaceFor(
    const UpdateSameSizeInPlaceDescriptor& updateDescriptor) {
  const u32 needed_space =
      updateDescriptor.TotalSize() + sizeof(u16) + sizeof(Delta);
  return (mDataOffset - (mValSize + (mNumDeltas * sizeof(u16)))) >=
         needed_space;
}

Delta& FatTuple::allocateDelta(u32 totalDeltaSize) {
  assert((total_space - used_space) >= (totalDeltaSize + sizeof(u16)));
  used_space += totalDeltaSize + sizeof(u16);
  mDataOffset -= totalDeltaSize;
  const u32 deltaId = mNumDeltas++;
  getDeltaOffsets()[deltaId] = mDataOffset;
  return *new (&getDelta(deltaId)) Delta();
}

// Caller should take care of WAL
void FatTuple::append(UpdateSameSizeInPlaceDescriptor& updateDescriptor) {
  const u32 totalDeltaSize = updateDescriptor.TotalSize() + sizeof(Delta);
  auto& newDelta = allocateDelta(totalDeltaSize);
  newDelta.mWorkerId = this->mWorkerId;
  newDelta.mTxId = this->mTxId;
  newDelta.mCommandId = this->mCommandId;
  std::memcpy(newDelta.payload, &updateDescriptor, updateDescriptor.size());
  BTreeLL::generateDiff(updateDescriptor,
                        newDelta.payload + updateDescriptor.size(),
                        this->GetValPtr());
}

// Pre: tuple is write locked
bool FatTuple::update(BTreeExclusiveIterator& xIter, Slice key, ValCallback cb,
                      UpdateSameSizeInPlaceDescriptor& updateDescriptor) {
  utils::Timer timer(CRCounters::myCounters().cc_ms_fat_tuple);
cont : {
  auto fatTuple = reinterpret_cast<FatTuple*>(xIter.MutableVal().data());
  if (FLAGS_vi_fupdate_fat_tuple) {
    cb(fatTuple->GetValue());
    return true;
  }

  if (fatTuple->hasSpaceFor(updateDescriptor)) {
    fatTuple->append(updateDescriptor);

    // WAL
    const u64 deltaSize = updateDescriptor.TotalSize();
    auto prevWorkerId = fatTuple->mWorkerId;
    auto prevTxId = fatTuple->mTxId;
    auto prevCommandId = fatTuple->mCommandId;

    auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WALUpdateSSIP>(
        key.size() + deltaSize, key, updateDescriptor, deltaSize, prevWorkerId,
        prevTxId, prevCommandId);

    fatTuple->mWorkerId = cr::Worker::my().mWorkerId;
    fatTuple->mTxId = cr::activeTX().startTS();
    // A version is not inserted in versions space however. Needed for decompose
    fatTuple->mCommandId = cr::Worker::my().mCommandId++;

    // Update the value in-place
    BTreeLL::generateDiff(updateDescriptor,
                          walHandler->payload + key.size() +
                              updateDescriptor.size(),
                          fatTuple->GetValPtr());
    cb(fatTuple->GetValue());
    BTreeLL::generateXORDiff(updateDescriptor,
                             walHandler->payload + key.size() +
                                 updateDescriptor.size(),
                             fatTuple->GetValPtr());
    walHandler.SubmitWal();
  } else {
    fatTuple->garbageCollection();
    if (fatTuple->hasSpaceFor(updateDescriptor)) {
      goto cont;
    } else {
      if (FLAGS_tmp6) {
        fatTuple->mNumDeltas = 0;
        fatTuple->used_space = fatTuple->mValSize;
        fatTuple->mDataOffset = fatTuple->total_space;
        return false;
      } else {
        const u32 new_length = fatTuple->mValSize + sizeof(ChainedTuple);
        fatTuple->convertToChained(xIter.mBTree.mTreeId);
        ENSURE(new_length < xIter.value().length());
        xIter.shorten(new_length);
        return false;
      }
      /*
      if (fatTuple->total_space < maxFatTupleLength()) {
        auto new_fat_tuple_length =
            std::min<u32>(maxFatTupleLength(), fatTuple->total_space * 2);
        auto buffer = utils::ArrayOnStack<u8>(FLAGS_page_size);
        ENSURE(xIter.value().length() <= FLAGS_page_size);
        std::memcpy(buffer, xIter.value().data(), xIter.value().length());
        //
        const bool did_extend = xIter.extendPayload(new_fat_tuple_length);
        ENSURE(did_extend);
        //
        std::memcpy(xIter.MutableVal().data(), buffer,
                    new_fat_tuple_length);
        fatTuple = reinterpret_cast<FatTuple*>(
            xIter.MutableVal().data());
        // ATTENTION fatTuple->total_space = new_fat_tuple_length -
        sizeof(FatTuple);
        goto cont;
      } else {
        TODOException();
      }
      */
    }
  }
  assert(fatTuple->total_space >= fatTuple->used_space);
  return true;
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
    auto materializedValue = utils::ArrayOnStack<u8>(mValSize);
    std::memcpy(materializedValue, GetValPtr(), mValSize);
    // we have to apply the diffs
    u16 numVisitedVersions = 2;
    for (int i = mNumDeltas - 1; i >= 0; i--) {
      const auto& delta = getDelta(i);
      const auto& desc = delta.getDescriptor();
      BTreeLL::applyDiff(desc, materializedValue, delta.payload + desc.size());
      if (cr::Worker::my().cc.VisibleForMe(delta.mWorkerId, delta.mTxId)) {
        valCallback(Slice(materializedValue, mValSize));
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
  auto tmpPage = utils::ArrayOnStack<u8>(tmpPageSize);
  auto& newFatTuple = *new (tmpPage) FatTuple(newLength);
  newFatTuple.mWorkerId = mWorkerId;
  newFatTuple.mTxId = mTxId;
  newFatTuple.mCommandId = mCommandId;
  newFatTuple.used_space += mValSize;
  newFatTuple.mValSize = mValSize;
  std::memcpy(newFatTuple.payload, payload, mValSize); // Copy value
  auto append_ll = [](FatTuple& fatTuple, u8* delta, u16 delta_length) {
    assert(fatTuple.total_space >=
           (fatTuple.used_space + delta_length + sizeof(u16)));
    const u16 i = fatTuple.mNumDeltas++;
    fatTuple.used_space += delta_length + sizeof(u16);
    fatTuple.mDataOffset -= delta_length;
    fatTuple.getDeltaOffsets()[i] = fatTuple.mDataOffset;
    std::memcpy(fatTuple.payload + fatTuple.mDataOffset, delta, delta_length);
  };
  for (u64 i = 0; i < mNumDeltas; i++) {
    append_ll(newFatTuple, reinterpret_cast<u8*>(&getDelta(i)),
              getDelta(i).TotalSize());
  }
  std::memcpy(this, tmpPage, tmpPageSize);
  assert(total_space >= used_space);
}

bool BTreeVI::convertChainedToFatTuple(BTreeExclusiveIterator& iterator) {
  utils::Timer timer(CRCounters::myCounters().cc_ms_fat_tuple_conversion);
  u16 number_of_deltas_to_replace = 0;
  std::vector<u8> dynamic_buffer;
  dynamic_buffer.resize(maxFatTupleLength());
  auto fatTuple = new (dynamic_buffer.data())
      FatTuple(dynamic_buffer.size() - sizeof(FatTuple));

  WORKERID next_worker_id;
  TXID next_tx_id;
  COMMANDID next_command_id;

  // Process the chain head
  MutableSlice head = iterator.MutableVal();
  auto& chain_head = *reinterpret_cast<ChainedTuple*>(head.data());
  ENSURE(chain_head.IsWriteLocked());

  fatTuple->mValSize = head.length() - sizeof(ChainedTuple);
  std::memcpy(fatTuple->payload + fatTuple->used_space, chain_head.payload,
              fatTuple->mValSize);
  fatTuple->used_space += fatTuple->mValSize;
  fatTuple->mWorkerId = chain_head.mWorkerId;
  fatTuple->mTxId = chain_head.mTxId;
  fatTuple->mCommandId = chain_head.mCommandId;

  next_worker_id = chain_head.mWorkerId;
  next_tx_id = chain_head.mTxId;
  next_command_id = chain_head.mCommandId;
  // TODO: check for used_space overflow
  bool abort_conversion = false;
  while (!abort_conversion) {
    if (cr::Worker::my().cc.isVisibleForAll(
            next_worker_id,
            next_tx_id)) { // Pruning versions space might get delayed
      break;
    }

    if (!cr::Worker::my().cc.retrieveVersion(
            next_worker_id, next_tx_id, next_command_id,
            [&](const u8* version, [[maybe_unused]] u64 payload_length) {
              number_of_deltas_to_replace++;
              const auto& chainedDelta =
                  *reinterpret_cast<const UpdateVersion*>(version);
              ENSURE(chainedDelta.type == Version::TYPE::UPDATE);
              ENSURE(chainedDelta.is_delta);
              const auto& updateDescriptor =
                  *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(
                      chainedDelta.payload);
              const u32 descriptor_and_diff_length =
                  updateDescriptor.TotalSize();
              const u32 needed_space =
                  sizeof(Delta) + descriptor_and_diff_length;

              if (!fatTuple->hasSpaceFor(updateDescriptor)) {
                fatTuple->garbageCollection();
                if (!fatTuple->hasSpaceFor(updateDescriptor)) {
                  abort_conversion = true;
                  return;
                }
              }

              // Add a FatTuple::Delta
              auto& newDelta = fatTuple->allocateDelta(needed_space);
              newDelta.mWorkerId = chainedDelta.mWorkerId;
              newDelta.mTxId = chainedDelta.mTxId;
              newDelta.mCommandId = chainedDelta.mCommandId;

              // Copy Descriptor + Diff
              std::memcpy(newDelta.payload, &updateDescriptor,
                          descriptor_and_diff_length);

              next_worker_id = chainedDelta.mWorkerId;
              next_tx_id = chainedDelta.mTxId;
              next_command_id = chainedDelta.mCommandId;
            })) {
      break;
    }
  }
  if (abort_conversion) {
    chain_head.WriteUnlock();
    return false;
  }

  // Reverse the order at the end
  fatTuple->ReverseDeltas();

  fatTuple->garbageCollection();
  if (fatTuple->used_space > maxFatTupleLength()) {
    chain_head.WriteUnlock();
    return false;
  }
  assert(fatTuple->total_space >= fatTuple->used_space);
  // We can not simply change total_space because this affects mDataOffset
  if (number_of_deltas_to_replace > convertToFatTupleThreshold()) {
    // Finalize the new FatTuple
    // TODO: corner cases, more careful about space usage

    ENSURE(fatTuple->total_space >= fatTuple->used_space);
    // cerr << fatTuple->total_space << "," << fatTuple->used_space << endl;
    // fatTuple->total_space = fatTuple->used_space;
    // fatTuple->resize(fatTuple->used_space);
    const u16 fat_tuple_length = sizeof(FatTuple) + fatTuple->total_space;
    if (iterator.value().length() < fat_tuple_length) {
      ENSURE(reinterpret_cast<const Tuple*>(iterator.value().data())->mFormat ==
             TupleFormat::CHAINED);
      const bool did_extend = iterator.extendPayload(fat_tuple_length);
      ENSURE(did_extend);
    } else {
      iterator.shorten(fat_tuple_length);
    }
    std::memcpy(iterator.MutableVal().data(), dynamic_buffer.data(),
                fat_tuple_length);
    iterator.MarkAsDirty();
    return true;
  } else {
    chain_head.WriteUnlock();
    return false;
  }
}

void FatTuple::convertToChained(TREEID treeId) {
  auto prevWorkerId = mWorkerId;
  auto prevTxId = mTxId;
  auto prevCommandId = mCommandId;
  for (s64 i = mNumDeltas - 1; i >= 0; i--) {
    auto& delta = getDelta(i);
    const u32 version_payload_length =
        delta.getDescriptor().TotalSize() + sizeof(UpdateVersion);
    cr::Worker::my().cc.mHistoryTree.insertVersion(
        prevWorkerId, prevTxId, prevCommandId, treeId, false,
        version_payload_length,
        [&](u8* version_payload) {
          auto& secondary_version = *new (version_payload) UpdateVersion(
              delta.mWorkerId, delta.mTxId, delta.mCommandId, true);
          std::memcpy(secondary_version.payload, delta.payload,
                      version_payload_length - sizeof(UpdateVersion));
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

} // namespace leanstore::storage::btree
