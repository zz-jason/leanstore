#include "BTreeVI.hpp"

#include "Units.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "utils/Defer.hpp"
#include "utils/JsonUtil.hpp"

#include "gflags/gflags.h"
#include <glog/logging.h>

#include <signal.h>

using namespace std;
using namespace leanstore::storage;
using OP_RESULT = leanstore::OP_RESULT;

// Assumptions made in this implementation:
// 1. We don't insert an already removed key
// 2. Secondary Versions contain delta
//
// Keep in mind that garbage collection may leave pages completely empty
// Missing points: FatTuple::remove, garbage leaves can escape from us

namespace leanstore {
namespace storage {
namespace btree {

OP_RESULT BTreeVI::Lookup(Slice key, ValCallback valCallback) {
  DCHECK(cr::Worker::my().IsTxStarted());

  OP_RESULT ret = lookupOptimistic(key, valCallback);
  if (ret == OP_RESULT::OTHER) {
    return lookupPessimistic(key, valCallback);
  }
  return ret;
}

OP_RESULT BTreeVI::lookupPessimistic(Slice key, ValCallback valCallback) {
  JUMPMU_TRY() {
    BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this),
                                 LATCH_FALLBACK_MODE::SHARED);
    auto ret = iterator.seekExact(key);
    if (ret != OP_RESULT::OK) {
      JUMPMU_RETURN OP_RESULT::NOT_FOUND;
    }
    [[maybe_unused]] const auto tupleHead =
        *reinterpret_cast<const ChainedTuple*>(iterator.value().data());
    iterator.assembleKey();
    auto reconstruct =
        GetVisibleTuple(iterator.key(), iterator.value(), valCallback);
    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().cc_read_chains[mTreeId]++;
      WorkerCounters::myCounters().cc_read_versions_visited[mTreeId] +=
          std::get<1>(reconstruct);
    }
    ret = std::get<0>(reconstruct);

    if (cr::activeTX().isOLAP() && ret == OP_RESULT::NOT_FOUND) {
      BTreeSharedIterator g_iterator(*static_cast<BTreeGeneric*>(mGraveyard));
      OP_RESULT ret = g_iterator.seekExact(key);
      if (ret == OP_RESULT::OK) {
        iterator.assembleKey();
        reconstruct =
            GetVisibleTuple(iterator.key(), iterator.value(), valCallback);
      }
    }

    if (ret != OP_RESULT::ABORT_TX && ret != OP_RESULT::OK) {
      // For debugging
      raise(SIGTRAP);
      cout << endl;
      cout << u64(std::get<1>(reconstruct)) << " , " << mTreeId << endl;
    }

    cr::Worker::my().mLogging.checkLogDepdency(tupleHead.mWorkerId,
                                               tupleHead.mTxId);

    JUMPMU_RETURN ret;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OP_RESULT::OTHER;
}

OP_RESULT BTreeVI::lookupOptimistic(Slice key, ValCallback valCallback) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> guardedLeaf;

      // Find the correct page containing the key
      FindLeafCanJump(key, guardedLeaf);

      // Find the correct slot of the key
      s16 slotId = guardedLeaf->lowerBound<true>(key);

      // Return NOT_FOUND if could find the slot
      if (slotId == -1) {
        guardedLeaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OP_RESULT::NOT_FOUND;
      }

      auto tupleHead = *reinterpret_cast<Tuple*>(guardedLeaf->ValData(slotId));
      guardedLeaf.JumpIfModifiedByOthers();

      // Return NOT_FOUND if the tuple is not visible
      if (!VisibleForMe(tupleHead.mWorkerId, tupleHead.mTxId, false)) {
        JUMPMU_RETURN OP_RESULT::NOT_FOUND;
      }

      u32 offset = 0;
      switch (tupleHead.mFormat) {
      case TupleFormat::CHAINED: {
        offset = sizeof(ChainedTuple);
        break;
      }
      case TupleFormat::FAT: {
        offset = sizeof(FatTuple);
        break;
      }
      default: {
        guardedLeaf.JumpIfModifiedByOthers();
        UNREACHABLE();
      }
      }

      // Fiund target payload, apply the callback
      Slice payload = guardedLeaf->Value(slotId);
      payload.remove_prefix(offset);
      valCallback(payload);
      guardedLeaf.JumpIfModifiedByOthers();

      COUNTERS_BLOCK() {
        WorkerCounters::myCounters().cc_read_chains[mTreeId]++;
        WorkerCounters::myCounters().cc_read_versions_visited[mTreeId] += 1;
      }

      cr::Worker::my().mLogging.checkLogDepdency(tupleHead.mWorkerId,
                                                 tupleHead.mTxId);

      JUMPMU_RETURN OP_RESULT::OK;
    }
    JUMPMU_CATCH() {
    }
  }

  // lookup failed, fallback to lookupPessimistic
  return OP_RESULT::OTHER;
}

OP_RESULT BTreeVI::updateSameSizeInPlace(
    Slice key, ValCallback callback,
    UpdateSameSizeInPlaceDescriptor& updateDescriptor) {
  DCHECK(cr::Worker::my().IsTxStarted());

  cr::activeTX().markAsWrite();
  cr::Worker::my().mLogging.walEnsureEnoughSpace(FLAGS_page_size);
  OP_RESULT ret;

  // 20K instructions more
  JUMPMU_TRY() {
    BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
    ret = xIter.seekExact(key);
    if (ret != OP_RESULT::OK) {
      if (cr::activeTX().isOLAP() && ret == OP_RESULT::NOT_FOUND) {
        const bool foundRemovedTuple =
            mGraveyard->Lookup(key, [&](Slice) {}) == OP_RESULT::OK;

        // The tuple to be updated is removed, abort the transaction.
        // TODO(jian.z): should we just return NOT_FOUND?
        if (foundRemovedTuple) {
          JUMPMU_RETURN OP_RESULT::ABORT_TX;
        }
      }

      LOG(ERROR) << "Update failed, key not found, key=" << ToString(key)
                 << ", txMode=" << ToString(cr::activeTX().mTxMode)
                 << ", seek ret=" << ToString(ret);
      JUMPMU_RETURN ret;
    }

    // Record is found
  restart : {
    auto primaryPayload = xIter.MutableVal();
    auto& tuple = *reinterpret_cast<Tuple*>(primaryPayload.data());
    auto visibleForMe = VisibleForMe(tuple.mWorkerId, tuple.mTxId, true);
    if (tuple.IsWriteLocked() || !visibleForMe) {

      LOG(ERROR) << "Update failed, primary tuple is write locked or not "
                    "visible for me"
                 << ", key=" << ToString(key)
                 << ", writeLocked=" << tuple.IsWriteLocked()
                 << ", visibleForMe=" << visibleForMe;
      JUMPMU_RETURN OP_RESULT::ABORT_TX;
    }

    // write lock the tuple
    tuple.WriteLock();
    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().cc_update_chains[mTreeId]++;
    }

    if (tuple.mFormat == TupleFormat::FAT) {
      auto fatTuple = reinterpret_cast<FatTuple*>(&tuple);
      auto res = fatTuple->update(xIter, key, callback, updateDescriptor);
      reinterpret_cast<Tuple*>(xIter.MutableVal().data())->WriteUnlock();
      // Attention: tuple pointer is not valid here

      xIter.MarkAsDirty();
      xIter.UpdateContentionStats();

      if (!res) {
        // Converted back to chained -> restart
        goto restart;
      }

      JUMPMU_RETURN OP_RESULT::OK;
    }

    auto& tupleHead = *reinterpret_cast<ChainedTuple*>(primaryPayload.data());
    if (FLAGS_vi_fat_tuple) {
      bool convert_to_fat_tuple =
          tupleHead.mCommandId != INVALID_COMMANDID &&
          cr::Worker::sOldestOltpStartTx != cr::Worker::sOldestAllStartTs &&
          !(tupleHead.mWorkerId == cr::Worker::my().mWorkerId &&
            tupleHead.mTxId == cr::activeTX().startTS());

      if (FLAGS_vi_fat_tuple_trigger == 0) {
        if (cr::Worker::my().cc.isVisibleForAll(tupleHead.mWorkerId,
                                                tupleHead.mTxId)) {
          tupleHead.oldest_tx = 0;
          tupleHead.updates_counter = 0;
          convert_to_fat_tuple = false;
        } else {
          if (tupleHead.oldest_tx ==
              static_cast<u16>(cr::Worker::sOldestAllStartTs & 0xFFFF)) {
            tupleHead.updates_counter++;
          } else {
            tupleHead.oldest_tx =
                static_cast<u16>(cr::Worker::sOldestAllStartTs & 0xFFFF);
            tupleHead.updates_counter = 0;
          }
        }
        convert_to_fat_tuple &=
            tupleHead.updates_counter > convertToFatTupleThreshold();
      } else if (FLAGS_vi_fat_tuple_trigger == 1) {
        convert_to_fat_tuple &= utils::RandomGenerator::getRandU64(
                                    0, convertToFatTupleThreshold()) == 0;
      } else {
        UNREACHABLE();
      }

      if (convert_to_fat_tuple) {
        COUNTERS_BLOCK() {
          WorkerCounters::myCounters().cc_fat_tuple_triggered[mTreeId]++;
        }
        tupleHead.updates_counter = 0;
        const bool convert_ret = convertChainedToFatTuple(xIter);
        if (convert_ret) {
          xIter.mGuardedLeaf->mHasGarbage = true;
          COUNTERS_BLOCK() {
            WorkerCounters::myCounters().cc_fat_tuple_convert[mTreeId]++;
          }
        }
        goto restart;
        UNREACHABLE();
      }
    } else if (FLAGS_vi_fat_tuple_alternative) {
      if (!cr::Worker::my().cc.isVisibleForAll(tupleHead.mWorkerId,
                                               tupleHead.mTxId)) {
        cr::Worker::my().cc.retrieveVersion(
            tupleHead.mWorkerId, tupleHead.mTxId, tupleHead.mCommandId,
            [&](const u8* version_payload, u64) {
              auto& version =
                  *reinterpret_cast<const Version*>(version_payload);
              cr::Worker::my().cc.retrieveVersion(
                  version.mWorkerId, version.mTxId, version.mCommandId,
                  [&](const u8*, u64) {});
            });
      }
    }
  }
    // Update in chained mode
    MutableSlice primaryPayload = xIter.MutableVal();
    auto& tupleHead = *reinterpret_cast<ChainedTuple*>(primaryPayload.data());
    const u64 deltaSize = updateDescriptor.TotalSize();
    const u64 versionPayloadSize = deltaSize + sizeof(UpdateVersion);
    COMMANDID commandId;

    // Write the ChainedTupleDelta
    if (!FLAGS_vi_fupdate_chained) {
      commandId = cr::Worker::my().cc.insertVersion(
          mTreeId, false, versionPayloadSize, [&](u8* version_payload) {
            auto& secondary_version = *new (version_payload) UpdateVersion(
                tupleHead.mWorkerId, tupleHead.mTxId, tupleHead.mCommandId,
                true);
            std::memcpy(secondary_version.payload, &updateDescriptor,
                        updateDescriptor.size());
            BTreeLL::generateDiff(updateDescriptor,
                                  secondary_version.payload +
                                      updateDescriptor.size(),
                                  tupleHead.payload);
          });
      COUNTERS_BLOCK() {
        WorkerCounters::myCounters().cc_update_versions_created[mTreeId]++;
      }
    }

    // WAL
    auto prevWorkerId = tupleHead.mWorkerId;
    auto prevTxId = tupleHead.mTxId;
    auto beforeCommandId = tupleHead.mCommandId;
    auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WALUpdateSSIP>(
        key.size() + deltaSize, key, updateDescriptor, deltaSize, prevWorkerId,
        prevTxId, beforeCommandId);

    BTreeLL::generateDiff(updateDescriptor,
                          walHandler->payload + key.size() +
                              updateDescriptor.size(),
                          tupleHead.payload);
    // Update
    callback(Slice(tupleHead.payload,
                   primaryPayload.length() - sizeof(ChainedTuple)));
    BTreeLL::generateXORDiff(updateDescriptor,
                             walHandler->payload + key.size() +
                                 updateDescriptor.size(),
                             tupleHead.payload);
    walHandler.SubmitWal();

    cr::Worker::my().mLogging.checkLogDepdency(tupleHead.mWorkerId,
                                               tupleHead.mTxId);

    tupleHead.mWorkerId = cr::Worker::my().mWorkerId;
    tupleHead.mTxId = cr::activeTX().startTS();
    tupleHead.mCommandId = commandId;

    tupleHead.WriteUnlock();
    xIter.MarkAsDirty();
    xIter.UpdateContentionStats();

    JUMPMU_RETURN OP_RESULT::OK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OP_RESULT::OTHER;
}

OP_RESULT BTreeVI::insert(Slice key, Slice val) {
  DCHECK(cr::Worker::my().IsTxStarted());
  SCOPED_DEFER({
    rapidjson::Document doc(rapidjson::kObjectType);
    BTreeGeneric::ToJSON(*this, &doc);
    DLOG(INFO) << "BTreeVI after insert: " << leanstore::utils::JsonToStr(&doc);
  });

  cr::activeTX().markAsWrite();
  cr::Worker::my().mLogging.walEnsureEnoughSpace(FLAGS_page_size * 1);
  u16 payloadSize = val.size() + sizeof(ChainedTuple);

  while (true) {
    JUMPMU_TRY() {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekToInsert(key);

      if (ret == OP_RESULT::DUPLICATE) {
        auto primaryPayload = iterator.MutableVal();
        auto& primaryVersion =
            *reinterpret_cast<ChainedTuple*>(primaryPayload.data());
        auto writeLocked = primaryVersion.IsWriteLocked();
        auto visibleForMe =
            VisibleForMe(primaryVersion.mWorkerId, primaryVersion.mTxId);
        if (writeLocked || !visibleForMe) {
          DLOG(ERROR) << "Insert failed, writeLocked=" << writeLocked
                      << ", visibleForMe=" << visibleForMe;
          JUMPMU_RETURN OP_RESULT::ABORT_TX;
        }
        DLOG(ERROR) << "Insert failed, key is duplicated";
        JUMPMU_RETURN OP_RESULT::DUPLICATE;
        // ENSURE(false);
        // Not implemented: maybe it has been removed but no GCed
      }

      ret = iterator.enoughSpaceInCurrentNode(key, payloadSize);
      if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
        iterator.splitForKey(key);
        JUMPMU_CONTINUE;
      }

      // WAL
      auto walHandler = iterator.mGuardedLeaf.ReserveWALPayload<WALInsert>(
          key.size() + val.size(), key, val);
      walHandler.SubmitWal();

      // insert
      BTreeVI::InsertToNode(
          iterator.mGuardedLeaf, key, val, cr::Worker::my().mWorkerId,
          cr::activeTX().startTS(), cr::activeTX().mTxMode, iterator.mSlotId);
      JUMPMU_RETURN OP_RESULT::OK;
    }
    JUMPMU_CATCH() {
      DLOG(ERROR) << "Encounter unhandled jump";
      UNREACHABLE();
    }
  }
  return OP_RESULT::OTHER;
}

OP_RESULT BTreeVI::remove(Slice key) {
  auto autoCommit(false);
  if (!cr::Worker::my().IsTxStarted()) {
    DLOG(INFO) << "Start implicit transaction";
    cr::Worker::my().startTX();
    autoCommit = true;
  }
  SCOPED_DEFER({
    // auto-commit the implicit transaction
    if (autoCommit) {
      cr::Worker::my().commitTX();
    }
  });

  // TODO: remove fat tuple
  cr::activeTX().markAsWrite();
  cr::Worker::my().mLogging.walEnsureEnoughSpace(FLAGS_page_size * 1);

  JUMPMU_TRY() {
    BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
    OP_RESULT ret = iterator.seekExact(key);
    if (ret != OP_RESULT::OK) {
      if (cr::activeTX().isOLAP() && ret == OP_RESULT::NOT_FOUND) {
        const bool foundRemovedTuple =
            mGraveyard->Lookup(key, [&](Slice) {}) == OP_RESULT::OK;
        if (foundRemovedTuple) {
          JUMPMU_RETURN OP_RESULT::ABORT_TX;
        }
      }

      RAISE_WHEN(cr::activeTX().atLeastSI());
      JUMPMU_RETURN OP_RESULT::NOT_FOUND;
    }

    if (FLAGS_vi_fremove) {
      ret = iterator.removeCurrent();
      ENSURE(ret == OP_RESULT::OK);
      iterator.mergeIfNeeded();
      JUMPMU_RETURN OP_RESULT::OK;
    }

    auto payload = iterator.MutableVal();
    ChainedTuple& chain_head = *reinterpret_cast<ChainedTuple*>(payload.data());

    // TODO: removing fat tuple is not supported atm
    ENSURE(chain_head.mFormat == TupleFormat::CHAINED);

    if (chain_head.IsWriteLocked() ||
        !VisibleForMe(chain_head.mWorkerId, chain_head.mTxId, true)) {
      JUMPMU_RETURN OP_RESULT::ABORT_TX;
    }
    ENSURE(!cr::activeTX().atLeastSI() || chain_head.is_removed == false);
    if (chain_head.is_removed) {
      JUMPMU_RETURN OP_RESULT::NOT_FOUND;
    }

    chain_head.WriteLock();

    DanglingPointer dangling_pointer;
    dangling_pointer.bf = iterator.mGuardedLeaf.mBf;
    dangling_pointer.latch_version_should_be =
        iterator.mGuardedLeaf.mGuard.mVersion;
    dangling_pointer.head_slot = iterator.mSlotId;
    u16 valSize = iterator.value().length() - sizeof(ChainedTuple);
    u16 versionPayloadSize = sizeof(RemoveVersion) + valSize + key.size();
    COMMANDID commandId = cr::Worker::my().cc.insertVersion(
        mTreeId, true, versionPayloadSize, [&](u8* secondary_payload) {
          auto& secondary_version = *new (secondary_payload) RemoveVersion(
              chain_head.mWorkerId, chain_head.mTxId, chain_head.mCommandId,
              key.size(), valSize);
          secondary_version.dangling_pointer = dangling_pointer;
          std::memcpy(secondary_version.payload, key.data(), key.size());
          std::memcpy(secondary_version.payload + key.size(),
                      chain_head.payload, valSize);
        });

    // WAL
    auto prevWorkerId(chain_head.mWorkerId);
    auto prevTxId(chain_head.mTxId);
    auto prevCommandId(chain_head.mCommandId);
    auto walHandler = iterator.mGuardedLeaf.ReserveWALPayload<WALRemove>(
        key.size() + valSize, key, Slice(chain_head.payload, valSize),
        prevWorkerId, prevTxId, prevCommandId);
    walHandler.SubmitWal();

    if (FLAGS_wal_tuple_rfa) {
      iterator.mGuardedLeaf.IncPageGSN();
    }

    if (payload.length() - sizeof(ChainedTuple) > 1) {
      iterator.shorten(sizeof(ChainedTuple));
    }
    chain_head.is_removed = true;
    chain_head.mWorkerId = cr::Worker::my().mWorkerId;
    chain_head.mTxId = cr::activeTX().startTS();
    chain_head.mCommandId = commandId;

    chain_head.WriteUnlock();
    iterator.MarkAsDirty();

    JUMPMU_RETURN OP_RESULT::OK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OP_RESULT::OTHER;
}

OP_RESULT BTreeVI::scanDesc(Slice startKey, ScanCallback callback) {
  auto autoCommit(false);
  if (!cr::Worker::my().IsTxStarted()) {
    DLOG(INFO) << "Start implicit transaction";
    cr::Worker::my().startTX();
    autoCommit = true;
  }
  SCOPED_DEFER({
    // auto-commit the implicit transaction
    if (autoCommit) {
      cr::Worker::my().commitTX();
    }
  });

  if (cr::activeTX().isOLAP()) {
    TODOException();
    return OP_RESULT::ABORT_TX;
  }
  return scan<false>(startKey, callback);
}

OP_RESULT BTreeVI::scanAsc(Slice startKey, ScanCallback callback) {
  auto autoCommit(false);
  if (!cr::Worker::my().IsTxStarted()) {
    DLOG(INFO) << "Start implicit transaction";
    cr::Worker::my().startTX();
    autoCommit = true;
  }
  SCOPED_DEFER({
    // auto-commit the implicit transaction
    if (autoCommit) {
      cr::Worker::my().commitTX();
    }
  });

  if (cr::activeTX().isOLAP()) {
    return scanOLAP(startKey, callback);
  } else {
    return scan<true>(startKey, callback);
  }
}

// TODO: Implement inserts after remove cases
std::tuple<OP_RESULT, u16> BTreeVI::reconstructChainedTuple(
    [[maybe_unused]] Slice key, Slice payload, ValCallback callback) {
  const auto& chainHead = *ChainedTuple::From(payload.data());
  if (VisibleForMe(chainHead.mWorkerId, chainHead.mTxId, false)) {
    if (chainHead.is_removed) {
      return {OP_RESULT::NOT_FOUND, 1};
    }

    auto valSize = payload.length() - sizeof(ChainedTuple);
    callback(chainHead.GetValue(valSize));
    return {OP_RESULT::OK, 1};
  }

  if (chainHead.isFinal()) {
    JUMPMU_RETURN{OP_RESULT::NOT_FOUND, 1};
  }

  // Head is not visible
  u16 chain_length = 1;
  u16 materialized_value_length = payload.length() - sizeof(ChainedTuple);
  auto materialized_value = std::make_unique<u8[]>(materialized_value_length);
  std::memcpy(materialized_value.get(), chainHead.payload,
              materialized_value_length);
  WORKERID nextWorkerId = chainHead.mWorkerId;
  TXID nextTxId = chainHead.mTxId;
  COMMANDID nextCommandId = chainHead.mCommandId;

  while (true) {
    bool found = cr::Worker::my().cc.retrieveVersion(
        nextWorkerId, nextTxId, nextCommandId,
        [&](const u8* version_payload, u64 version_length) {
          const auto& version =
              *reinterpret_cast<const Version*>(version_payload);
          if (version.type == Version::TYPE::UPDATE) {
            const auto& update_version =
                *reinterpret_cast<const UpdateVersion*>(version_payload);
            if (update_version.is_delta) {
              // Apply delta
              const auto& updateDescriptor =
                  *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(
                      update_version.payload);
              BTreeLL::applyDiff(updateDescriptor, materialized_value.get(),
                                 update_version.payload +
                                     updateDescriptor.size());
            } else {
              materialized_value_length =
                  version_length - sizeof(UpdateVersion);
              materialized_value =
                  std::make_unique<u8[]>(materialized_value_length);
              std::memcpy(materialized_value.get(), update_version.payload,
                          materialized_value_length);
            }
          } else if (version.type == Version::TYPE::REMOVE) {
            const auto& removeVersion =
                *reinterpret_cast<const RemoveVersion*>(version_payload);
            materialized_value_length = removeVersion.mValSize;
            materialized_value =
                std::make_unique<u8[]>(materialized_value_length);
            std::memcpy(materialized_value.get(), removeVersion.payload,
                        materialized_value_length);
          } else {
            UNREACHABLE();
          }

          nextWorkerId = version.mWorkerId;
          nextTxId = version.mTxId;
          nextCommandId = version.mCommandId;
        });
    if (!found) {
      cerr << std::find(cr::Worker::my().cc.local_workers_start_ts.get(),
                        cr::Worker::my().cc.local_workers_start_ts.get() +
                            cr::Worker::my().mNumAllWorkers,
                        nextTxId) -
                  cr::Worker::my().cc.local_workers_start_ts.get()
           << endl;
      cerr << "tls = " << cr::Worker::my().mActiveTx.startTS() << endl;
      RAISE_WHEN(true);
      RAISE_WHEN(nextCommandId != INVALID_COMMANDID);
      return {OP_RESULT::NOT_FOUND, chain_length};
    }
    if (VisibleForMe(nextWorkerId, nextTxId, false)) {
      callback(Slice(materialized_value.get(), materialized_value_length));
      return {OP_RESULT::OK, chain_length};
    }
    chain_length++;
    ENSURE(chain_length <= FLAGS_vi_max_chain_length);
  }
  return {OP_RESULT::NOT_FOUND, chain_length};
}

} // namespace btree
} // namespace storage
} // namespace leanstore
