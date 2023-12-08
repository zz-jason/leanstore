#include "BTreeVI.hpp"

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
    explainIfNot(ret == OP_RESULT::OK);
    if (ret != OP_RESULT::OK) {
      JUMPMU_RETURN OP_RESULT::NOT_FOUND;
    }
    [[maybe_unused]] const auto tupleHead =
        *reinterpret_cast<const ChainedTuple*>(iterator.value().data());
    iterator.assembleKey();
    auto reconstruct = reconstructTuple(iterator.key(), iterator.value(),
                                        [&](Slice val) { valCallback(val); });
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
        reconstruct = reconstructTuple(iterator.key(), iterator.value(),
                                       [&](Slice val) { valCallback(val); });
      }
    }

    if (ret != OP_RESULT::ABORT_TX && ret != OP_RESULT::OK) {
      // For debugging
      raise(SIGTRAP);
      cout << endl;
      cout << u64(std::get<1>(reconstruct)) << " , " << mTreeId << endl;
    }

    cr::Worker::my().mLogging.checkLogDepdency(tupleHead.mWorkerId,
                                               tupleHead.tx_ts);

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
      HybridPageGuard<BTreeNode> leaf;

      // Find the correct page containing the key
      FindLeafCanJump(key, leaf);

      // Find the correct slot of the key
      s16 slotId = leaf->lowerBound<true>(key);

      // Return NOT_FOUND if could find the slot
      if (slotId == -1) {
        leaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OP_RESULT::NOT_FOUND;
      }

      auto tupleHead = *reinterpret_cast<Tuple*>(leaf->ValData(slotId));
      leaf.JumpIfModifiedByOthers();

      // Return NOT_FOUND if the tuple is not visible
      if (!isVisibleForMe(tupleHead.mWorkerId, tupleHead.tx_ts, false)) {
        JUMPMU_RETURN OP_RESULT::NOT_FOUND;
      }

      u32 offset = 0;
      switch (tupleHead.tuple_format) {
      case TupleFormat::CHAINED: {
        offset = sizeof(ChainedTuple);
        break;
      }
      case TupleFormat::FAT_TUPLE_DIFFERENT_ATTRIBUTES: {
        offset = sizeof(FatTupleDifferentAttributes);
        break;
      }
      default: {
        leaf.JumpIfModifiedByOthers();
        UNREACHABLE();
      }
      }

      // Fiund target payload, apply the callback
      Slice payload = leaf->Value(slotId);
      payload.remove_prefix(offset);
      valCallback(payload);
      leaf.JumpIfModifiedByOthers();

      COUNTERS_BLOCK() {
        WorkerCounters::myCounters().cc_read_chains[mTreeId]++;
        WorkerCounters::myCounters().cc_read_versions_visited[mTreeId] += 1;
      }

      cr::Worker::my().mLogging.checkLogDepdency(tupleHead.mWorkerId,
                                                 tupleHead.tx_ts);

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
    UpdateSameSizeInPlaceDescriptor& update_descriptor) {
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

  cr::activeTX().markAsWrite();
  cr::Worker::my().mLogging.walEnsureEnoughSpace(PAGE_SIZE * 1);
  OP_RESULT ret;

  // 20K instructions more
  JUMPMU_TRY() {
    BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
    ret = iterator.seekExact(key);
    if (ret != OP_RESULT::OK) {
      if (cr::activeTX().isOLAP() && ret == OP_RESULT::NOT_FOUND) {
        const bool removed_tuple_found =
            mGraveyard->Lookup(key, [&](Slice) {}) == OP_RESULT::OK;
        if (removed_tuple_found) {
          JUMPMU_RETURN OP_RESULT::ABORT_TX;
        }
      }

      raise(SIGTRAP);
      JUMPMU_RETURN ret;
    }

    // Record is found
  restart : {
    MutableSlice primaryPayload = iterator.mutableValue();
    auto& tuple = *reinterpret_cast<Tuple*>(primaryPayload.data());
    if (tuple.IsWriteLocked() ||
        !isVisibleForMe(tuple.mWorkerId, tuple.tx_ts, true)) {
      JUMPMU_RETURN OP_RESULT::ABORT_TX;
    }
    tuple.WriteLock();
    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().cc_update_chains[mTreeId]++;
    }

    if (tuple.tuple_format == TupleFormat::FAT_TUPLE_DIFFERENT_ATTRIBUTES) {
      const bool res =
          reinterpret_cast<FatTupleDifferentAttributes*>(&tuple)->update(
              iterator, key, callback, update_descriptor);
      reinterpret_cast<Tuple*>(iterator.mutableValue().data())->WriteUnlock();
      // Attention: tuple pointer is not valid here

      iterator.markAsDirty();
      iterator.UpdateContentionStats();

      if (!res) {
        // Converted back to chained -> restart
        goto restart;
      }

      JUMPMU_RETURN OP_RESULT::OK;
    }

    auto& tupleHead = *reinterpret_cast<ChainedTuple*>(primaryPayload.data());
    if (FLAGS_vi_fat_tuple) {
      bool convert_to_fat_tuple =
          tupleHead.command_id != Tuple::INVALID_COMMANDID &&
          cr::Worker::sOldestOltpStartTx != cr::Worker::sOldestAllStartTs &&
          !(tupleHead.mWorkerId == cr::Worker::my().mWorkerId &&
            tupleHead.tx_ts == cr::activeTX().startTS());

      // -------------------------------------------------------------------------------------
      if (FLAGS_vi_fat_tuple_trigger == 0) {
        if (cr::Worker::my().cc.isVisibleForAll(tupleHead.mWorkerId,
                                                tupleHead.tx_ts)) {
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
      // -------------------------------------------------------------------------------------
      if (convert_to_fat_tuple) {
        COUNTERS_BLOCK() {
          WorkerCounters::myCounters().cc_fat_tuple_triggered[mTreeId]++;
        }
        tupleHead.updates_counter = 0;
        const bool convert_ret =
            convertChainedToFatTupleDifferentAttributes(iterator);
        if (convert_ret) {
          iterator.mGuardedLeaf->mHasGarbage = true;
          COUNTERS_BLOCK() {
            WorkerCounters::myCounters().cc_fat_tuple_convert[mTreeId]++;
          }
        }
        goto restart;
        UNREACHABLE();
      }
    } else if (FLAGS_vi_fat_tuple_alternative) {
      if (!cr::Worker::my().cc.isVisibleForAll(tupleHead.mWorkerId,
                                               tupleHead.tx_ts)) {
        cr::Worker::my().cc.retrieveVersion(
            tupleHead.mWorkerId, tupleHead.tx_ts, tupleHead.command_id,
            [&](const u8* version_payload, u64) {
              auto& version =
                  *reinterpret_cast<const Version*>(version_payload);
              cr::Worker::my().cc.retrieveVersion(
                  version.mWorkerId, version.tx_id, version.command_id,
                  [&](const u8*, u64) {});
            });
      }
    }
  }
    // Update in chained mode
    MutableSlice primaryPayload = iterator.mutableValue();
    auto& tupleHead = *reinterpret_cast<ChainedTuple*>(primaryPayload.data());
    const u16 delta_and_descriptor_size =
        update_descriptor.size() + update_descriptor.diffLength();
    const u16 version_payload_length =
        delta_and_descriptor_size + sizeof(UpdateVersion);
    COMMANDID command_id;
    // -------------------------------------------------------------------------------------
    // Write the ChainedTupleDelta
    if (!FLAGS_vi_fupdate_chained) {
      command_id = cr::Worker::my().cc.insertVersion(
          mTreeId, false, version_payload_length, [&](u8* version_payload) {
            auto& secondary_version = *new (version_payload) UpdateVersion(
                tupleHead.mWorkerId, tupleHead.tx_ts, tupleHead.command_id,
                true);
            std::memcpy(secondary_version.payload, &update_descriptor,
                        update_descriptor.size());
            BTreeLL::generateDiff(update_descriptor,
                                  secondary_version.payload +
                                      update_descriptor.size(),
                                  tupleHead.payload);
          });
      COUNTERS_BLOCK() {
        WorkerCounters::myCounters().cc_update_versions_created[mTreeId]++;
      }
    }
    // -------------------------------------------------------------------------------------
    // WAL
    auto walHandler = iterator.mGuardedLeaf.ReserveWALPayload<WALUpdateSSIP>(
        key.size() + delta_and_descriptor_size);
    walHandler->type = WALPayload::TYPE::WALUpdate;
    walHandler->key_length = key.size();
    walHandler->delta_length = delta_and_descriptor_size;
    walHandler->before_worker_id = tupleHead.mWorkerId;
    walHandler->before_tx_id = tupleHead.tx_ts;
    walHandler->before_command_id = tupleHead.command_id;
    std::memcpy(walHandler->payload, key.data(), key.size());
    std::memcpy(walHandler->payload + key.size(), &update_descriptor,
                update_descriptor.size());
    BTreeLL::generateDiff(update_descriptor,
                          walHandler->payload + key.size() +
                              update_descriptor.size(),
                          tupleHead.payload);
    // Update
    callback(Slice(tupleHead.payload,
                   primaryPayload.length() - sizeof(ChainedTuple)));
    BTreeLL::generateXORDiff(update_descriptor,
                             walHandler->payload + key.size() +
                                 update_descriptor.size(),
                             tupleHead.payload);
    walHandler.SubmitWal();
    // -------------------------------------------------------------------------------------
    cr::Worker::my().mLogging.checkLogDepdency(tupleHead.mWorkerId,
                                               tupleHead.tx_ts);
    // -------------------------------------------------------------------------------------
    tupleHead.mWorkerId = cr::Worker::my().mWorkerId;
    tupleHead.tx_ts = cr::activeTX().startTS();
    tupleHead.command_id = command_id;
    // -------------------------------------------------------------------------------------
    tupleHead.WriteUnlock();
    iterator.markAsDirty();
    iterator.UpdateContentionStats();
    // -------------------------------------------------------------------------------------
    JUMPMU_RETURN OP_RESULT::OK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OP_RESULT::OTHER;
}

OP_RESULT BTreeVI::insert(Slice key, Slice val) {
  // check implicit transaction
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
    auto doc = BTreeGeneric::ToJSON(*this);
    DLOG(INFO) << "BTreeVI after insert: " << leanstore::utils::JsonToStr(&doc);
  });

  cr::activeTX().markAsWrite();
  cr::Worker::my().mLogging.walEnsureEnoughSpace(PAGE_SIZE * 1);
  u16 payload_length = val.size() + sizeof(ChainedTuple);

  while (true) {
    JUMPMU_TRY() {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekToInsert(key);

      if (ret == OP_RESULT::DUPLICATE) {
        auto primaryPayload = iterator.mutableValue();
        auto& primaryVersion =
            *reinterpret_cast<ChainedTuple*>(primaryPayload.data());
        auto writeLocked = primaryVersion.IsWriteLocked();
        auto visibleForMe =
            isVisibleForMe(primaryVersion.mWorkerId, primaryVersion.tx_ts);
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

      ret = iterator.enoughSpaceInCurrentNode(key, payload_length);
      if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
        iterator.splitForKey(key);
        JUMPMU_CONTINUE;
      }

      // WAL
      auto walHandler = iterator.mGuardedLeaf.ReserveWALPayload<WALInsert>(
          key.size() + val.size(), key, val);
      walHandler.SubmitWal();

      iterator.insertInCurrentNode(key, payload_length);
      MutableSlice payload = iterator.mutableValue();
      auto& primaryVersion = *new (payload.data()) ChainedTuple(
          cr::Worker::my().mWorkerId, cr::activeTX().startTS());
      std::memcpy(primaryVersion.payload, val.data(), val.size());

      if (cr::activeTX().mTxMode == TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT) {
        primaryVersion.tx_ts = MSB | 0;
      }

      iterator.markAsDirty();
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
  cr::Worker::my().mLogging.walEnsureEnoughSpace(PAGE_SIZE * 1);

  JUMPMU_TRY() {
    BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
    OP_RESULT ret = iterator.seekExact(key);
    if (ret != OP_RESULT::OK) {
      if (cr::activeTX().isOLAP() && ret == OP_RESULT::NOT_FOUND) {
        const bool removed_tuple_found =
            mGraveyard->Lookup(key, [&](Slice) {}) == OP_RESULT::OK;
        if (removed_tuple_found) {
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

    auto payload = iterator.mutableValue();
    ChainedTuple& chain_head = *reinterpret_cast<ChainedTuple*>(payload.data());

    // TODO: removing fat tuple is not supported atm
    ENSURE(chain_head.tuple_format == TupleFormat::CHAINED);

    if (chain_head.IsWriteLocked() ||
        !isVisibleForMe(chain_head.mWorkerId, chain_head.tx_ts, true)) {
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
        iterator.mGuardedLeaf.guard.version;
    dangling_pointer.head_slot = iterator.mSlotId;
    u16 value_length = iterator.value().length() - sizeof(ChainedTuple);
    u16 version_payload_length =
        sizeof(RemoveVersion) + value_length + key.size();
    COMMANDID command_id = cr::Worker::my().cc.insertVersion(
        mTreeId, true, version_payload_length, [&](u8* secondary_payload) {
          auto& secondary_version = *new (secondary_payload) RemoveVersion(
              chain_head.mWorkerId, chain_head.tx_ts, chain_head.command_id,
              key.size(), value_length);
          secondary_version.dangling_pointer = dangling_pointer;
          std::memcpy(secondary_version.payload, key.data(), key.size());
          std::memcpy(secondary_version.payload + key.size(),
                      chain_head.payload, value_length);
        });

    // WAL
    auto beforeWorkerId(chain_head.mWorkerId);
    auto beforeTxId(chain_head.tx_ts);
    auto BeforeCommandId(chain_head.command_id);
    auto walHandler = iterator.mGuardedLeaf.ReserveWALPayload<WALRemove>(
        key.size() + value_length, key, Slice(chain_head.payload, value_length),
        beforeWorkerId, beforeTxId, BeforeCommandId);
    walHandler.SubmitWal();

    if (FLAGS_wal_tuple_rfa) {
      iterator.mGuardedLeaf.incrementGSN();
    }

    if (payload.length() - sizeof(ChainedTuple) > 1) {
      iterator.shorten(sizeof(ChainedTuple));
    }
    chain_head.is_removed = true;
    chain_head.mWorkerId = cr::Worker::my().mWorkerId;
    chain_head.tx_ts = cr::activeTX().startTS();
    chain_head.command_id = command_id;

    chain_head.WriteUnlock();
    iterator.markAsDirty();

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
    [[maybe_unused]] Slice key, Slice payload,
    std::function<void(Slice value)> callback) {
  u16 chain_length = 1;
  u16 materialized_value_length;
  std::unique_ptr<u8[]> materialized_value;
  const ChainedTuple& chain_head =
      *reinterpret_cast<const ChainedTuple*>(payload.data());
  if (isVisibleForMe(chain_head.mWorkerId, chain_head.tx_ts, false)) {
    if (chain_head.is_removed) {
      return {OP_RESULT::NOT_FOUND, 1};
    } else {
      callback(
          Slice(chain_head.payload, payload.length() - sizeof(ChainedTuple)));
      return {OP_RESULT::OK, 1};
    }
  }

  // Head is not visible
  materialized_value_length = payload.length() - sizeof(ChainedTuple);
  materialized_value = std::make_unique<u8[]>(materialized_value_length);
  std::memcpy(materialized_value.get(), chain_head.payload,
              materialized_value_length);
  WORKERID next_worker_id = chain_head.mWorkerId;
  TXID next_tx_id = chain_head.tx_ts;
  COMMANDID next_command_id = chain_head.command_id;

  while (true) {
    bool found = cr::Worker::my().cc.retrieveVersion(
        next_worker_id, next_tx_id, next_command_id,
        [&](const u8* version_payload, u64 version_length) {
          const auto& version =
              *reinterpret_cast<const Version*>(version_payload);
          if (version.type == Version::TYPE::UPDATE) {
            const auto& update_version =
                *reinterpret_cast<const UpdateVersion*>(version_payload);
            if (update_version.is_delta) {
              // Apply delta
              const auto& update_descriptor =
                  *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(
                      update_version.payload);
              BTreeLL::applyDiff(update_descriptor, materialized_value.get(),
                                 update_version.payload +
                                     update_descriptor.size());
            } else {
              materialized_value_length =
                  version_length - sizeof(UpdateVersion);
              materialized_value =
                  std::make_unique<u8[]>(materialized_value_length);
              std::memcpy(materialized_value.get(), update_version.payload,
                          materialized_value_length);
            }
          } else if (version.type == Version::TYPE::REMOVE) {
            const auto& remove_version =
                *reinterpret_cast<const RemoveVersion*>(version_payload);
            materialized_value_length = remove_version.value_length;
            materialized_value =
                std::make_unique<u8[]>(materialized_value_length);
            std::memcpy(materialized_value.get(), remove_version.payload,
                        materialized_value_length);
          } else {
            UNREACHABLE();
          }

          next_worker_id = version.mWorkerId;
          next_tx_id = version.tx_id;
          next_command_id = version.command_id;
        });
    if (!found) {
      cerr << std::find(cr::Worker::my().cc.local_workers_start_ts.get(),
                        cr::Worker::my().cc.local_workers_start_ts.get() +
                            cr::Worker::my().mNumAllWorkers,
                        next_tx_id) -
                  cr::Worker::my().cc.local_workers_start_ts.get()
           << endl;
      cerr << "tls = " << cr::Worker::my().mActiveTx.startTS() << endl;
      RAISE_WHEN(true);
      RAISE_WHEN(next_command_id != ChainedTuple::INVALID_COMMANDID);
      return {OP_RESULT::NOT_FOUND, chain_length};
    }
    if (isVisibleForMe(next_worker_id, next_tx_id, false)) {
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
