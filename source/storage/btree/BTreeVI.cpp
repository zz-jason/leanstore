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
    BTreeSharedIterator iter(*static_cast<BTreeGeneric*>(this));
    auto ret = iter.seekExact(key);
    if (ret != OP_RESULT::OK) {
      JUMPMU_RETURN OP_RESULT::NOT_FOUND;
    }

    auto versionsRead(0);
    std::tie(ret, versionsRead) = GetVisibleTuple(iter.value(), valCallback);
    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().cc_read_chains[mTreeId]++;
      WorkerCounters::myCounters().cc_read_versions_visited[mTreeId] +=
          versionsRead;
    }

    if (cr::activeTX().isOLAP() && ret == OP_RESULT::NOT_FOUND) {
      BTreeSharedIterator gIter(*static_cast<BTreeGeneric*>(mGraveyard));
      ret = gIter.seekExact(key);
      if (ret != OP_RESULT::OK) {
        JUMPMU_RETURN OP_RESULT::NOT_FOUND;
      }
      std::tie(ret, versionsRead) = GetVisibleTuple(iter.value(), valCallback);
      COUNTERS_BLOCK() {
        WorkerCounters::myCounters().cc_read_chains[mTreeId]++;
        WorkerCounters::myCounters().cc_read_versions_visited[mTreeId] +=
            versionsRead;
      }
    }

    if (ret != OP_RESULT::OK) {
      JUMPMU_RETURN OP_RESULT::NOT_FOUND;
    }
    JUMPMU_RETURN ret;
  }
  JUMPMU_CATCH() {
    LOG(ERROR) << "lookupPessimistic failed";
  }

  return OP_RESULT::NOT_FOUND;
}

OP_RESULT BTreeVI::lookupOptimistic(Slice key, ValCallback valCallback) {
  const size_t maxAttempts = 2;
  for (size_t i = maxAttempts; i > 0; i--) {
    JUMPMU_TRY() {
      // Find the correct page containing the key
      GuardedBufferFrame<BTreeNode> guardedLeaf;
      FindLeafCanJump(key, guardedLeaf);

      // Find the correct slot of the key
      // Return NOT_FOUND if could find the slot
      s16 slotId = guardedLeaf->lowerBound<true>(key);
      if (slotId == -1) {
        guardedLeaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OP_RESULT::NOT_FOUND;
      }

      auto rawVal = guardedLeaf->ValData(slotId);
      auto tuple = *Tuple::From(rawVal);
      guardedLeaf.JumpIfModifiedByOthers();

      // Return NOT_FOUND if the tuple is not visible
      if (!VisibleForMe(tuple.mWorkerId, tuple.mTxId, false)) {
        JUMPMU_RETURN OP_RESULT::NOT_FOUND;
      }

      u32 offset = 0;
      switch (tuple.mFormat) {
      case TupleFormat::CHAINED: {
        const auto chainedTuple = ChainedTuple::From(rawVal);
        if (chainedTuple->mIsRemoved) {
          JUMPMU_RETURN OP_RESULT::NOT_FOUND;
        }
        offset = sizeof(ChainedTuple);
        break;
      }
      case TupleFormat::FAT: {
        offset = sizeof(FatTuple);
        break;
      }
      default: {
        LOG(ERROR) << "Unhandled tuple format: "
                   << TupleFormatUtil::ToString(tuple.mFormat);
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

      JUMPMU_RETURN OP_RESULT::OK;
    }
    JUMPMU_CATCH() {
    }
  }

  // lookup failed, fallback to lookupPessimistic
  LOG(INFO) << "lookupOptimistic failed " << maxAttempts
            << " times, fallback to lookupPessimistic";
  return OP_RESULT::OTHER;
}

OP_RESULT BTreeVI::updateSameSizeInPlace(Slice key,
                                         MutValCallback updateCallBack,
                                         UpdateDesc& updateDesc) {
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
    auto rawVal = xIter.MutableVal();
    auto& tuple = *Tuple::From(rawVal.data());
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

    // update on fat tuple
    if (tuple.mFormat == TupleFormat::FAT) {
      auto& fatTuple = *FatTuple::From(rawVal.data());
      auto res = fatTuple.update(xIter, key, updateCallBack, updateDesc);
      // Attention: previous tuple pointer is not valid here
      xIter.MarkAsDirty();
      xIter.UpdateContentionStats();
      Tuple::From(rawVal.data())->WriteUnlock();
      if (!res) {
        // Converted back to chained -> restart
        goto restart;
      }

      JUMPMU_RETURN OP_RESULT::OK;
    }

    auto& chainedTuple = *ChainedTuple::From(rawVal.data());
    chainedTuple.UpdateStats();
    // convert to fat tuple if it's frequently updated by me and other workers
    if (FLAGS_enable_fat_tuple && chainedTuple.ShouldConvertToFatTuple()) {
      COUNTERS_BLOCK() {
        WorkerCounters::myCounters().cc_fat_tuple_triggered[mTreeId]++;
      }
      chainedTuple.mTotalUpdates = 0;
      if (Tuple::ToFat(xIter)) {
        // convert succeed
        xIter.mGuardedLeaf->mHasGarbage = true;
        COUNTERS_BLOCK() {
          WorkerCounters::myCounters().cc_fat_tuple_convert[mTreeId]++;
        }
      }
      goto restart;
      UNREACHABLE();
    }
  }
    // Update in chained mode
    MutableSlice rawVal = xIter.MutableVal();
    auto& chainedTuple = *ChainedTuple::From(rawVal.data());
    auto deltaPayloadSize = updateDesc.TotalSize();
    const u64 versionSize = deltaPayloadSize + sizeof(UpdateVersion);
    COMMANDID commandId = INVALID_COMMANDID;

    // Move the newest tuple to the history version tree.
    commandId = cr::Worker::my().cc.insertVersion(
        mTreeId, false, versionSize, [&](u8* versionBuf) {
          auto& updateVersion = *new (versionBuf) UpdateVersion(
              chainedTuple.mWorkerId, chainedTuple.mTxId,
              chainedTuple.mCommandId, true);
          std::memcpy(updateVersion.payload, &updateDesc, updateDesc.size());
          auto diffDest = updateVersion.payload + updateDesc.size();
          updateDesc.GenerateDiff(diffDest, chainedTuple.payload);
        });
    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().cc_update_versions_created[mTreeId]++;
    }

    // WAL
    auto prevWorkerId = chainedTuple.mWorkerId;
    auto prevTxId = chainedTuple.mTxId;
    auto prevCommandId = chainedTuple.mCommandId;
    auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WALUpdateSSIP>(
        key.size() + deltaPayloadSize, key, updateDesc, deltaPayloadSize,
        prevWorkerId, prevTxId, prevCommandId);
    auto diffDest = walHandler->payload + key.size() + updateDesc.size();
    updateDesc.GenerateDiff(diffDest, chainedTuple.payload);
    updateDesc.GenerateXORDiff(diffDest, chainedTuple.payload);
    walHandler.SubmitWal();

    // Update
    updateCallBack(MutableSlice(chainedTuple.payload,
                                rawVal.length() - sizeof(ChainedTuple)));
    chainedTuple.mWorkerId = cr::Worker::my().mWorkerId;
    chainedTuple.mTxId = cr::activeTX().startTS();
    chainedTuple.mCommandId = commandId;

    chainedTuple.WriteUnlock();
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

  cr::activeTX().markAsWrite();
  cr::Worker::my().mLogging.walEnsureEnoughSpace(FLAGS_page_size * 1);
  u16 payloadSize = val.size() + sizeof(ChainedTuple);

  while (true) {
    BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
    auto ret = xIter.seekToInsert(key);

    if (ret == OP_RESULT::DUPLICATE) {
      auto mutRawVal = xIter.MutableVal();
      auto& chainedTuple = *ChainedTuple::From(mutRawVal.data());
      auto writeLocked = chainedTuple.IsWriteLocked();
      auto visibleForMe =
          VisibleForMe(chainedTuple.mWorkerId, chainedTuple.mTxId);
      if (writeLocked || !visibleForMe) {
        DLOG(ERROR) << "Insert failed, writeLocked=" << writeLocked
                    << ", visibleForMe=" << visibleForMe;
        return OP_RESULT::ABORT_TX;
      }
      LOG(INFO) << "Insert failed, key is duplicated";
      return OP_RESULT::DUPLICATE;
    }

    ret = xIter.enoughSpaceInCurrentNode(key, payloadSize);
    if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
      xIter.splitForKey(key);
      continue;
    }

    // WAL
    auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WALInsert>(
        key.size() + val.size(), key, val);
    walHandler.SubmitWal();

    // insert
    BTreeVI::InsertToNode(xIter.mGuardedLeaf, key, val,
                          cr::Worker::my().mWorkerId, cr::activeTX().startTS(),
                          cr::activeTX().mTxMode, xIter.mSlotId);
    return OP_RESULT::OK;
  }
  return OP_RESULT::OTHER;
}

OP_RESULT BTreeVI::remove(Slice key) {
  DCHECK(cr::Worker::my().IsTxStarted());

  cr::activeTX().markAsWrite();
  cr::Worker::my().mLogging.walEnsureEnoughSpace(FLAGS_page_size);

  JUMPMU_TRY() {
    BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
    OP_RESULT ret = xIter.seekExact(key);
    if (ret != OP_RESULT::OK) {
      if (cr::activeTX().isOLAP() && ret == OP_RESULT::NOT_FOUND) {
        auto foundRemovedTuple =
            mGraveyard->Lookup(key, [&](Slice) {}) == OP_RESULT::OK;
        if (foundRemovedTuple) {
          JUMPMU_RETURN OP_RESULT::ABORT_TX;
        }
      }

      JUMPMU_RETURN OP_RESULT::NOT_FOUND;
    }

    auto mutRawVal = xIter.MutableVal();
    ChainedTuple& chainedTuple = *ChainedTuple::From(mutRawVal.data());
    // TODO: removing fat tuple is not supported atm
    DCHECK(chainedTuple.mFormat == TupleFormat::CHAINED);
    if (chainedTuple.IsWriteLocked() ||
        !VisibleForMe(chainedTuple.mWorkerId, chainedTuple.mTxId, true)) {
      JUMPMU_RETURN OP_RESULT::ABORT_TX;
    }
    ENSURE(!cr::activeTX().atLeastSI() || chainedTuple.mIsRemoved == false);
    if (chainedTuple.mIsRemoved) {
      JUMPMU_RETURN OP_RESULT::NOT_FOUND;
    }

    chainedTuple.WriteLock();

    DanglingPointer danglingPointer(xIter.mGuardedLeaf.mBf,
                                    xIter.mGuardedLeaf.mGuard.mVersion,
                                    xIter.mSlotId);
    u16 valSize = xIter.value().length() - sizeof(ChainedTuple);
    u16 versionSize = sizeof(RemoveVersion) + valSize + key.size();
    COMMANDID commandId = cr::Worker::my().cc.insertVersion(
        mTreeId, true, versionSize, [&](u8* versionBuf) {
          auto& removeVersion = *new (versionBuf) RemoveVersion(
              chainedTuple.mWorkerId, chainedTuple.mTxId,
              chainedTuple.mCommandId, key.size(), valSize);
          removeVersion.dangling_pointer = danglingPointer;
          std::memcpy(removeVersion.payload, key.data(), key.size());
          std::memcpy(removeVersion.payload + key.size(), chainedTuple.payload,
                      valSize);
        });

    // WAL
    auto prevWorkerId(chainedTuple.mWorkerId);
    auto prevTxId(chainedTuple.mTxId);
    auto prevCommandId(chainedTuple.mCommandId);
    auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WALRemove>(
        key.size() + valSize, key, Slice(chainedTuple.payload, valSize),
        prevWorkerId, prevTxId, prevCommandId);
    walHandler.SubmitWal();

    if (mutRawVal.length() - sizeof(ChainedTuple) > 1) {
      xIter.shorten(sizeof(ChainedTuple));
    }
    chainedTuple.mIsRemoved = true;
    chainedTuple.mWorkerId = cr::Worker::my().mWorkerId;
    chainedTuple.mTxId = cr::activeTX().startTS();
    chainedTuple.mCommandId = commandId;

    chainedTuple.WriteUnlock();
    xIter.MarkAsDirty();

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

} // namespace btree
} // namespace storage
} // namespace leanstore
