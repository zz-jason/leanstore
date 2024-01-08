#include "BTreeVI.hpp"

#include "Units.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "utils/Defer.hpp"
#include "utils/JsonUtil.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <signal.h>

using namespace std;
using namespace leanstore::storage;
using OpCode = leanstore::OpCode;

// Assumptions made in this implementation:
// 1. We don't insert an already removed key
// 2. Secondary Versions contain delta
//
// Keep in mind that garbage collection may leave pages completely empty
// Missing points: FatTuple::remove, garbage leaves can escape from us

namespace leanstore {
namespace storage {
namespace btree {

OpCode BTreeVI::Lookup(Slice key, ValCallback valCallback) {
  DCHECK(cr::Worker::my().IsTxStarted());

  OpCode ret = lookupOptimistic(key, valCallback);
  if (ret == OpCode::kOther) {
    return lookupPessimistic(key, valCallback);
  }
  return ret;
}

OpCode BTreeVI::lookupPessimistic(Slice key, ValCallback valCallback) {
  JUMPMU_TRY() {
    BTreeSharedIterator iter(*static_cast<BTreeGeneric*>(this));
    auto ret = iter.seekExact(key);
    if (ret != OpCode::kOK) {
      JUMPMU_RETURN OpCode::kNotFound;
    }

    auto versionsRead(0);
    std::tie(ret, versionsRead) = GetVisibleTuple(iter.value(), valCallback);
    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().cc_read_chains[mTreeId]++;
      WorkerCounters::myCounters().cc_read_versions_visited[mTreeId] +=
          versionsRead;
    }

    if (cr::activeTX().isOLAP() && ret == OpCode::kNotFound) {
      BTreeSharedIterator gIter(*static_cast<BTreeGeneric*>(mGraveyard));
      ret = gIter.seekExact(key);
      if (ret != OpCode::kOK) {
        JUMPMU_RETURN OpCode::kNotFound;
      }
      std::tie(ret, versionsRead) = GetVisibleTuple(iter.value(), valCallback);
      COUNTERS_BLOCK() {
        WorkerCounters::myCounters().cc_read_chains[mTreeId]++;
        WorkerCounters::myCounters().cc_read_versions_visited[mTreeId] +=
            versionsRead;
      }
    }

    if (ret != OpCode::kOK) {
      JUMPMU_RETURN OpCode::kNotFound;
    }
    JUMPMU_RETURN ret;
  }
  JUMPMU_CATCH() {
    LOG(ERROR) << "lookupPessimistic failed";
  }

  return OpCode::kNotFound;
}

OpCode BTreeVI::lookupOptimistic(Slice key, ValCallback valCallback) {
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
        JUMPMU_RETURN OpCode::kNotFound;
      }

      auto rawVal = guardedLeaf->ValData(slotId);
      auto tuple = *Tuple::From(rawVal);
      guardedLeaf.JumpIfModifiedByOthers();

      // Return NOT_FOUND if the tuple is not visible
      if (!VisibleForMe(tuple.mWorkerId, tuple.mTxId, false)) {
        JUMPMU_RETURN OpCode::kNotFound;
      }

      u32 offset = 0;
      switch (tuple.mFormat) {
      case TupleFormat::CHAINED: {
        const auto chainedTuple = ChainedTuple::From(rawVal);
        if (chainedTuple->mIsRemoved) {
          JUMPMU_RETURN OpCode::kNotFound;
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

      JUMPMU_RETURN OpCode::kOK;
    }
    JUMPMU_CATCH() {
    }
  }

  // lookup failed, fallback to lookupPessimistic
  LOG(INFO) << "lookupOptimistic failed " << maxAttempts
            << " times, fallback to lookupPessimistic";
  return OpCode::kOther;
}

OpCode BTreeVI::updateSameSizeInPlace(Slice key, MutValCallback updateCallBack,
                                      UpdateDesc& updateDesc) {
  DCHECK(cr::Worker::my().IsTxStarted());
  cr::Worker::my().mLogging.walEnsureEnoughSpace(FLAGS_page_size);

  BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
  auto ret = xIter.seekExact(key);
  if (ret != OpCode::kOK) {
    if (cr::activeTX().isOLAP() && ret == OpCode::kNotFound) {
      const bool foundRemovedTuple =
          mGraveyard->Lookup(key, [&](Slice) {}) == OpCode::kOK;

      // The tuple to be updated is removed, abort the transaction.
      if (foundRemovedTuple) {
        return OpCode::kAbortTx;
      }
    }

    LOG(ERROR) << "Update failed, key not found, key=" << ToString(key)
               << ", txMode=" << ToString(cr::activeTX().mTxMode)
               << ", seek ret=" << ToString(ret);
    return ret;
  }

  // Record is found
  while (true) {
    auto rawVal = xIter.MutableVal();
    auto& tuple = *Tuple::From(rawVal.data());
    auto visibleForMe = VisibleForMe(tuple.mWorkerId, tuple.mTxId, true);
    if (tuple.IsWriteLocked() || !visibleForMe) {
      LOG(ERROR) << "Update failed, primary tuple is write locked or not "
                    "visible for me"
                 << ", key=" << ToString(key)
                 << ", writeLocked=" << tuple.IsWriteLocked()
                 << ", visibleForMe=" << visibleForMe;
      return OpCode::kAbortTx;
    }

    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().cc_update_chains[mTreeId]++;
    }

    // write lock the tuple
    tuple.WriteLock();

    switch (tuple.mFormat) {
    case TupleFormat::FAT: {
      auto& fatTuple = *FatTuple::From(rawVal.data());
      auto succeed = fatTuple.update(xIter, key, updateCallBack, updateDesc);
      xIter.MarkAsDirty();
      xIter.UpdateContentionStats();
      Tuple::From(rawVal.data())->WriteUnlock();
      if (!succeed) {
        continue;
      }
      return OpCode::kOK;
    }
    case TupleFormat::CHAINED: {
      auto& chainedTuple = *ChainedTuple::From(rawVal.data());
      chainedTuple.UpdateStats();

      // convert to fat tuple if it's frequently updated by me and other workers
      if (FLAGS_enable_fat_tuple && chainedTuple.ShouldConvertToFatTuple()) {
        COUNTERS_BLOCK() {
          WorkerCounters::myCounters().cc_fat_tuple_triggered[mTreeId]++;
        }
        chainedTuple.mTotalUpdates = 0;
        auto succeed = Tuple::ToFat(xIter);
        if (succeed) {
          xIter.mGuardedLeaf->mHasGarbage = true;
          COUNTERS_BLOCK() {
            WorkerCounters::myCounters().cc_fat_tuple_convert[mTreeId]++;
          }
        }
        Tuple::From(rawVal.data())->WriteUnlock();
        continue;
      }

      // update the chained tuple
      chainedTuple.Update(xIter, key, updateCallBack, updateDesc);
      return OpCode::kOK;
    }
    default: {
      LOG(ERROR) << "Unhandled tuple format: "
                 << TupleFormatUtil::ToString(tuple.mFormat);
    }
    }
  }
  return OpCode::kOther;
}

OpCode BTreeVI::insert(Slice key, Slice val) {
  DCHECK(cr::Worker::my().IsTxStarted());

  cr::Worker::my().mLogging.walEnsureEnoughSpace(FLAGS_page_size * 1);
  u16 payloadSize = val.size() + sizeof(ChainedTuple);

  while (true) {
    BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
    auto ret = xIter.seekToInsert(key);

    if (ret == OpCode::kDuplicated) {
      auto mutRawVal = xIter.MutableVal();
      auto& chainedTuple = *ChainedTuple::From(mutRawVal.data());
      auto writeLocked = chainedTuple.IsWriteLocked();
      auto visibleForMe =
          VisibleForMe(chainedTuple.mWorkerId, chainedTuple.mTxId);
      if (writeLocked || !visibleForMe) {
        DLOG(ERROR) << "Insert failed, writeLocked=" << writeLocked
                    << ", visibleForMe=" << visibleForMe;
        return OpCode::kAbortTx;
      }
      LOG(INFO) << "Insert failed, key is duplicated";
      return OpCode::kDuplicated;
    }

    ret = xIter.enoughSpaceInCurrentNode(key, payloadSize);
    if (ret == OpCode::kSpaceNotEnough) {
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
    return OpCode::kOK;
  }
  return OpCode::kOther;
}

OpCode BTreeVI::remove(Slice key) {
  DCHECK(cr::Worker::my().IsTxStarted());
  cr::Worker::my().mLogging.walEnsureEnoughSpace(FLAGS_page_size);

  JUMPMU_TRY() {
    BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
    OpCode ret = xIter.seekExact(key);
    if (ret != OpCode::kOK) {
      if (cr::activeTX().isOLAP() && ret == OpCode::kNotFound) {
        auto foundRemovedTuple =
            mGraveyard->Lookup(key, [&](Slice) {}) == OpCode::kOK;
        if (foundRemovedTuple) {
          JUMPMU_RETURN OpCode::kAbortTx;
        }
      }

      JUMPMU_RETURN OpCode::kNotFound;
    }

    auto mutRawVal = xIter.MutableVal();
    ChainedTuple& chainedTuple = *ChainedTuple::From(mutRawVal.data());
    // TODO: removing fat tuple is not supported atm
    DCHECK(chainedTuple.mFormat == TupleFormat::CHAINED);
    if (chainedTuple.IsWriteLocked() ||
        !VisibleForMe(chainedTuple.mWorkerId, chainedTuple.mTxId, true)) {
      JUMPMU_RETURN OpCode::kAbortTx;
    }
    ENSURE(!cr::activeTX().atLeastSI() || chainedTuple.mIsRemoved == false);
    if (chainedTuple.mIsRemoved) {
      JUMPMU_RETURN OpCode::kNotFound;
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

    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BTreeVI::ScanDesc(Slice startKey, ScanCallback callback) {
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
    return OpCode::kAbortTx;
  }
  return scan<false>(startKey, callback);
}

OpCode BTreeVI::ScanAsc(Slice startKey, ScanCallback callback) {
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
