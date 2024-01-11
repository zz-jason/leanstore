#include "BTreeVI.hpp"

#include "shared-headers/Units.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

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
  BTreeSharedIterator iter(*static_cast<BTreeGeneric*>(this));
  if (iter.seekExact(key) != OpCode::kOK) {
    return OpCode::kNotFound;
  }

  auto [ret, versionsRead] = GetVisibleTuple(iter.value(), valCallback);
  COUNTERS_BLOCK() {
    WorkerCounters::myCounters().cc_read_chains[mTreeId]++;
    WorkerCounters::myCounters().cc_read_versions_visited[mTreeId] +=
        versionsRead;
  }

  if (cr::activeTX().IsOLAP() && ret == OpCode::kNotFound) {
    BTreeSharedIterator gIter(*static_cast<BTreeGeneric*>(mGraveyard));
    ret = gIter.seekExact(key);
    if (ret != OpCode::kOK) {
      return OpCode::kNotFound;
    }
    std::tie(ret, versionsRead) = GetVisibleTuple(gIter.value(), valCallback);
    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().cc_read_chains[mTreeId]++;
      WorkerCounters::myCounters().cc_read_versions_visited[mTreeId] +=
          versionsRead;
    }
  }

  return ret;
}

OpCode BTreeVI::updateSameSizeInPlace(Slice key, MutValCallback updateCallBack,
                                      UpdateDesc& updateDesc) {
  DCHECK(cr::Worker::my().IsTxStarted());
  cr::Worker::my().mLogging.walEnsureEnoughSpace(FLAGS_page_size);
  JUMPMU_TRY() {

    BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
    auto ret = xIter.seekExact(key);
    if (ret != OpCode::kOK) {
      if (cr::activeTX().IsOLAP() && ret == OpCode::kNotFound) {
        const bool foundRemovedTuple =
            mGraveyard->Lookup(key, [&](Slice) {}) == OpCode::kOK;

        // The tuple to be updated is removed, abort the transaction.
        if (foundRemovedTuple) {
          JUMPMU_RETURN OpCode::kAbortTx;
        }
      }

      LOG(ERROR) << "Update failed, key not found, key=" << ToString(key)
                 << ", txMode=" << ToString(cr::activeTX().mTxMode)
                 << ", seek ret=" << ToString(ret);
      JUMPMU_RETURN ret;
    }

    // Record is found
    while (true) {
      auto rawVal = xIter.MutableVal();
      auto& tuple = *Tuple::From(rawVal.data());
      auto visibleForMe = VisibleForMe(tuple.mWorkerId, tuple.mTxId);
      if (tuple.IsWriteLocked() || !visibleForMe) {
        LOG(ERROR) << "Update failed, primary tuple is write locked or not "
                      "visible for me"
                   << ", key=" << ToString(key)
                   << ", writeLocked=" << tuple.IsWriteLocked()
                   << ", visibleForMe=" << visibleForMe;
        JUMPMU_RETURN OpCode::kAbortTx;
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
          JUMPMU_CONTINUE;
        }
        JUMPMU_RETURN OpCode::kOK;
      }
      case TupleFormat::CHAINED: {
        auto& chainedTuple = *ChainedTuple::From(rawVal.data());
        chainedTuple.UpdateStats();

        // convert to fat tuple if it's frequently updated by me and other
        // workers
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
          JUMPMU_CONTINUE;
        }

        // update the chained tuple
        chainedTuple.Update(xIter, key, updateCallBack, updateDesc);
        JUMPMU_RETURN OpCode::kOK;
      }
      default: {
        LOG(ERROR) << "Unhandled tuple format: "
                   << TupleFormatUtil::ToString(tuple.mFormat);
      }
      }
    }
  }
  JUMPMU_CATCH() {
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
        LOG(INFO) << "Conflict detected, please abort and retry"
                  << ", workerId=" << cr::Worker::my().mWorkerId
                  << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
                  << ", tupleLastWriter=" << chainedTuple.mWorkerId
                  << ", tupleLastStartTs=" << chainedTuple.mTxId
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
                          cr::Worker::my().mWorkerId, cr::activeTX().mStartTs,
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
      if (cr::activeTX().IsOLAP() && ret == OpCode::kNotFound) {
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
        !VisibleForMe(chainedTuple.mWorkerId, chainedTuple.mTxId)) {
      JUMPMU_RETURN OpCode::kAbortTx;
    }
    ENSURE(!cr::activeTX().AtLeastSI() || chainedTuple.mIsRemoved == false);
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
    chainedTuple.mTxId = cr::activeTX().mStartTs;
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
  DCHECK(cr::Worker::my().IsTxStarted());

  if (cr::activeTX().IsOLAP()) {
    TODOException();
    return OpCode::kAbortTx;
  }
  return scan<false>(startKey, callback);
}

OpCode BTreeVI::ScanAsc(Slice startKey, ScanCallback callback) {
  DCHECK(cr::Worker::my().IsTxStarted());

  if (cr::activeTX().IsOLAP()) {
    return scanOLAP(startKey, callback);
  }
  return scan<true>(startKey, callback);
}

} // namespace btree
} // namespace storage
} // namespace leanstore
