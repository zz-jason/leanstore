#include "BTreeVI.hpp"

#include "shared-headers/Units.hpp"
#include "storage/btree/BTreeLL.hpp"
#include "storage/btree/core/BTreeWALPayload.hpp"

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
  DCHECK(cr::Worker::my().IsTxStarted())
      << "Worker is not in a transaction"
      << ", workerId=" << cr::Worker::my().mWorkerId
      << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs;

  BTreeSharedIterator iter(*static_cast<BTreeGeneric*>(this));
  if (iter.seekExact(key) != OpCode::kOK) {
    return OpCode::kNotFound;
  }

  auto [ret, versionsRead] = GetVisibleTuple(iter.value(), valCallback);
  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().cc_read_chains[mTreeId]++;
    WorkerCounters::MyCounters().cc_read_versions_visited[mTreeId] +=
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
      WorkerCounters::MyCounters().cc_read_chains[mTreeId]++;
      WorkerCounters::MyCounters().cc_read_versions_visited[mTreeId] +=
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
        auto foundRemovedTuple =
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
      auto mutRawVal = xIter.MutableVal();
      auto& tuple = *Tuple::From(mutRawVal.Data());
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
        WorkerCounters::MyCounters().cc_update_chains[mTreeId]++;
      }

      // write lock the tuple
      tuple.WriteLock();

      switch (tuple.mFormat) {
      case TupleFormat::FAT: {
        auto& fatTuple = *FatTuple::From(mutRawVal.Data());
        auto succeed = fatTuple.update(xIter, key, updateCallBack, updateDesc);
        xIter.MarkAsDirty();
        xIter.UpdateContentionStats();
        Tuple::From(mutRawVal.Data())->WriteUnlock();
        if (!succeed) {
          JUMPMU_CONTINUE;
        }
        JUMPMU_RETURN OpCode::kOK;
      }
      case TupleFormat::CHAINED: {
        auto& chainedTuple = *ChainedTuple::From(mutRawVal.Data());
        chainedTuple.UpdateStats();

        // convert to fat tuple if it's frequently updated by me and other
        // workers
        if (FLAGS_enable_fat_tuple && chainedTuple.ShouldConvertToFatTuple()) {
          COUNTERS_BLOCK() {
            WorkerCounters::MyCounters().cc_fat_tuple_triggered[mTreeId]++;
          }
          chainedTuple.mTotalUpdates = 0;
          auto succeed = Tuple::ToFat(xIter);
          if (succeed) {
            xIter.mGuardedLeaf->mHasGarbage = true;
            COUNTERS_BLOCK() {
              WorkerCounters::MyCounters().cc_fat_tuple_convert[mTreeId]++;
            }
          }
          Tuple::From(mutRawVal.Data())->WriteUnlock();
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
      auto& chainedTuple = *ChainedTuple::From(mutRawVal.Data());
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
    ChainedTuple& chainedTuple = *ChainedTuple::From(mutRawVal.Data());
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

    if (mutRawVal.Size() - sizeof(ChainedTuple) > 1) {
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

void BTreeVI::undo(const u8* walPayloadPtr, const u64 txId [[maybe_unused]]) {
  auto& walPayload = *reinterpret_cast<const WALPayload*>(walPayloadPtr);
  switch (walPayload.type) {
  case WALPayload::TYPE::WALInsert: {
    return undoLastInsert(static_cast<const WALInsert*>(&walPayload));
  }
  case WALPayload::TYPE::WALUpdate: {
    return undoLastUpdate(static_cast<const WALUpdateSSIP*>(&walPayload));
  }
  case WALPayload::TYPE::WALRemove: {
    return undoLastRemove(static_cast<const WALRemove*>(&walPayload));
  }
  default: {
    break;
  }
  }
}

void BTreeVI::undoLastInsert(const WALInsert* walInsert) {
  // Assuming no insert after remove
  Slice key(walInsert->payload, walInsert->mKeySize);
  for (int retry = 0; true; retry++) {
    JUMPMU_TRY() {
      BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
      OpCode ret = xIter.seekExact(key);
      DCHECK(ret == OpCode::kOK)
          << "Cannot find the inserted key in btree"
          << ", workerId=" << cr::Worker::my().mWorkerId
          << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
          << ", key=" << ToString(key) << ", ret=" << ToString(ret);

      // TODO(jian.z): write compensation wal entry
      ret = xIter.removeCurrent();
      DCHECK(ret == OpCode::kOK)
          << "Failed to remove the inserted key in btree"
          << ", workerId=" << cr::Worker::my().mWorkerId
          << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
          << ", key=" << ToString(key) << ", ret=" << ToString(ret);

      xIter.MarkAsDirty();
      xIter.mergeIfNeeded();

      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
      LOG_IF(WARNING, retry % 100 == 0)
          << "Undo insert failed"
          << ", workerId=" << cr::Worker::my().mWorkerId
          << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
          << ", retry=" << retry;
    }
  }
}

void BTreeVI::undoLastUpdate(const WALUpdateSSIP* walUpdate) {
  Slice key(walUpdate->payload, walUpdate->mKeySize);
  for (int retry = 0; true; retry++) {
    JUMPMU_TRY() {
      BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
      OpCode ret = xIter.seekExact(key);
      DCHECK(ret == OpCode::kOK)
          << "Cannot find the updated key in btree"
          << ", workerId=" << cr::Worker::my().mWorkerId
          << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
          << ", key=" << ToString(key) << ", ret=" << ToString(ret);

      auto mutRawVal = xIter.MutableVal();
      auto& tuple = *Tuple::From(mutRawVal.Data());
      DCHECK(!tuple.IsWriteLocked())
          << "Tuple is write locked"
          << ", workerId=" << cr::Worker::my().mWorkerId
          << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
          << ", key=" << ToString(key);

      if (tuple.mFormat == TupleFormat::FAT) {
        FatTuple::From(mutRawVal.Data())->undoLastUpdate();
      } else {
        auto& chainedTuple = *ChainedTuple::From(mutRawVal.Data());
        chainedTuple.mWorkerId = walUpdate->mPrevWorkerId;
        chainedTuple.mTxId = walUpdate->mPrevTxId;
        chainedTuple.mCommandId = walUpdate->mPrevCommandId;
        auto& updateDesc =
            *UpdateDesc::From(walUpdate->payload + walUpdate->mKeySize);
        auto* xorData =
            walUpdate->payload + walUpdate->mKeySize + updateDesc.Size();

        // 1. copy the new value to buffer
        const auto buffSize = updateDesc.NumBytes4WAL() - updateDesc.Size();
        u8 buff[buffSize];
        std::memcpy(buff, xorData, buffSize);

        // 2. calculate the old value based on xor result and old value
        BTreeLL::XorToBuffer(updateDesc, chainedTuple.payload, buff);

        // 3. replace new value with old value
        BTreeLL::CopyToValue(updateDesc, buff, chainedTuple.payload);
      }
      xIter.MarkAsDirty();
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
      LOG_IF(WARNING, retry % 100 == 0)
          << "Undo update failed"
          << ", workerId=" << cr::Worker::my().mWorkerId
          << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
          << ", retry=" << retry;
    }
  }
}

void BTreeVI::undoLastRemove(const WALRemove* walRemove) {
  Slice key(walRemove->payload, walRemove->mKeySize);
  for (int retry = 0; true; retry++) {

    JUMPMU_TRY() {
      BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
      OpCode ret = xIter.seekExact(key);
      DCHECK(ret == OpCode::kOK)
          << "Cannot find the tombstone of removed key in btree"
          << ", workerId=" << cr::Worker::my().mWorkerId
          << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
          << ", key=" << ToString(key) << ", ret=" << ToString(ret);

      // resize the current slot to store the removed tuple
      auto removedValSize = walRemove->mValSize + sizeof(ChainedTuple);
      auto curRawVal = xIter.value();
      if (curRawVal.size() < removedValSize) {
        auto succeed = xIter.extendPayload(removedValSize);
        DCHECK(succeed)
            << "Failed to extend btree node slot to store the removed tuple"
            << ", workerId" << cr::Worker::my().mWorkerId
            << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
            << ", key=" << ToString(key)
            << ", curRawValSize=" << curRawVal.size()
            << ", removedValSize=" << removedValSize;
      } else if (curRawVal.size() > removedValSize) {
        xIter.shorten(removedValSize);
      }

      auto curMutRawVal = xIter.MutableVal();
      auto& chainedTuple = *new (curMutRawVal.Data()) ChainedTuple(
          walRemove->mPrevWorkerId, walRemove->mPrevTxId,
          Slice(walRemove->payload + walRemove->mKeySize, walRemove->mValSize));
      chainedTuple.mCommandId = walRemove->mPrevCommandId;

      xIter.MarkAsDirty();
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
      LOG_IF(WARNING, retry % 100 == 0)
          << "Undo remove failed"
          << ", workerId=" << cr::Worker::my().mWorkerId
          << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
          << ", retry=" << retry;
    }
  }
}

} // namespace btree
} // namespace storage
} // namespace leanstore
