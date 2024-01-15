#include "TransactionKV.hpp"

#include "KVInterface.hpp"
#include "shared-headers/Units.hpp"
#include "storage/btree/BasicKV.hpp"
#include "storage/btree/ChainedTuple.hpp"
#include "storage/btree/Tuple.hpp"
#include "storage/btree/core/BTreeWALPayload.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

using namespace std;
using namespace leanstore::storage;
using OpCode = leanstore::OpCode;

namespace leanstore::storage::btree {

OpCode TransactionKV::Lookup(Slice key, ValCallback valCallback) {
  DCHECK(cr::Worker::my().IsTxStarted())
      << "Worker is not in a transaction"
      << ", workerId=" << cr::Worker::my().mWorkerId
      << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs;

  BTreeSharedIterator iter(*static_cast<BTreeGeneric*>(this));
  if (!iter.SeekExact(key)) {
    return OpCode::kNotFound;
  }

  auto [ret, versionsRead] = GetVisibleTuple(iter.value(), valCallback);
  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().cc_read_chains[mTreeId]++;
    WorkerCounters::MyCounters().cc_read_versions_visited[mTreeId] +=
        versionsRead;
  }

  if (cr::ActiveTx().IsOLAP() && ret == OpCode::kNotFound) {
    BTreeSharedIterator gIter(*static_cast<BTreeGeneric*>(mGraveyard));
    if (!gIter.SeekExact(key)) {
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

OpCode TransactionKV::UpdateInPlace(Slice key, MutValCallback updateCallBack,
                                    UpdateDesc& updateDesc) {
  DCHECK(cr::Worker::my().IsTxStarted());
  cr::Worker::my().mLogging.WalEnsureEnoughSpace(FLAGS_page_size);
  JUMPMU_TRY() {
    BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
    if (!xIter.SeekExact(key)) {
      // Conflict detected, the tuple to be updated by the long-running OLAP
      // transaction is removed by newer transactions, abort it.
      if (cr::ActiveTx().IsOLAP() &&
          mGraveyard->Lookup(key, [&](Slice) {}) == OpCode::kOK) {
        JUMPMU_RETURN OpCode::kAbortTx;
      }

      LOG(ERROR) << "Update failed, key not found, key=" << ToString(key)
                 << ", txMode=" << ToString(cr::ActiveTx().mTxMode);
      JUMPMU_RETURN OpCode::kNotFound;
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
      case TupleFormat::kFat: {
        auto succeed = UpdateInFatTuple(xIter, key, updateCallBack, updateDesc);
        xIter.MarkAsDirty();
        xIter.UpdateContentionStats();
        Tuple::From(mutRawVal.Data())->WriteUnlock();
        if (!succeed) {
          JUMPMU_CONTINUE;
        }
        JUMPMU_RETURN OpCode::kOK;
      }
      case TupleFormat::kChained: {
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

OpCode TransactionKV::Insert(Slice key, Slice val) {
  DCHECK(cr::Worker::my().IsTxStarted());

  cr::Worker::my().mLogging.WalEnsureEnoughSpace(FLAGS_page_size * 1);
  u16 payloadSize = val.size() + sizeof(ChainedTuple);

  while (true) {
    BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
    auto ret = xIter.seekToInsert(key);

    if (ret == OpCode::kDuplicated) {
      auto mutRawVal = xIter.MutableVal();
      auto& chainedTuple = *ChainedTuple::From(mutRawVal.Data());
      auto writeLocked = chainedTuple.IsWriteLocked();
      if (writeLocked ||
          !VisibleForMe(chainedTuple.mWorkerId, chainedTuple.mTxId)) {
        LOG(INFO) << "Conflict detected, please abort and retry"
                  << ", workerId=" << cr::Worker::my().mWorkerId
                  << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
                  << ", tupleLastWriter=" << chainedTuple.mWorkerId
                  << ", tupleLastStartTs=" << chainedTuple.mTxId
                  << ", visibleForMe="
                  << VisibleForMe(chainedTuple.mWorkerId, chainedTuple.mTxId);
        return OpCode::kAbortTx;
      }
      LOG(INFO) << "Insert failed, key is duplicated";
      return OpCode::kDuplicated;
    }

    if (!xIter.HasEnoughSpaceFor(key.size(), payloadSize)) {
      xIter.SplitForKey(key);
      continue;
    }

    // WAL
    auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WALInsert>(
        key.size() + val.size(), key, val);
    walHandler.SubmitWal();

    // insert
    TransactionKV::InsertToNode(
        xIter.mGuardedLeaf, key, val, cr::Worker::my().mWorkerId,
        cr::ActiveTx().mStartTs, cr::ActiveTx().mTxMode, xIter.mSlotId);
    return OpCode::kOK;
  }
  return OpCode::kOther;
}

OpCode TransactionKV::Remove(Slice key) {
  DCHECK(cr::Worker::my().IsTxStarted());
  cr::Worker::my().mLogging.WalEnsureEnoughSpace(FLAGS_page_size);

  JUMPMU_TRY() {
    BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
    if (!xIter.SeekExact(key)) {
      // Conflict detected, the tuple to be removed by the long-running OLAP
      // transaction is removed by newer transactions, abort it.
      if (cr::ActiveTx().IsOLAP() &&
          mGraveyard->Lookup(key, [&](Slice) {}) == OpCode::kOK) {
        JUMPMU_RETURN OpCode::kAbortTx;
      }

      JUMPMU_RETURN OpCode::kNotFound;
    }

    auto mutRawVal = xIter.MutableVal();
    auto* tuple = Tuple::From(mutRawVal.Data());

    // remove fat tuple is not supported yet
    if (tuple->mFormat == TupleFormat::kFat) {
      LOG(ERROR) << "Remove failed, fat tuple is not supported yet";
      JUMPMU_RETURN OpCode::kNotFound;
    }

    // remove the chained tuple
    auto& chainedTuple = *static_cast<ChainedTuple*>(tuple);
    if (chainedTuple.IsWriteLocked() ||
        !VisibleForMe(chainedTuple.mWorkerId, chainedTuple.mTxId)) {
      LOG(INFO) << "Conflict detected, please abort and retry"
                << ", workerId=" << cr::Worker::my().mWorkerId
                << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
                << ", tupleLastWriter=" << chainedTuple.mWorkerId
                << ", tupleLastStartTs=" << chainedTuple.mTxId
                << ", visibleForMe="
                << VisibleForMe(chainedTuple.mWorkerId, chainedTuple.mTxId);
      JUMPMU_RETURN OpCode::kAbortTx;
    }

    if (chainedTuple.mIsRemoved) {
      JUMPMU_RETURN OpCode::kNotFound;
    }

    chainedTuple.WriteLock();
    DanglingPointer danglingPointer(xIter);
    auto valSize = xIter.value().size() - sizeof(ChainedTuple);
    auto val = chainedTuple.GetValue(valSize);
    auto versionSize = sizeof(RemoveVersion) + val.size() + key.size();
    auto commandId = cr::Worker::my().cc.PutVersion(
        mTreeId, true, versionSize, [&](u8* versionBuf) {
          new (versionBuf)
              RemoveVersion(chainedTuple.mWorkerId, chainedTuple.mTxId,
                            chainedTuple.mCommandId, key, val, danglingPointer);
        });

    // WAL
    auto prevWorkerId(chainedTuple.mWorkerId);
    auto prevTxId(chainedTuple.mTxId);
    auto prevCommandId(chainedTuple.mCommandId);
    auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WALTxRemove>(
        key.size() + val.size(), key, val, prevWorkerId, prevTxId,
        prevCommandId);
    walHandler.SubmitWal();

    // remove the tuple in the btree
    if (mutRawVal.Size() > sizeof(ChainedTuple)) {
      xIter.shorten(sizeof(ChainedTuple));
    }

    // mark as removed
    chainedTuple.mIsRemoved = true;
    chainedTuple.mWorkerId = cr::Worker::my().mWorkerId;
    chainedTuple.mTxId = cr::ActiveTx().mStartTs;
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

OpCode TransactionKV::ScanDesc(Slice startKey, ScanCallback callback) {
  DCHECK(cr::Worker::my().IsTxStarted());

  if (cr::ActiveTx().IsOLAP()) {
    TODOException();
    return OpCode::kAbortTx;
  }
  return scan<false>(startKey, callback);
}

OpCode TransactionKV::ScanAsc(Slice startKey, ScanCallback callback) {
  DCHECK(cr::Worker::my().IsTxStarted());

  if (cr::ActiveTx().IsOLAP()) {
    return scanOLAP(startKey, callback);
  }
  return scan<true>(startKey, callback);
}

void TransactionKV::undo(const u8* walPayloadPtr,
                         const u64 txId [[maybe_unused]]) {
  auto& walPayload = *reinterpret_cast<const WALPayload*>(walPayloadPtr);
  switch (walPayload.mType) {
  case WALPayload::TYPE::WALInsert: {
    return undoLastInsert(static_cast<const WALInsert*>(&walPayload));
  }
  case WALPayload::TYPE::WALTxUpdate: {
    return undoLastUpdate(static_cast<const WALTxUpdate*>(&walPayload));
  }
  case WALPayload::TYPE::WALTxRemove: {
    return undoLastRemove(static_cast<const WALTxRemove*>(&walPayload));
  }
  default: {
    LOG(ERROR) << "Unknown wal payload type: " << (u64)walPayload.mType;
  }
  }
}

void TransactionKV::undoLastInsert(const WALInsert* walInsert) {
  // Assuming no insert after remove
  auto key = walInsert->GetKey();
  for (int retry = 0; true; retry++) {
    JUMPMU_TRY() {
      BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
      auto succeed = xIter.SeekExact(key);
      DCHECK(succeed) << "Cannot find the inserted key in btree"
                      << ", workerId=" << cr::Worker::my().mWorkerId
                      << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
                      << ", key=" << ToString(key);

      // TODO(jian.z): write compensation wal entry
      auto ret = xIter.removeCurrent();
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

void TransactionKV::undoLastUpdate(const WALTxUpdate* walUpdate) {
  auto key = walUpdate->GetKey();
  for (int retry = 0; true; retry++) {
    JUMPMU_TRY() {
      BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
      auto succeed = xIter.SeekExact(key);
      DCHECK(succeed) << "Cannot find the updated key in btree"
                      << ", workerId=" << cr::Worker::my().mWorkerId
                      << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
                      << ", key=" << ToString(key);

      auto mutRawVal = xIter.MutableVal();
      auto& tuple = *Tuple::From(mutRawVal.Data());
      DCHECK(!tuple.IsWriteLocked())
          << "Tuple is write locked"
          << ", workerId=" << cr::Worker::my().mWorkerId
          << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
          << ", key=" << ToString(key);

      if (tuple.mFormat == TupleFormat::kFat) {
        FatTuple::From(mutRawVal.Data())->UndoLastUpdate();
      } else {
        auto& chainedTuple = *ChainedTuple::From(mutRawVal.Data());
        chainedTuple.mWorkerId = walUpdate->mPrevWorkerId;
        chainedTuple.mTxId = walUpdate->mPrevTxId;
        chainedTuple.mCommandId = walUpdate->mPrevCommandId;
        auto& updateDesc = *walUpdate->GetUpdateDesc();
        auto* xorData = walUpdate->GetDeltaPtr();

        // 1. copy the new value to buffer
        auto deltaSize = walUpdate->GetDeltaSize();
        u8 buff[deltaSize];
        std::memcpy(buff, xorData, deltaSize);

        // 2. calculate the old value based on xor result and old value
        BasicKV::XorToBuffer(updateDesc, chainedTuple.mPayload, buff);

        // 3. replace new value with old value
        BasicKV::CopyToValue(updateDesc, buff, chainedTuple.mPayload);
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

void TransactionKV::undoLastRemove(const WALTxRemove* walRemove) {
  Slice removedKey = walRemove->RemovedKey();
  for (int retry = 0; true; retry++) {
    JUMPMU_TRY() {
      BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
      auto succeed = xIter.SeekExact(removedKey);
      DCHECK(succeed) << "Cannot find the tombstone of removed key"
                      << ", workerId=" << cr::Worker::my().mWorkerId
                      << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
                      << ", removedKey=" << ToString(removedKey);

      // resize the current slot to store the removed tuple
      auto chainedTupleSize = walRemove->mValSize + sizeof(ChainedTuple);
      auto curRawVal = xIter.value();
      if (curRawVal.size() < chainedTupleSize) {
        auto succeed = xIter.extendPayload(chainedTupleSize);
        DCHECK(succeed) << "Failed to extend btree node slot to store the "
                           "recovered chained tuple"
                        << ", workerId" << cr::Worker::my().mWorkerId
                        << ", startTs=" << cr::Worker::my().mActiveTx.mStartTs
                        << ", removedKey=" << ToString(removedKey)
                        << ", curRawValSize=" << curRawVal.size()
                        << ", chainedTupleSize=" << chainedTupleSize;
      } else if (curRawVal.size() > chainedTupleSize) {
        xIter.shorten(chainedTupleSize);
      }

      auto curMutRawVal = xIter.MutableVal();
      new (curMutRawVal.Data())
          ChainedTuple(walRemove->mPrevWorkerId, walRemove->mPrevTxId,
                       walRemove->mPrevCommandId, walRemove->RemovedVal());

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

bool TransactionKV::UpdateInFatTuple(BTreeExclusiveIterator& xIter, Slice key,
                                     MutValCallback updateCallBack,
                                     UpdateDesc& updateDesc) {
  utils::Timer timer(CRCounters::MyCounters().cc_ms_fat_tuple);
  while (true) {
    auto* fatTuple = reinterpret_cast<FatTuple*>(xIter.MutableVal().Data());
    DCHECK(fatTuple->IsWriteLocked())
        << "Tuple should be write locked before update";

    if (!fatTuple->HasSpaceFor(updateDesc)) {
      fatTuple->GarbageCollection();
      if (fatTuple->HasSpaceFor(updateDesc)) {
        continue;
      }

      // Not enough space to store the fat tuple, convert to chained
      auto chainedTupleSize = fatTuple->mValSize + sizeof(ChainedTuple);
      DCHECK(chainedTupleSize < xIter.value().length());
      fatTuple->ConvertToChained(xIter.mBTree.mTreeId);
      xIter.shorten(chainedTupleSize);
      return false;
    }

    auto performUpdate = [&]() {
      fatTuple->Append(updateDesc);
      fatTuple->mWorkerId = cr::Worker::my().mWorkerId;
      fatTuple->mTxId = cr::ActiveTx().mStartTs;
      fatTuple->mCommandId = cr::Worker::my().mCommandId++;
      updateCallBack(fatTuple->GetMutableValue());
      DCHECK(fatTuple->mPayloadCapacity >= fatTuple->mPayloadSize);
    };

    if (!xIter.mBTree.mConfig.mEnableWal) {
      performUpdate();
      return true;
    }

    auto sizeOfDescAndDelta = updateDesc.SizeWithDelta();
    auto prevWorkerId = fatTuple->mWorkerId;
    auto prevTxId = fatTuple->mTxId;
    auto prevCommandId = fatTuple->mCommandId;
    auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WALTxUpdate>(
        key.size() + sizeOfDescAndDelta, key, updateDesc, sizeOfDescAndDelta,
        prevWorkerId, prevTxId, prevCommandId);
    auto* walBuf = walHandler->GetDeltaPtr();

    // 1. copy old value to wal buffer
    BasicKV::CopyToBuffer(updateDesc, fatTuple->GetValPtr(), walBuf);

    // 2. update the value in-place
    performUpdate();

    // 3. xor with the updated new value and store to wal buffer
    BasicKV::XorToBuffer(updateDesc, fatTuple->GetValPtr(), walBuf);
    walHandler.SubmitWal();
    return true;
  }
}

} // namespace leanstore::storage::btree
