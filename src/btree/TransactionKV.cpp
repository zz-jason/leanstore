#include "leanstore/btree/TransactionKV.hpp"

#include "btree/core/BTreeWalPayload.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "leanstore/btree/BasicKV.hpp"
#include "leanstore/btree/ChainedTuple.hpp"
#include "leanstore/btree/Tuple.hpp"
#include "leanstore/btree/core/BTreeGeneric.hpp"
#include "leanstore/btree/core/PessimisticSharedIterator.hpp"
#include "leanstore/concurrency/WorkerContext.hpp"
#include "leanstore/sync/HybridGuard.hpp"
#include "leanstore/utils/Defer.hpp"
#include "leanstore/utils/Error.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Result.hpp"
#include "leanstore/utils/UserThread.hpp"

#include <cstring>
#include <format>
#include <string_view>

namespace leanstore::storage::btree {

Result<TransactionKV*> TransactionKV::Create(leanstore::LeanStore* store,
                                             const std::string& treeName, BTreeConfig config,
                                             BasicKV* graveyard) {
  auto [treePtr, treeId] = store->mTreeRegistry->CreateTree(treeName, [&]() {
    return std::unique_ptr<BufferManagedTree>(static_cast<BufferManagedTree*>(new TransactionKV()));
  });

  if (treePtr == nullptr) {
    return std::unexpected(utils::Error::General(std::format(
        "Failed to create TransactionKV, treeName has been taken, treeName={}", treeName)));
  }

  auto* tree = DownCast<TransactionKV*>(treePtr);
  tree->Init(store, treeId, std::move(config), graveyard);

  return tree;
}

void TransactionKV::Init(leanstore::LeanStore* store, TREEID treeId, BTreeConfig config,
                         BasicKV* graveyard) {
  this->mGraveyard = graveyard;
  BasicKV::Init(store, treeId, std::move(config));
}

OpCode TransactionKV::lookupOptimistic(Slice key, ValCallback valCallback) {
  JUMPMU_TRY() {
    GuardedBufferFrame<BTreeNode> guardedLeaf;
    FindLeafCanJump(key, guardedLeaf, LatchMode::kOptimisticOrJump);
    auto slotId = guardedLeaf->LowerBound<true>(key);
    if (slotId != -1) {
      auto [ret, versionsRead] = getVisibleTuple(guardedLeaf->Value(slotId), valCallback);
      guardedLeaf.JumpIfModifiedByOthers();
      JUMPMU_RETURN ret;
    }

    guardedLeaf.JumpIfModifiedByOthers();
    JUMPMU_RETURN OpCode::kNotFound;
  }
  JUMPMU_CATCH() {
  }

  // lock optimistically failed, return kOther to retry
  return OpCode::kOther;
}

OpCode TransactionKV::Lookup(Slice key, ValCallback valCallback) {
  LS_DCHECK(cr::WorkerContext::My().IsTxStarted(),
            "WorkerContext is not in a transaction, workerId={}, startTs={}",
            cr::WorkerContext::My().mWorkerId, cr::WorkerContext::My().mActiveTx.mStartTs);
  auto lookupInGraveyard = [&]() {
    auto gIter = mGraveyard->GetIterator();
    if (gIter.SeekToEqual(key); !gIter.Valid()) {
      return OpCode::kNotFound;
    }
    auto [ret, versionsRead] = getVisibleTuple(gIter.Val(), valCallback);
    return ret;
  };

  auto optimisticRet = lookupOptimistic(key, valCallback);
  if (optimisticRet == OpCode::kOK) {
    return OpCode::kOK;
  }
  if (optimisticRet == OpCode::kNotFound) {
    // In a lookup-after-remove(other worker) scenario, the tuple may be garbage
    // collected and moved to the graveyard, check the graveyard for the key.
    return cr::ActiveTx().IsLongRunning() ? lookupInGraveyard() : OpCode::kNotFound;
  }

  // lookup pessimistically
  auto iter = GetIterator();
  if (iter.SeekToEqual(key); !iter.Valid()) {
    // In a lookup-after-remove(other worker) scenario, the tuple may be garbage
    // collected and moved to the graveyard, check the graveyard for the key.
    return cr::ActiveTx().IsLongRunning() ? lookupInGraveyard() : OpCode::kNotFound;
  }

  auto [ret, versionsRead] = getVisibleTuple(iter.Val(), valCallback);
  if (cr::ActiveTx().IsLongRunning() && ret == OpCode::kNotFound) {
    ret = lookupInGraveyard();
  }
  return ret;
}

OpCode TransactionKV::UpdatePartial(Slice key, MutValCallback updateCallBack,
                                    UpdateDesc& updateDesc) {
  LS_DCHECK(cr::WorkerContext::My().IsTxStarted());
  JUMPMU_TRY() {
    auto xIter = GetExclusiveIterator();
    if (xIter.SeekToEqual(key); !xIter.Valid()) {
      // Conflict detected, the tuple to be updated by the long-running
      // transaction is removed by newer transactions, abort it.
      if (cr::ActiveTx().IsLongRunning() && mGraveyard->Lookup(key, [&](Slice) {}) == OpCode::kOK) {
        JUMPMU_RETURN OpCode::kAbortTx;
      }
      Log::Error("Update failed, key not found, key={}, txMode={}", key.ToString(),
                 ToString(cr::ActiveTx().mTxMode));
      JUMPMU_RETURN OpCode::kNotFound;
    }

    // Record is found
    while (true) {
      auto mutRawVal = xIter.MutableVal();
      auto& tuple = *Tuple::From(mutRawVal.Data());
      auto visibleForMe = cr::WorkerContext::My().mCc.VisibleForMe(tuple.mWorkerId, tuple.mTxId);
      if (tuple.IsWriteLocked() || !visibleForMe) {
        // conflict detected, the tuple is write locked by other worker or not
        // visible for me
        JUMPMU_RETURN OpCode::kAbortTx;
      }

      // write lock the tuple
      tuple.WriteLock();
      SCOPED_DEFER({
        LS_DCHECK(!Tuple::From(mutRawVal.Data())->IsWriteLocked(),
                  "Tuple should be write unlocked after update");
      });

      switch (tuple.mFormat) {
      case TupleFormat::kFat: {
        auto succeed = UpdateInFatTuple(xIter, key, updateCallBack, updateDesc);
        xIter.UpdateContentionStats();
        Tuple::From(mutRawVal.Data())->WriteUnlock();
        if (!succeed) {
          JUMPMU_CONTINUE;
        }
        JUMPMU_RETURN OpCode::kOK;
      }
      case TupleFormat::kChained: {
        auto& chainedTuple = *ChainedTuple::From(mutRawVal.Data());
        if (chainedTuple.mIsTombstone) {
          chainedTuple.WriteUnlock();
          JUMPMU_RETURN OpCode::kNotFound;
        }

        chainedTuple.UpdateStats();

        // convert to fat tuple if it's frequently updated by me and other
        // workers
        if (mStore->mStoreOption->mEnableFatTuple && chainedTuple.ShouldConvertToFatTuple()) {
          chainedTuple.mTotalUpdates = 0;
          auto succeed = Tuple::ToFat(xIter);
          if (succeed) {
            xIter.mGuardedLeaf->mHasGarbage = true;
          }
          Tuple::From(mutRawVal.Data())->WriteUnlock();
          JUMPMU_CONTINUE;
        }

        // update the chained tuple
        chainedTuple.Update(xIter, key, updateCallBack, updateDesc);
        JUMPMU_RETURN OpCode::kOK;
      }
      default: {
        Log::Error("Unhandled tuple format: {}", TupleFormatUtil::ToString(tuple.mFormat));
      }
      }
    }
  }
  JUMPMU_CATCH() {
  }
  return OpCode::kOther;
}

OpCode TransactionKV::Insert(Slice key, Slice val) {
  LS_DCHECK(cr::WorkerContext::My().IsTxStarted());
  uint16_t payloadSize = val.size() + sizeof(ChainedTuple);

  while (true) {
    auto xIter = GetExclusiveIterator();
    auto ret = xIter.SeekToInsert(key);

    if (ret == OpCode::kDuplicated) {
      auto mutRawVal = xIter.MutableVal();
      auto* chainedTuple = ChainedTuple::From(mutRawVal.Data());
      auto lastWorkerId = chainedTuple->mWorkerId;
      auto lastTxId = chainedTuple->mTxId;
      auto isWriteLocked = chainedTuple->IsWriteLocked();
      LS_DCHECK(!chainedTuple->mWriteLocked,
                "Duplicate tuple should not be write locked, workerId={}, startTs={}, key={}, "
                "tupleLastWriter={}, tupleLastStartTs={}, tupleWriteLocked={}",
                cr::WorkerContext::My().mWorkerId, cr::WorkerContext::My().mActiveTx.mStartTs,
                key.ToString(), lastWorkerId, lastTxId, isWriteLocked);

      auto visibleForMe =
          cr::WorkerContext::My().mCc.VisibleForMe(chainedTuple->mWorkerId, chainedTuple->mTxId);

      if (chainedTuple->mIsTombstone && visibleForMe) {
        insertAfterRemove(xIter, key, val);
        return OpCode::kOK;
      }

      // conflict on tuple not visible for me
      if (!visibleForMe) {
        auto lastWorkerId = chainedTuple->mWorkerId;
        auto lastTxId = chainedTuple->mTxId;
        auto isWriteLocked = chainedTuple->IsWriteLocked();
        auto isTombsone = chainedTuple->mIsTombstone;
        Log::Info("Insert conflicted, current transaction should be aborted, workerId={}, "
                  "startTs={}, key={}, tupleLastWriter={}, tupleLastTxId={}, "
                  "tupleIsWriteLocked={}, tupleIsRemoved={}, tupleVisibleForMe={}",
                  cr::WorkerContext::My().mWorkerId, cr::WorkerContext::My().mActiveTx.mStartTs,
                  ToString(key), lastWorkerId, lastTxId, isWriteLocked, isTombsone, visibleForMe);
        return OpCode::kAbortTx;
      }

      // duplicated on tuple inserted by former committed transactions
      auto isTombsone = chainedTuple->mIsTombstone;
      Log::Info("Insert duplicated, workerId={}, startTs={}, key={}, tupleLastWriter={}, "
                "tupleLastTxId={}, tupleIsWriteLocked={}, tupleIsRemoved={}, tupleVisibleForMe={}",
                cr::WorkerContext::My().mWorkerId, cr::WorkerContext::My().mActiveTx.mStartTs,
                key.ToString(), lastWorkerId, lastTxId, isWriteLocked, isTombsone, visibleForMe);
      return OpCode::kDuplicated;
    }

    if (!xIter.HasEnoughSpaceFor(key.size(), payloadSize)) {
      xIter.SplitForKey(key);
      continue;
    }

    // WAL
    xIter.mGuardedLeaf.WriteWal<WalTxInsert>(key.size() + val.size(), key, val, 0, 0,
                                             kInvalidCommandid);

    // insert
    TransactionKV::InsertToNode(xIter.mGuardedLeaf, key, val, cr::WorkerContext::My().mWorkerId,
                                cr::ActiveTx().mStartTs, xIter.mSlotId);
    return OpCode::kOK;
  }
}

std::tuple<OpCode, uint16_t> TransactionKV::getVisibleTuple(Slice payload, ValCallback callback) {
  std::tuple<OpCode, uint16_t> ret;
  while (true) {
    JUMPMU_TRY() {
      const auto* const tuple = Tuple::From(payload.data());
      switch (tuple->mFormat) {
      case TupleFormat::kChained: {
        const auto* const chainedTuple = ChainedTuple::From(payload.data());
        ret = chainedTuple->GetVisibleTuple(payload, callback);
        JUMPMU_RETURN ret;
      }
      case TupleFormat::kFat: {
        const auto* const fatTuple = FatTuple::From(payload.data());
        ret = fatTuple->GetVisibleTuple(callback);
        JUMPMU_RETURN ret;
      }
      default: {
        Log::Error("Unhandled tuple format: {}", TupleFormatUtil::ToString(tuple->mFormat));
      }
      }
    }
    JUMPMU_CATCH() {
    }
  }
}

void TransactionKV::insertAfterRemove(PessimisticExclusiveIterator& xIter, Slice key, Slice val) {
  auto mutRawVal = xIter.MutableVal();
  auto* chainedTuple = ChainedTuple::From(mutRawVal.Data());
  auto lastWorkerId = chainedTuple->mWorkerId;
  auto lastTxId = chainedTuple->mTxId;
  auto lastCommandId = chainedTuple->mCommandId;
  auto isWriteLocked [[maybe_unused]] = chainedTuple->IsWriteLocked();
  LS_DCHECK(chainedTuple->mIsTombstone,
            "Tuple should be removed before insert, workerId={}, "
            "startTs={}, key={}, tupleLastWriter={}, "
            "tupleLastStartTs={}, tupleWriteLocked={}",
            cr::WorkerContext::My().mWorkerId, cr::WorkerContext::My().mActiveTx.mStartTs,
            key.ToString(), lastWorkerId, lastTxId, isWriteLocked);

  // create an insert version
  auto versionSize = sizeof(InsertVersion) + val.size() + key.size();
  auto commandId =
      cr::WorkerContext::My().mCc.PutVersion(mTreeId, false, versionSize, [&](uint8_t* versionBuf) {
        new (versionBuf) InsertVersion(lastWorkerId, lastTxId, lastCommandId, key, val);
      });

  // WAL
  auto prevWorkerId = chainedTuple->mWorkerId;
  auto prevTxId = chainedTuple->mTxId;
  auto prevCommandId = chainedTuple->mCommandId;
  xIter.mGuardedLeaf.WriteWal<WalTxInsert>(key.size() + val.size(), key, val, prevWorkerId,
                                           prevTxId, prevCommandId);

  // store the old chained tuple update stats
  auto totalUpdatesCopy = chainedTuple->mTotalUpdates;
  auto oldestTxCopy = chainedTuple->mOldestTx;

  // make room for the new chained tuple
  auto chainedTupleSize = val.size() + sizeof(ChainedTuple);
  if (mutRawVal.Size() < chainedTupleSize) {
    auto succeed [[maybe_unused]] = xIter.ExtendPayload(chainedTupleSize);
    LS_DCHECK(succeed,
              "Failed to extend btree node slot to store the expanded "
              "chained tuple, workerId={}, startTs={}, key={}, "
              "curRawValSize={}, chainedTupleSize={}",
              cr::WorkerContext::My().mWorkerId, cr::WorkerContext::My().mActiveTx.mStartTs,
              key.ToString(), mutRawVal.Size(), chainedTupleSize);

  } else if (mutRawVal.Size() > chainedTupleSize) {
    xIter.ShortenWithoutCompaction(chainedTupleSize);
  }

  // get the new value place and recreate a new chained tuple there
  auto newMutRawVal = xIter.MutableVal();
  auto* newChainedTuple = new (newMutRawVal.Data())
      ChainedTuple(cr::WorkerContext::My().mWorkerId, cr::ActiveTx().mStartTs, commandId, val);
  newChainedTuple->mTotalUpdates = totalUpdatesCopy;
  newChainedTuple->mOldestTx = oldestTxCopy;
  newChainedTuple->UpdateStats();
}

OpCode TransactionKV::Remove(Slice key) {
  LS_DCHECK(cr::WorkerContext::My().IsTxStarted());
  JUMPMU_TRY() {
    auto xIter = GetExclusiveIterator();
    if (xIter.SeekToEqual(key); !xIter.Valid()) {
      // Conflict detected, the tuple to be removed by the long-running transaction is removed by
      // newer transactions, abort it.
      if (cr::ActiveTx().IsLongRunning() && mGraveyard->Lookup(key, [&](Slice) {}) == OpCode::kOK) {
        JUMPMU_RETURN OpCode::kAbortTx;
      }
      JUMPMU_RETURN OpCode::kNotFound;
    }

    auto mutRawVal = xIter.MutableVal();
    auto* tuple = Tuple::From(mutRawVal.Data());

    // remove fat tuple is not supported yet
    if (tuple->mFormat == TupleFormat::kFat) {
      Log::Error("Remove failed, fat tuple is not supported yet");
      JUMPMU_RETURN OpCode::kNotFound;
    }

    // remove the chained tuple
    auto& chainedTuple = *static_cast<ChainedTuple*>(tuple);
    auto lastWorkerId = chainedTuple.mWorkerId;
    auto lastTxId = chainedTuple.mTxId;
    if (chainedTuple.IsWriteLocked() ||
        !cr::WorkerContext::My().mCc.VisibleForMe(lastWorkerId, lastTxId)) {
      Log::Info("Remove conflicted, current transaction should be aborted, workerId={}, "
                "startTs={}, key={}, tupleLastWriter={}, tupleLastStartTs={}, tupleVisibleForMe={}",
                cr::WorkerContext::My().mWorkerId, cr::WorkerContext::My().mActiveTx.mStartTs,
                key.ToString(), lastWorkerId, lastTxId,
                cr::WorkerContext::My().mCc.VisibleForMe(lastWorkerId, lastTxId));
      JUMPMU_RETURN OpCode::kAbortTx;
    }

    if (chainedTuple.mIsTombstone) {
      JUMPMU_RETURN OpCode::kNotFound;
    }

    chainedTuple.WriteLock();
    SCOPED_DEFER({
      LS_DCHECK(!Tuple::From(mutRawVal.Data())->IsWriteLocked(),
                "Tuple should be write unlocked after remove");
    });

    // 1. move current (key, value) pair to the version storage
    DanglingPointer danglingPointer(xIter);
    auto valSize = xIter.Val().size() - sizeof(ChainedTuple);
    auto val = chainedTuple.GetValue(valSize);
    auto versionSize = sizeof(RemoveVersion) + val.size() + key.size();
    auto commandId = cr::WorkerContext::My().mCc.PutVersion(
        mTreeId, true, versionSize, [&](uint8_t* versionBuf) {
          new (versionBuf) RemoveVersion(chainedTuple.mWorkerId, chainedTuple.mTxId,
                                         chainedTuple.mCommandId, key, val, danglingPointer);
        });

    // 2. write wal
    auto prevWorkerId = chainedTuple.mWorkerId;
    auto prevTxId = chainedTuple.mTxId;
    auto prevCommandId = chainedTuple.mCommandId;
    xIter.mGuardedLeaf.WriteWal<WalTxRemove>(key.size() + val.size(), key, val, prevWorkerId,
                                             prevTxId, prevCommandId);

    // 3. remove the tuple, leave a tombsone
    if (mutRawVal.Size() > sizeof(ChainedTuple)) {
      xIter.ShortenWithoutCompaction(sizeof(ChainedTuple));
    }
    chainedTuple.mIsTombstone = true;
    chainedTuple.mWorkerId = cr::WorkerContext::My().mWorkerId;
    chainedTuple.mTxId = cr::ActiveTx().mStartTs;
    chainedTuple.mCommandId = commandId;

    chainedTuple.WriteUnlock();
    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode TransactionKV::ScanDesc(Slice startKey, ScanCallback callback) {
  LS_DCHECK(cr::WorkerContext::My().IsTxStarted());
  if (cr::ActiveTx().IsLongRunning()) {
    TODOException();
    return OpCode::kAbortTx;
  }
  return scan4ShortRunningTx<false>(startKey, callback);
}

OpCode TransactionKV::ScanAsc(Slice startKey, ScanCallback callback) {
  LS_DCHECK(cr::WorkerContext::My().IsTxStarted());
  if (cr::ActiveTx().IsLongRunning()) {
    return scan4LongRunningTx(startKey, callback);
  }
  return scan4ShortRunningTx<true>(startKey, callback);
}

void TransactionKV::undo(const uint8_t* walPayloadPtr, const uint64_t txId [[maybe_unused]]) {
  auto& walPayload = *reinterpret_cast<const WalPayload*>(walPayloadPtr);
  switch (walPayload.mType) {
  case WalPayload::Type::kWalTxInsert: {
    return undoLastInsert(static_cast<const WalTxInsert*>(&walPayload));
  }
  case WalPayload::Type::kWalTxUpdate: {
    return undoLastUpdate(static_cast<const WalTxUpdate*>(&walPayload));
  }
  case WalPayload::Type::kWalTxRemove: {
    return undoLastRemove(static_cast<const WalTxRemove*>(&walPayload));
  }
  default: {
    Log::Error("Unknown wal payload type: {}", (uint64_t)walPayload.mType);
  }
  }
}

void TransactionKV::undoLastInsert(const WalTxInsert* walInsert) {
  // Assuming no insert after remove
  auto key = walInsert->GetKey();
  while (true) {
    JUMPMU_TRY() {
      auto xIter = GetExclusiveIterator();
      xIter.SeekToEqual(key);
      LS_DCHECK(xIter.Valid(),
                "Cannot find the inserted key in btree, workerId={}, "
                "startTs={}, key={}",
                cr::WorkerContext::My().mWorkerId, cr::WorkerContext::My().mActiveTx.mStartTs,
                key.ToString());
      // TODO(jian.z): write compensation wal entry
      if (walInsert->mPrevCommandId != kInvalidCommandid) {
        // only remove the inserted value and mark the chained tuple as
        // removed
        auto mutRawVal = xIter.MutableVal();
        auto* chainedTuple = ChainedTuple::From(mutRawVal.Data());

        if (mutRawVal.Size() > sizeof(ChainedTuple)) {
          xIter.ShortenWithoutCompaction(sizeof(ChainedTuple));
        }

        // mark as removed
        chainedTuple->mIsTombstone = true;
        chainedTuple->mWorkerId = walInsert->mPrevWorkerId;
        chainedTuple->mTxId = walInsert->mPrevTxId;
        chainedTuple->mCommandId = walInsert->mPrevCommandId;
      } else {
        // It's the first insert of of the value, remove the whole key-value
        // from the btree.
        auto ret = xIter.RemoveCurrent();
        if (ret != OpCode::kOK) {
          Log::Error("Undo last insert failed, failed to remove current key, "
                     "workerId={}, startTs={}, key={}, ret={}",
                     cr::WorkerContext::My().mWorkerId, cr::WorkerContext::My().mActiveTx.mStartTs,
                     key.ToString(), ToString(ret));
        }
      }

      xIter.TryMergeIfNeeded();
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
      Log::Warn("Undo insert failed, workerId={}, startTs={}", cr::WorkerContext::My().mWorkerId,
                cr::WorkerContext::My().mActiveTx.mStartTs);
    }
  }
}

void TransactionKV::undoLastUpdate(const WalTxUpdate* walUpdate) {
  auto key = walUpdate->GetKey();
  while (true) {
    JUMPMU_TRY() {
      auto xIter = GetExclusiveIterator();
      xIter.SeekToEqual(key);
      LS_DCHECK(xIter.Valid(),
                "Cannot find the updated key in btree, workerId={}, "
                "startTs={}, key={}",
                cr::WorkerContext::My().mWorkerId, cr::WorkerContext::My().mActiveTx.mStartTs,
                key.ToString());

      auto mutRawVal = xIter.MutableVal();
      auto& tuple = *Tuple::From(mutRawVal.Data());
      LS_DCHECK(!tuple.IsWriteLocked(), "Tuple is write locked, workerId={}, startTs={}, key={}",
                cr::WorkerContext::My().mWorkerId, cr::WorkerContext::My().mActiveTx.mStartTs,
                key.ToString());
      if (tuple.mFormat == TupleFormat::kFat) {
        FatTuple::From(mutRawVal.Data())->UndoLastUpdate();
      } else {
        auto& chainedTuple = *ChainedTuple::From(mutRawVal.Data());
        chainedTuple.mWorkerId = walUpdate->mPrevWorkerId;
        chainedTuple.mTxId = walUpdate->mPrevTxId;
        chainedTuple.mCommandId ^= walUpdate->mXorCommandId;
        auto& updateDesc = *walUpdate->GetUpdateDesc();
        auto* xorData = walUpdate->GetDeltaPtr();

        // 1. copy the new value to buffer
        auto deltaSize = walUpdate->GetDeltaSize();
        uint8_t buff[deltaSize];
        std::memcpy(buff, xorData, deltaSize);

        // 2. calculate the old value based on xor result and old value
        BasicKV::XorToBuffer(updateDesc, chainedTuple.mPayload, buff);

        // 3. replace new value with old value
        BasicKV::CopyToValue(updateDesc, buff, chainedTuple.mPayload);
      }
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
      Log::Warn("Undo update failed, workerId={}, startTs={}", cr::WorkerContext::My().mWorkerId,
                cr::WorkerContext::My().mActiveTx.mStartTs);
    }
  }
}

void TransactionKV::undoLastRemove(const WalTxRemove* walRemove) {
  Slice removedKey = walRemove->RemovedKey();
  while (true) {
    JUMPMU_TRY() {
      auto xIter = GetExclusiveIterator();
      xIter.SeekToEqual(removedKey);
      LS_DCHECK(xIter.Valid(),
                "Cannot find the tombstone of removed key, workerId={}, "
                "startTs={}, removedKey={}",
                cr::WorkerContext::My().mWorkerId, cr::WorkerContext::My().mActiveTx.mStartTs,
                removedKey.ToString());

      // resize the current slot to store the removed tuple
      auto chainedTupleSize = walRemove->mValSize + sizeof(ChainedTuple);
      auto curRawVal = xIter.Val();
      if (curRawVal.size() < chainedTupleSize) {
        auto succeed [[maybe_unused]] = xIter.ExtendPayload(chainedTupleSize);
        LS_DCHECK(succeed,
                  "Failed to extend btree node slot to store the "
                  "recovered chained tuple, workerId={}, startTs={}, "
                  "removedKey={}, curRawValSize={}, chainedTupleSize={}",
                  cr::WorkerContext::My().mWorkerId, cr::WorkerContext::My().mActiveTx.mStartTs,
                  removedKey.ToString(), curRawVal.size(), chainedTupleSize);
      } else if (curRawVal.size() > chainedTupleSize) {
        xIter.ShortenWithoutCompaction(chainedTupleSize);
      }

      auto curMutRawVal = xIter.MutableVal();
      new (curMutRawVal.Data()) ChainedTuple(walRemove->mPrevWorkerId, walRemove->mPrevTxId,
                                             walRemove->mPrevCommandId, walRemove->RemovedVal());

      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
      Log::Warn("Undo remove failed, workerId={}, startTs={}", cr::WorkerContext::My().mWorkerId,
                cr::WorkerContext::My().mActiveTx.mStartTs);
    }
  }
}

bool TransactionKV::UpdateInFatTuple(PessimisticExclusiveIterator& xIter, Slice key,
                                     MutValCallback updateCallBack, UpdateDesc& updateDesc) {
  while (true) {
    auto* fatTuple = reinterpret_cast<FatTuple*>(xIter.MutableVal().Data());
    LS_DCHECK(fatTuple->IsWriteLocked(), "Tuple should be write locked");

    if (!fatTuple->HasSpaceFor(updateDesc)) {
      fatTuple->GarbageCollection();
      if (fatTuple->HasSpaceFor(updateDesc)) {
        continue;
      }

      // Not enough space to store the fat tuple, convert to chained
      auto chainedTupleSize = fatTuple->mValSize + sizeof(ChainedTuple);
      LS_DCHECK(chainedTupleSize < xIter.Val().length());
      fatTuple->ConvertToChained(xIter.mBTree.mTreeId);
      xIter.ShortenWithoutCompaction(chainedTupleSize);
      return false;
    }

    auto performUpdate = [&]() {
      fatTuple->Append(updateDesc);
      fatTuple->mWorkerId = cr::WorkerContext::My().mWorkerId;
      fatTuple->mTxId = cr::ActiveTx().mStartTs;
      fatTuple->mCommandId = cr::WorkerContext::My().mCommandId++;
      updateCallBack(fatTuple->GetMutableValue());
      LS_DCHECK(fatTuple->mPayloadCapacity >= fatTuple->mPayloadSize);
    };

    if (!xIter.mBTree.mConfig.mEnableWal) {
      performUpdate();
      return true;
    }

    auto sizeOfDescAndDelta = updateDesc.SizeWithDelta();
    auto prevWorkerId = fatTuple->mWorkerId;
    auto prevTxId = fatTuple->mTxId;
    auto prevCommandId = fatTuple->mCommandId;
    auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WalTxUpdate>(
        key.size() + sizeOfDescAndDelta, key, updateDesc, sizeOfDescAndDelta, prevWorkerId,
        prevTxId, prevCommandId);
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

SpaceCheckResult TransactionKV::CheckSpaceUtilization(BufferFrame& bf) {
  if (!mStore->mStoreOption->mEnableXMerge) {
    return SpaceCheckResult::kNothing;
  }

  HybridGuard bfGuard(&bf.mHeader.mLatch);
  bfGuard.ToOptimisticOrJump();
  if (bf.mPage.mBTreeId != mTreeId) {
    jumpmu::Jump();
  }

  GuardedBufferFrame<BTreeNode> guardedNode(mStore->mBufferManager.get(), std::move(bfGuard), &bf);
  if (!guardedNode->mIsLeaf || !triggerPageWiseGarbageCollection(guardedNode)) {
    return BTreeGeneric::CheckSpaceUtilization(bf);
  }

  guardedNode.ToExclusiveMayJump();
  TXID sysTxId = utils::tlsStore->AllocSysTxTs();
  guardedNode.SyncSystemTxId(sysTxId);

  for (uint16_t i = 0; i < guardedNode->mNumSlots; i++) {
    auto& tuple = *Tuple::From(guardedNode->ValData(i));
    if (tuple.mFormat == TupleFormat::kFat) {
      auto& fatTuple = *FatTuple::From(guardedNode->ValData(i));
      const uint32_t newLength = fatTuple.mValSize + sizeof(ChainedTuple);
      fatTuple.ConvertToChained(mTreeId);
      LS_DCHECK(newLength < guardedNode->ValSize(i));
      guardedNode->ShortenPayload(i, newLength);
      LS_DCHECK(tuple.mFormat == TupleFormat::kChained);
    }
  }
  guardedNode->mHasGarbage = false;
  guardedNode.unlock();

  const SpaceCheckResult result = BTreeGeneric::CheckSpaceUtilization(bf);
  if (result == SpaceCheckResult::kPickAnotherBf) {
    return SpaceCheckResult::kPickAnotherBf;
  }
  return SpaceCheckResult::kRestartSameBf;
}

// Only point-gc and for removed tuples
void TransactionKV::GarbageCollect(const uint8_t* versionData, WORKERID versionWorkerId,
                                   TXID versionTxId, bool calledBefore) {
  const auto& version = *RemoveVersion::From(versionData);

  // Delete tombstones caused by transactions below mCc.mLocalWmkOfAllTx.
  if (versionTxId <= cr::WorkerContext::My().mCc.mLocalWmkOfAllTx) {
    LS_DLOG("Delete tombstones caused by transactions below "
            "mCc.mLocalWmkOfAllTx, versionWorkerId={}, versionTxId={}",
            versionWorkerId, versionTxId);
    LS_DCHECK(version.mDanglingPointer.mBf != nullptr);
    JUMPMU_TRY() {
      PessimisticExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this),
                                         version.mDanglingPointer.mBf,
                                         version.mDanglingPointer.mLatchVersionShouldBe);
      auto& node = xIter.mGuardedLeaf;
      auto& chainedTuple [[maybe_unused]] =
          *ChainedTuple::From(node->ValData(version.mDanglingPointer.mHeadSlot));
      LS_DCHECK(chainedTuple.mFormat == TupleFormat::kChained && !chainedTuple.IsWriteLocked() &&
                chainedTuple.mWorkerId == versionWorkerId && chainedTuple.mTxId == versionTxId &&
                chainedTuple.mIsTombstone);
      node->RemoveSlot(version.mDanglingPointer.mHeadSlot);
      xIter.TryMergeIfNeeded();
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
      LS_DLOG("Delete tombstones caused by transactions below mCc.mLocalWmkOfAllTx "
              "page has been modified since last delete");
    }
    return;
  }

  auto removedKey = version.RemovedKey();

  // Delete the removedKey from graveyard since no transaction needs it
  if (calledBefore) {
    LS_DLOG("Meet the removedKey again, delete it from graveyard, "
            "versionWorkerId={}, versionTxId={}, removedKey={}",
            versionWorkerId, versionTxId, removedKey.ToString());
    JUMPMU_TRY() {
      auto xIter = mGraveyard->GetExclusiveIterator();
      if (xIter.SeekToEqual(removedKey); xIter.Valid()) {
        auto ret [[maybe_unused]] = xIter.RemoveCurrent();
        LS_DCHECK(ret == OpCode::kOK,
                  "Failed to delete the removedKey from graveyard, ret={}, "
                  "versionWorkerId={}, versionTxId={}, removedKey={}",
                  ToString(ret), versionWorkerId, versionTxId, removedKey.ToString());
      } else {
        Log::Fatal("Cannot find the removedKey in graveyard, "
                   "versionWorkerId={}, versionTxId={}, removedKey={}",
                   versionWorkerId, versionTxId, removedKey.ToString());
      }
    }
    JUMPMU_CATCH() {
    }
    return;
  }

  // Move the removedKey to graveyard, it's removed by short-running
  // transaction but still visible for long-running transactions
  //
  // TODO(jian.z): handle corner cases in insert-after-remove scenario
  JUMPMU_TRY() {
    auto xIter = GetExclusiveIterator();
    if (xIter.SeekToEqual(removedKey); !xIter.Valid()) {
      Log::Fatal("Cannot find the removedKey in TransactionKV, should not "
                 "happen, versionWorkerId={}, versionTxId={}, removedKey={}",
                 versionWorkerId, versionTxId, removedKey.ToString());
      JUMPMU_RETURN;
    }

    MutableSlice mutRawVal = xIter.MutableVal();
    auto& tuple = *Tuple::From(mutRawVal.Data());
    if (tuple.mFormat == TupleFormat::kFat) {
      LS_DLOG("Skip moving removedKey to graveyard for FatTuple, "
              "versionWorkerId={}, versionTxId={}, removedKey={}",
              versionWorkerId, versionTxId, removedKey.ToString());
      JUMPMU_RETURN;
    }

    ChainedTuple& chainedTuple = *ChainedTuple::From(mutRawVal.Data());
    if (chainedTuple.IsWriteLocked()) {
      Log::Fatal("The removedKey is write locked, should not happen, "
                 "versionWorkerId={}, versionTxId={}, removedKey={}",
                 versionWorkerId, versionTxId, removedKey.ToString());
      JUMPMU_RETURN;
    }

    if (chainedTuple.mWorkerId == versionWorkerId && chainedTuple.mTxId == versionTxId &&
        chainedTuple.mIsTombstone) {

      LS_DCHECK(chainedTuple.mTxId > cr::WorkerContext::My().mCc.mLocalWmkOfAllTx,
                "The removedKey is under mCc.mLocalWmkOfAllTx, should "
                "not happen, mCc.mLocalWmkOfAllTx={}, "
                "versionWorkerId={}, versionTxId={}, removedKey={}",
                cr::WorkerContext::My().mCc.mLocalWmkOfAllTx, versionWorkerId, versionTxId,
                removedKey.ToString());
      if (chainedTuple.mTxId <= cr::WorkerContext::My().mCc.mLocalWmkOfShortTx) {
        LS_DLOG("Move the removedKey to graveyard, versionWorkerId={}, "
                "versionTxId={}, removedKey={}",
                versionWorkerId, versionTxId, removedKey.ToString());
        // insert the removed key value to graveyard
        auto graveyardXIter = mGraveyard->GetExclusiveIterator();
        auto gRet = graveyardXIter.InsertKV(removedKey, xIter.Val());
        if (gRet != OpCode::kOK) {
          Log::Fatal("Failed to insert the removedKey to graveyard, ret={}, "
                     "versionWorkerId={}, versionTxId={}, removedKey={}, "
                     "removedVal={}",
                     ToString(gRet), versionWorkerId, versionTxId, removedKey.ToString(),
                     xIter.Val().ToString());
        }

        // remove the tombsone from main tree
        auto ret [[maybe_unused]] = xIter.RemoveCurrent();
        LS_DCHECK(ret == OpCode::kOK,
                  "Failed to delete the removedKey tombstone from main tree, ret={}, "
                  "versionWorkerId={}, versionTxId={}, removedKey={}",
                  ToString(ret), versionWorkerId, versionTxId, removedKey.ToString());
        xIter.TryMergeIfNeeded();
      } else {
        Log::Fatal("Meet a remove version upper than mCc.mLocalWmkOfShortTx, "
                   "should not happen, mCc.mLocalWmkOfShortTx={}, "
                   "versionWorkerId={}, versionTxId={}, removedKey={}",
                   cr::WorkerContext::My().mCc.mLocalWmkOfShortTx, versionWorkerId, versionTxId,
                   removedKey.ToString());
      }
    } else {
      LS_DLOG("Skip moving removedKey to graveyard, tuple changed after "
              "remove, versionWorkerId={}, versionTxId={}, removedKey={}",
              versionWorkerId, versionTxId, removedKey.ToString());
    }
  }
  JUMPMU_CATCH() {
    LS_DLOG("GarbageCollect failed, try for next round, "
            "versionWorkerId={}, versionTxId={}, removedKey={}",
            versionWorkerId, versionTxId, removedKey.ToString());
  }
}

void TransactionKV::unlock(const uint8_t* walEntryPtr) {
  const WalPayload& entry = *reinterpret_cast<const WalPayload*>(walEntryPtr);
  Slice key;
  switch (entry.mType) {
  case WalPayload::Type::kWalTxInsert: {
    // Assuming no insert after remove
    auto& walInsert = *reinterpret_cast<const WalTxInsert*>(&entry);
    key = walInsert.GetKey();
    break;
  }
  case WalPayload::Type::kWalTxUpdate: {
    auto& walUpdate = *reinterpret_cast<const WalTxUpdate*>(&entry);
    key = walUpdate.GetKey();
    break;
  }
  case WalPayload::Type::kWalTxRemove: {
    auto& removeEntry = *reinterpret_cast<const WalTxRemove*>(&entry);
    key = removeEntry.RemovedKey();
    break;
  }
  default: {
    return;
    break;
  }
  }

  JUMPMU_TRY() {
    auto xIter = GetExclusiveIterator();
    xIter.SeekToEqual(key);
    LS_DCHECK(xIter.Valid(), "Cannot find the key in btree, workerId={}, startTs={}, key={}",
              cr::WorkerContext::My().mWorkerId, cr::WorkerContext::My().mActiveTx.mStartTs,
              key.ToString());
    auto& tuple = *Tuple::From(xIter.MutableVal().Data());
    ENSURE(tuple.mFormat == TupleFormat::kChained);
  }
  JUMPMU_CATCH() {
    UNREACHABLE();
  }
}

// TODO: index range lock for serializability
template <bool asc>
OpCode TransactionKV::scan4ShortRunningTx(Slice key, ScanCallback callback) {
  bool keepScanning = true;
  JUMPMU_TRY() {
    auto iter = GetIterator();
    if (asc) {
      iter.SeekToFirstGreaterEqual(key);
    } else {
      iter.SeekToLastLessEqual(key);
    }

    while (iter.Valid()) {
      iter.AssembleKey();
      Slice scannedKey = iter.Key();
      getVisibleTuple(iter.Val(),
                      [&](Slice scannedVal) { keepScanning = callback(scannedKey, scannedVal); });
      if (!keepScanning) {
        JUMPMU_RETURN OpCode::kOK;
      }

      if (asc) {
        iter.Next();
      } else {
        iter.Prev();
      }
    }
    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
    LS_DCHECK(false, "Scan failed, key={}", key.ToString());
  }
  JUMPMU_RETURN OpCode::kOther;
}

// TODO: support scanning desc
template <bool asc>
OpCode TransactionKV::scan4LongRunningTx(Slice key, ScanCallback callback) {
  bool keepScanning = true;
  JUMPMU_TRY() {
    auto iter = GetIterator();
    OpCode oRet;

    auto gIter = mGraveyard->GetIterator();
    OpCode gRet;

    Slice graveyardLowerBound, graveyardUpperBound;
    graveyardLowerBound = key;

    if (iter.SeekToFirstGreaterEqual(key); !iter.Valid()) {
      JUMPMU_RETURN OpCode::kOK;
    }
    oRet = OpCode::kOK;
    iter.AssembleKey();

    // Now it begins
    graveyardUpperBound = iter.mGuardedLeaf->GetUpperFence();
    auto gRange = [&]() {
      gIter.Reset();
      if (mGraveyard->IsRangeEmpty(graveyardLowerBound, graveyardUpperBound)) {
        gRet = OpCode::kOther;
        return;
      }
      if (gIter.SeekToFirstGreaterEqual(graveyardLowerBound); !gIter.Valid()) {
        gRet = OpCode::kNotFound;
        return;
      }

      gIter.AssembleKey();
      if (gIter.Key() > graveyardUpperBound) {
        gRet = OpCode::kOther;
        gIter.Reset();
        return;
      }

      gRet = OpCode::kOK;
    };

    gRange();
    auto takeFromOltp = [&]() {
      getVisibleTuple(iter.Val(), [&](Slice value) { keepScanning = callback(iter.Key(), value); });
      if (!keepScanning) {
        return false;
      }
      const bool isLastOne = iter.IsLastOne();
      if (isLastOne) {
        gIter.Reset();
      }
      iter.Next();
      oRet = iter.Valid() ? OpCode::kOK : OpCode::kNotFound;
      if (isLastOne) {
        if (iter.mBuffer.size() < iter.mFenceSize + 1u) {
          std::basic_string<uint8_t> newBuffer(iter.mBuffer.size() + 1, 0);
          memcpy(newBuffer.data(), iter.mBuffer.data(), iter.mFenceSize);
          iter.mBuffer = std::move(newBuffer);
        }
        graveyardLowerBound = Slice(&iter.mBuffer[0], iter.mFenceSize + 1);
        graveyardUpperBound = iter.mGuardedLeaf->GetUpperFence();
        gRange();
      }
      return true;
    };
    while (true) {
      if (gRet != OpCode::kOK && oRet == OpCode::kOK) {
        iter.AssembleKey();
        if (!takeFromOltp()) {
          JUMPMU_RETURN OpCode::kOK;
        }
      } else if (gRet == OpCode::kOK && oRet != OpCode::kOK) {
        gIter.AssembleKey();
        Slice gKey = gIter.Key();
        getVisibleTuple(gIter.Val(), [&](Slice value) { keepScanning = callback(gKey, value); });
        if (!keepScanning) {
          JUMPMU_RETURN OpCode::kOK;
        }
        gIter.Next();
        gRet = gIter.Valid() ? OpCode::kOK : OpCode::kNotFound;
      } else if (gRet == OpCode::kOK && oRet == OpCode::kOK) {
        iter.AssembleKey();
        gIter.AssembleKey();
        Slice gKey = gIter.Key();
        Slice oltpKey = iter.Key();
        if (oltpKey <= gKey) {
          if (!takeFromOltp()) {
            JUMPMU_RETURN OpCode::kOK;
          }
        } else {
          getVisibleTuple(gIter.Val(), [&](Slice value) { keepScanning = callback(gKey, value); });
          if (!keepScanning) {
            JUMPMU_RETURN OpCode::kOK;
          }
          gIter.Next();
          gRet = gIter.Valid() ? OpCode::kOK : OpCode::kNotFound;
        }
      } else {
        JUMPMU_RETURN OpCode::kOK;
      }
    }
  }
  JUMPMU_CATCH() {
    LS_DCHECK(false);
  }
  JUMPMU_RETURN OpCode::kOther;
}

} // namespace leanstore::storage::btree
