#pragma once

#include "Tuple.hpp"
#include "leanstore/Units.hpp"
#include "leanstore/btree/BasicKV.hpp"
#include "leanstore/btree/core/PessimisticExclusiveIterator.hpp"
#include "leanstore/concurrency/CRManager.hpp"
#include "leanstore/concurrency/WorkerContext.hpp"

namespace leanstore::storage::btree {

//! History versions of chained tuple are stored in the history tree of the
//! current worker thread.
//! Chained: only scheduled gc.
class __attribute__((packed)) ChainedTuple : public Tuple {
public:
  uint16_t mTotalUpdates = 0;

  uint16_t mOldestTx = 0;

  uint8_t mIsTombstone = 1;

  // latest version in-place
  uint8_t mPayload[];

public:
  //! Construct a ChainedTuple, copy the value to its payload
  ///
  //! NOTE: Payload space should be allocated in advance. This constructor is
  //! usually called by a placmenet new operator.
  ChainedTuple(WORKERID workerId, TXID txId, Slice val)
      : Tuple(TupleFormat::kChained, workerId, txId),
        mIsTombstone(false) {
    std::memcpy(mPayload, val.data(), val.size());
  }

  ChainedTuple(WORKERID workerId, TXID txId, COMMANDID commandId, Slice val)
      : Tuple(TupleFormat::kChained, workerId, txId, commandId),
        mIsTombstone(false) {
    std::memcpy(mPayload, val.data(), val.size());
  }

  //! Construct a ChainedTuple from an existing FatTuple, the new ChainedTuple
  //! may share the same space with the input FatTuple, so std::memmove is
  //! used to handle the overlap bytes.
  ///
  //! NOTE: This constructor is usually called by a placmenet new operator on
  //! the address of the FatTuple
  ChainedTuple(FatTuple& oldFatTuple)
      : Tuple(TupleFormat::kChained, oldFatTuple.mWorkerId, oldFatTuple.mTxId,
              oldFatTuple.mCommandId),
        mIsTombstone(false) {
    std::memmove(mPayload, oldFatTuple.mPayload, oldFatTuple.mValSize);
  }

public:
  inline Slice GetValue(size_t size) const {
    return Slice(mPayload, size);
  }

  std::tuple<OpCode, uint16_t> GetVisibleTuple(Slice payload, ValCallback callback) const;

  void UpdateStats() {
    if (cr::WorkerContext::My().mCc.VisibleForAll(mTxId) ||
        mOldestTx !=
            static_cast<uint16_t>(
                cr::WorkerContext::My().mStore->mCRManager->mGlobalWmkInfo.mOldestActiveTx &
                0xFFFF)) {
      mOldestTx = 0;
      mTotalUpdates = 0;
      return;
    }
    mTotalUpdates++;
  }

  bool ShouldConvertToFatTuple() {
    bool commandValid = mCommandId != kInvalidCommandid;
    bool hasLongRunningOLAP =
        cr::WorkerContext::My().mStore->mCRManager->mGlobalWmkInfo.HasActiveLongRunningTx();
    bool frequentlyUpdated =
        mTotalUpdates > cr::WorkerContext::My().mStore->mStoreOption->mWorkerThreads;
    bool recentUpdatedByOthers =
        mWorkerId != cr::WorkerContext::My().mWorkerId || mTxId != cr::ActiveTx().mStartTs;
    return commandValid && hasLongRunningOLAP && recentUpdatedByOthers && frequentlyUpdated;
  }

  void Update(PessimisticExclusiveIterator& xIter, Slice key, MutValCallback updateCallBack,
              UpdateDesc& updateDesc);

public:
  inline static const ChainedTuple* From(const uint8_t* buffer) {
    return reinterpret_cast<const ChainedTuple*>(buffer);
  }

  inline static ChainedTuple* From(uint8_t* buffer) {
    return reinterpret_cast<ChainedTuple*>(buffer);
  }
};

} // namespace leanstore::storage::btree