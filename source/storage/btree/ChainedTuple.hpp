#pragma once

#include "Tuple.hpp"
#include "shared-headers/Units.hpp"
#include "storage/btree/BasicKV.hpp"
#include "storage/btree/core/BTreeExclusiveIterator.hpp"
#include "storage/btree/core/BTreeWALPayload.hpp"

#include <glog/logging.h>

namespace leanstore::storage::btree {

/// History versions of chained tuple are stored in the history tree of the
/// current worker thread.
/// Chained: only scheduled gc.
class __attribute__((packed)) ChainedTuple : public Tuple {
public:
  u16 mTotalUpdates = 0;

  u16 mOldestTx = 0;

  u8 mIsTombstone = 1;

  // latest version in-place
  u8 mPayload[];

public:
  /// Construct a ChainedTuple, copy the value to its payload
  ///
  /// NOTE: Payload space should be allocated in advance. This constructor is
  /// usually called by a placmenet new operator.
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

  /// Construct a ChainedTuple from an existing FatTuple, the new ChainedTuple
  /// may share the same space with the input FatTuple, so std::memmove is
  /// used to handle the overlap bytes.
  ///
  /// NOTE: This constructor is usually called by a placmenet new operator on
  /// the address of the FatTuple
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

  std::tuple<OpCode, u16> GetVisibleTuple(Slice payload,
                                          ValCallback callback) const;

  void UpdateStats() {
    if (cr::Worker::My().cc.VisibleForAll(mTxId) ||
        mOldestTx != static_cast<u16>(cr::Worker::sOldestActiveTx & 0xFFFF)) {
      mOldestTx = 0;
      mTotalUpdates = 0;
      return;
    }
    mTotalUpdates++;
  }

  bool ShouldConvertToFatTuple() {
    bool commandValid = mCommandId != kInvalidCommandid;
    bool hasLongRunningOLAP =
        cr::Worker::sOldestActiveShortTx != cr::Worker::sOldestActiveTx;
    bool frequentlyUpdated = mTotalUpdates > FLAGS_worker_threads;
    bool recentUpdatedByOthers = mWorkerId != cr::Worker::My().mWorkerId ||
                                 mTxId != cr::ActiveTx().mStartTs;
    return commandValid && hasLongRunningOLAP && recentUpdatedByOthers &&
           frequentlyUpdated;
  }

  void Update(BTreeExclusiveIterator& xIter, Slice key,
              MutValCallback updateCallBack, UpdateDesc& updateDesc);

public:
  inline static const ChainedTuple* From(const u8* buffer) {
    return reinterpret_cast<const ChainedTuple*>(buffer);
  }

  inline static ChainedTuple* From(u8* buffer) {
    return reinterpret_cast<ChainedTuple*>(buffer);
  }
};

inline std::tuple<OpCode, u16> ChainedTuple::GetVisibleTuple(
    Slice payload, ValCallback callback) const {
  if (cr::Worker::My().cc.VisibleForMe(mWorkerId, mTxId)) {
    if (mIsTombstone) {
      return {OpCode::kNotFound, 1};
    }

    auto valSize = payload.length() - sizeof(ChainedTuple);
    callback(GetValue(valSize));
    return {OpCode::kOK, 1};
  }

  if (mCommandId == kInvalidCommandid) {
    return {OpCode::kNotFound, 1};
  }

  // Head is not visible
  u16 valueSize = payload.length() - sizeof(ChainedTuple);
  auto valueBuf = std::make_unique<u8[]>(valueSize);
  std::memcpy(valueBuf.get(), this->mPayload, valueSize);

  WORKERID prevWorkerId = mWorkerId;
  TXID prevTxId = mTxId;
  COMMANDID prevCommandId = mCommandId;

  u16 versionsRead = 1;
  while (true) {
    bool found = cr::Worker::My().cc.GetVersion(
        prevWorkerId, prevTxId, prevCommandId,
        [&](const u8* versionBuf, u64 versionSize) {
          auto& version = *reinterpret_cast<const Version*>(versionBuf);
          switch (version.mType) {
          case VersionType::kUpdate: {
            auto& updateVersion = *UpdateVersion::From(versionBuf);
            if (updateVersion.mIsDelta) {
              // Apply delta
              auto& updateDesc = *UpdateDesc::From(updateVersion.mPayload);
              auto* oldValOfSlots = updateVersion.mPayload + updateDesc.Size();
              BasicKV::CopyToValue(updateDesc, oldValOfSlots, valueBuf.get());
            } else {
              valueSize = versionSize - sizeof(UpdateVersion);
              valueBuf = std::make_unique<u8[]>(valueSize);
              std::memcpy(valueBuf.get(), updateVersion.mPayload, valueSize);
            }
            break;
          }
          case VersionType::kRemove: {
            auto& removeVersion = *RemoveVersion::From(versionBuf);
            auto removedVal = removeVersion.RemovedVal();
            valueSize = removeVersion.mValSize;
            valueBuf = std::make_unique<u8[]>(removedVal.size());
            std::memcpy(valueBuf.get(), removedVal.data(), removedVal.size());
            break;
          }
          case VersionType::kInsert: {
            auto& insertVersion = *InsertVersion::From(versionBuf);
            valueSize = insertVersion.mValSize;
            valueBuf = std::make_unique<u8[]>(valueSize);
            std::memcpy(valueBuf.get(), insertVersion.mPayload, valueSize);
            break;
          }
          }

          prevWorkerId = version.mWorkerId;
          prevTxId = version.mTxId;
          prevCommandId = version.mCommandId;
        });
    if (!found) {
      LOG(ERROR) << "Not found in the version tree"
                 << ", workerId=" << cr::Worker::My().mWorkerId
                 << ", startTs=" << cr::ActiveTx().mStartTs
                 << ", versionsRead=" << versionsRead
                 << ", prevWorkerId=" << prevWorkerId
                 << ", prevTxId=" << prevTxId
                 << ", prevCommandId=" << prevCommandId;
      return {OpCode::kNotFound, versionsRead};
    }

    if (cr::Worker::My().cc.VisibleForMe(prevWorkerId, prevTxId)) {
      callback(Slice(valueBuf.get(), valueSize));
      return {OpCode::kOK, versionsRead};
    }
    versionsRead++;
  }
  return {OpCode::kNotFound, versionsRead};
}

inline void ChainedTuple::Update(BTreeExclusiveIterator& xIter, Slice key,
                                 MutValCallback updateCallBack,
                                 UpdateDesc& updateDesc) {
  auto sizeOfDescAndDelta = updateDesc.SizeWithDelta();
  auto versionSize = sizeOfDescAndDelta + sizeof(UpdateVersion);

  // Move the newest tuple to the history version tree.
  auto treeId = xIter.mBTree.mTreeId;
  auto commandId = cr::Worker::My().cc.PutVersion(
      treeId, false, versionSize, [&](u8* versionBuf) {
        auto& updateVersion =
            *new (versionBuf) UpdateVersion(mWorkerId, mTxId, mCommandId, true);
        std::memcpy(updateVersion.mPayload, &updateDesc, updateDesc.Size());
        auto* dest = updateVersion.mPayload + updateDesc.Size();
        BasicKV::CopyToBuffer(updateDesc, mPayload, dest);
      });

  auto performUpdate = [&]() {
    auto mutRawVal = xIter.MutableVal();
    auto userValSize = mutRawVal.Size() - sizeof(ChainedTuple);
    updateCallBack(MutableSlice(mPayload, userValSize));
    mWorkerId = cr::Worker::My().mWorkerId;
    mTxId = cr::ActiveTx().mStartTs;
    mCommandId = commandId;
  };

  SCOPED_DEFER({
    WriteUnlock();
    xIter.MarkAsDirty();
    xIter.UpdateContentionStats();
    COUNTERS_BLOCK() {
      WorkerCounters::MyCounters().cc_update_versions_created[treeId]++;
    }
  });

  if (!xIter.mBTree.mConfig.mEnableWal) {
    performUpdate();
    return;
  }

  auto prevWorkerId = mWorkerId;
  auto prevTxId = mTxId;
  auto prevCommandId = mCommandId;
  auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WALTxUpdate>(
      key.size() + sizeOfDescAndDelta, key, updateDesc, sizeOfDescAndDelta,
      prevWorkerId, prevTxId, prevCommandId);
  auto* walBuf = walHandler->GetDeltaPtr();

  // 1. copy old value to wal buffer
  BasicKV::CopyToBuffer(updateDesc, mPayload, walBuf);

  // 2. update the value in-place
  performUpdate();

  // 3. xor with the updated new value and store to wal buffer
  BasicKV::XorToBuffer(updateDesc, mPayload, walBuf);

  walHandler.SubmitWal();
}

} // namespace leanstore::storage::btree