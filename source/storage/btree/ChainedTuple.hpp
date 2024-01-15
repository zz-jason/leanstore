#pragma once

#include "Tuple.hpp"
#include "shared-headers/Units.hpp"
#include "storage/btree/BTreeLL.hpp"
#include "storage/btree/core/BTreeExclusiveIterator.hpp"

#include <glog/logging.h>

namespace leanstore {
namespace storage {
namespace btree {

/// History versions of chained tuple are stored in the history tree of the
/// current worker thread.
/// Chained: only scheduled gc todos.
class __attribute__((packed)) ChainedTuple : public Tuple {
public:
  u16 mTotalUpdates = 0;

  u16 mOldestTx = 0;

  u8 mIsRemoved = 1;

  // latest version in-place
  u8 mPayload[];

public:
  /// Construct a ChainedTuple, copy the value to its payload
  ///
  /// NOTE: Payload space should be allocated in advance. This constructor is
  /// usually called by a placmenet new operator.
  ChainedTuple(WORKERID workerId, TXID txId, Slice val)
      : Tuple(TupleFormat::kChained, workerId, txId),
        mIsRemoved(false) {
    std::memcpy(mPayload, val.data(), val.size());
  }

  ChainedTuple(WORKERID workerId, TXID txId, COMMANDID commandId, Slice val)
      : Tuple(TupleFormat::kChained, workerId, txId, commandId),
        mIsRemoved(false) {
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
        mIsRemoved(false) {
    std::memmove(mPayload, oldFatTuple.mPayload, oldFatTuple.mValSize);
  }

public:
  inline Slice GetValue(size_t size) const {
    return Slice(mPayload, size);
  }

  std::tuple<OpCode, u16> GetVisibleTuple(Slice payload,
                                          ValCallback callback) const;

  void UpdateStats() {
    if (cr::Worker::my().cc.isVisibleForAll(mTxId) ||
        mOldestTx != static_cast<u16>(cr::Worker::sOldestAllStartTs & 0xFFFF)) {
      mOldestTx = 0;
      mTotalUpdates = 0;
      return;
    }
    mTotalUpdates++;
  }

  bool ShouldConvertToFatTuple() {
    bool commandValid = mCommandId != kInvalidCommandid;
    bool hasLongRunningOLAP =
        cr::Worker::sOldestOltpStartTx != cr::Worker::sOldestAllStartTs;
    bool frequentlyUpdated = mTotalUpdates > FLAGS_worker_threads;
    bool recentUpdatedByOthers = mWorkerId != cr::Worker::my().mWorkerId ||
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
  if (cr::Worker::my().cc.VisibleForMe(mWorkerId, mTxId)) {
    if (mIsRemoved) {
      return {OpCode::kNotFound, 1};
    }

    auto valSize = payload.length() - sizeof(ChainedTuple);
    callback(GetValue(valSize));
    return {OpCode::kOK, 1};
  }

  if (mCommandId == kInvalidCommandid) {
    JUMPMU_RETURN{OpCode::kNotFound, 1};
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
    bool found = cr::Worker::my().cc.retrieveVersion(
        prevWorkerId, prevTxId, prevCommandId,
        [&](const u8* versionBuf, u64 versionSize) {
          auto& version = *reinterpret_cast<const Version*>(versionBuf);
          if (version.mType == VersionType::kUpdate) {
            auto& updateVersion = *UpdateVersion::From(versionBuf);
            if (updateVersion.mIsDelta) {
              // Apply delta
              auto& updateDesc = *UpdateDesc::From(updateVersion.mPayload);
              auto* oldValOfSlots = updateVersion.mPayload + updateDesc.Size();
              BTreeLL::CopyToValue(updateDesc, oldValOfSlots, valueBuf.get());
            } else {
              valueSize = versionSize - sizeof(UpdateVersion);
              valueBuf = std::make_unique<u8[]>(valueSize);
              std::memcpy(valueBuf.get(), updateVersion.mPayload, valueSize);
            }
          } else if (version.mType == VersionType::kRemove) {
            auto& removeVersion = *RemoveVersion::From(versionBuf);
            auto removedVal = removeVersion.RemovedVal();
            valueSize = removeVersion.mValSize;
            valueBuf = std::make_unique<u8[]>(removedVal.size());
            std::memcpy(valueBuf.get(), removedVal.data(), removedVal.size());
          } else {
            UNREACHABLE();
          }

          prevWorkerId = version.mWorkerId;
          prevTxId = version.mTxId;
          prevCommandId = version.mCommandId;
        });
    if (!found) {
      LOG(ERROR) << "Not found in the version tree"
                 << ", workerId=" << cr::Worker::my().mWorkerId
                 << ", startTs=" << cr::ActiveTx().mStartTs
                 << ", versionsRead=" << versionsRead
                 << ", prevWorkerId=" << prevWorkerId
                 << ", prevTxId=" << prevTxId
                 << ", prevCommandId=" << prevCommandId
                 << ", prevTxId belongs to="
                 << std::find(cr::Worker::my().cc.local_workers_start_ts.get(),
                              cr::Worker::my().cc.local_workers_start_ts.get() +
                                  cr::Worker::my().mNumAllWorkers,
                              prevTxId) -
                        cr::Worker::my().cc.local_workers_start_ts.get();
      return {OpCode::kNotFound, versionsRead};
    }

    if (cr::Worker::my().cc.VisibleForMe(prevWorkerId, prevTxId)) {
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
  auto commandId = cr::Worker::my().cc.insertVersion(
      treeId, false, versionSize, [&](u8* versionBuf) {
        auto& updateVersion =
            *new (versionBuf) UpdateVersion(mWorkerId, mTxId, mCommandId, true);
        std::memcpy(updateVersion.mPayload, &updateDesc, updateDesc.Size());
        auto* dest = updateVersion.mPayload + updateDesc.Size();
        BTreeLL::CopyToBuffer(updateDesc, mPayload, dest);
      });

  auto performUpdate = [&]() {
    auto mutRawVal = xIter.MutableVal();
    auto userValSize = mutRawVal.Size() - sizeof(ChainedTuple);
    updateCallBack(MutableSlice(mPayload, userValSize));
    mWorkerId = cr::Worker::my().mWorkerId;
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

  if (!xIter.mBTree.config.mEnableWal) {
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
  BTreeLL::CopyToBuffer(updateDesc, mPayload, walBuf);

  // 2. update the value in-place
  performUpdate();

  // 3. xor with the updated new value and store to wal buffer
  BTreeLL::XorToBuffer(updateDesc, mPayload, walBuf);

  walHandler.SubmitWal();
}

} // namespace btree
} // namespace storage
} // namespace leanstore