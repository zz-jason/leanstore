#include "leanstore/btree/ChainedTuple.hpp"

#include "btree/core/BTreeWalPayload.hpp"
#include "leanstore/utils/Log.hpp"

namespace leanstore::storage::btree {

std::tuple<OpCode, uint16_t> ChainedTuple::GetVisibleTuple(Slice payload,
                                                           ValCallback callback) const {
  if (cr::Worker::My().mCc.VisibleForMe(mWorkerId, mTxId)) {
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
  uint16_t valueSize = payload.length() - sizeof(ChainedTuple);
  auto valueBuf = std::make_unique<uint8_t[]>(valueSize);
  std::memcpy(valueBuf.get(), this->mPayload, valueSize);

  WORKERID newerWorkerId = mWorkerId;
  TXID newerTxId = mTxId;
  COMMANDID newerCommandId = mCommandId;

  uint16_t versionsRead = 1;
  while (true) {
    bool found = cr::Worker::My().mCc.GetVersion(
        newerWorkerId, newerTxId, newerCommandId,
        [&](const uint8_t* versionBuf, uint64_t versionSize) {
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
              valueBuf = std::make_unique<uint8_t[]>(valueSize);
              std::memcpy(valueBuf.get(), updateVersion.mPayload, valueSize);
            }
            break;
          }
          case VersionType::kRemove: {
            auto& removeVersion = *RemoveVersion::From(versionBuf);
            auto removedVal = removeVersion.RemovedVal();
            valueSize = removeVersion.mValSize;
            valueBuf = std::make_unique<uint8_t[]>(removedVal.size());
            std::memcpy(valueBuf.get(), removedVal.data(), removedVal.size());
            break;
          }
          case VersionType::kInsert: {
            auto& insertVersion = *InsertVersion::From(versionBuf);
            valueSize = insertVersion.mValSize;
            valueBuf = std::make_unique<uint8_t[]>(valueSize);
            std::memcpy(valueBuf.get(), insertVersion.mPayload, valueSize);
            break;
          }
          }

          newerWorkerId = version.mWorkerId;
          newerTxId = version.mTxId;
          newerCommandId = version.mCommandId;
        });
    if (!found) {
      Log::Error("Not found in the version tree, workerId={}, startTs={}, "
                 "versionsRead={}, newerWorkerId={}, newerTxId={}, "
                 "newerCommandId={}",
                 cr::Worker::My().mWorkerId, cr::ActiveTx().mStartTs, versionsRead, newerWorkerId,
                 newerTxId, newerCommandId);
      return {OpCode::kNotFound, versionsRead};
    }

    if (cr::Worker::My().mCc.VisibleForMe(newerWorkerId, newerTxId)) {
      callback(Slice(valueBuf.get(), valueSize));
      return {OpCode::kOK, versionsRead};
    }
    versionsRead++;
  }
  return {OpCode::kNotFound, versionsRead};
}

void ChainedTuple::Update(BTreePessimisticExclusiveIterator& xIter, Slice key,
                          MutValCallback updateCallBack, UpdateDesc& updateDesc) {
  auto sizeOfDescAndDelta = updateDesc.SizeWithDelta();
  auto versionSize = sizeOfDescAndDelta + sizeof(UpdateVersion);

  // Move the newest tuple to the history version tree.
  auto treeId = xIter.mBTree.mTreeId;
  auto currCommandId =
      cr::Worker::My().mCc.PutVersion(treeId, false, versionSize, [&](uint8_t* versionBuf) {
        auto& updateVersion = *new (versionBuf) UpdateVersion(mWorkerId, mTxId, mCommandId, true);
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
    mCommandId = currCommandId;
  };

  SCOPED_DEFER({
    WriteUnlock();
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
  auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WalTxUpdate>(
      key.size() + sizeOfDescAndDelta, key, updateDesc, sizeOfDescAndDelta, prevWorkerId, prevTxId,
      prevCommandId ^ currCommandId);
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