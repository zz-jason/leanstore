#pragma once

#include "BasicKV.hpp"
#include "Config.hpp"
#include "KVInterface.hpp"
#include "concurrency-recovery/Worker.hpp"
#include "shared-headers/Units.hpp"
#include "storage/btree/BasicKV.hpp"
#include "storage/btree/ChainedTuple.hpp"
#include "storage/btree/Tuple.hpp"
#include "storage/btree/core/BTreeExclusiveIterator.hpp"
#include "storage/btree/core/BTreeGeneric.hpp"
#include "storage/btree/core/BTreeWALPayload.hpp"
#include "storage/buffer-manager/GuardedBufferFrame.hpp"
#include "utils/Defer.hpp"

#include <glog/logging.h>

#include <string>

namespace leanstore {

class LeanStore;

} // namespace leanstore

namespace leanstore::storage::btree {

// Assumptions made in this implementation:
// 1. We don't insert an already removed key
// 2. Secondary Versions contain delta
//
// Keep in mind that garbage collection may leave pages completely empty
// Missing points: FatTuple::remove, garbage leaves can escape from us
class TransactionKV : public BasicKV {
public:
  /// Graveyard to store removed tuples for long-running transactions.
  BasicKV* mGraveyard;

  TransactionKV() {
    mTreeType = BTreeType::kTransactionKV;
  }

public:
  OpCode Lookup(Slice key, ValCallback valCallback) override;

  OpCode ScanAsc(Slice startKey, ScanCallback) override;

  OpCode ScanDesc(Slice startKey, ScanCallback) override;

  OpCode Insert(Slice key, Slice val) override;

  OpCode UpdatePartial(Slice key, MutValCallback updateCallBack,
                       UpdateDesc& updateDesc) override;

  OpCode Remove(Slice key) override;

  void Init(leanstore::LeanStore* store, TREEID treeId, BTreeConfig config,
            BasicKV* graveyard);

  SpaceCheckResult CheckSpaceUtilization(BufferFrame& bf) override;

  // This undo implementation works only for rollback and not for undo
  // operations during recovery
  void undo(const u8* walEntryPtr, const u64) override;

  void GarbageCollect(const u8* entryPtr, WORKERID versionWorkerId,
                      TXID versionTxId, bool calledBefore) override;

  void unlock(const u8* walEntryPtr) override;

private:
  template <bool asc = true>
  OpCode scan4ShortRunningTx(Slice key, ScanCallback callback);

  template <bool asc = true>
  OpCode scan4LongRunningTx(Slice key, ScanCallback callback);

  inline bool VisibleForMe(WORKERID workerId, TXID txId) {
    return cr::Worker::My().cc.VisibleForMe(workerId, txId);
  }

  inline static bool triggerPageWiseGarbageCollection(
      GuardedBufferFrame<BTreeNode>& guardedNode) {
    return guardedNode->mHasGarbage;
  }

  inline std::tuple<OpCode, u16> GetVisibleTuple(Slice payload,
                                                 ValCallback callback) {
    std::tuple<OpCode, u16> ret;
    SCOPED_DEFER(DCHECK(std::get<0>(ret) == OpCode::kOK ||
                        std::get<0>(ret) == OpCode::kNotFound)
                     << "GetVisibleTuple should return either OK or NotFound";);
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
          LOG(ERROR) << "Unhandled tuple format: "
                     << TupleFormatUtil::ToString(tuple->mFormat);
        }
        }
      }
      JUMPMU_CATCH() {
      }
    }
  }

  void insertAfterRemove(BTreeExclusiveIterator& xIter, Slice key, Slice val);

  void undoLastInsert(const WALTxInsert* walInsert);

  void undoLastUpdate(const WALTxUpdate* walUpdate);

  void undoLastRemove(const WALTxRemove* walRemove);

public:
  static TransactionKV* Create(leanstore::LeanStore* store,
                               const std::string& treeName, BTreeConfig& config,
                               BasicKV* graveyard);

  inline static void InsertToNode(GuardedBufferFrame<BTreeNode>& guardedNode,
                                  Slice key, Slice val, WORKERID workerId,
                                  TXID txStartTs,
                                  TxMode txMode [[maybe_unused]], s32& slotId) {
    auto totalValSize = sizeof(ChainedTuple) + val.size();
    slotId = guardedNode->insertDoNotCopyPayload(key, totalValSize, slotId);
    auto* tupleAddr = guardedNode->ValData(slotId);
    new (tupleAddr) ChainedTuple(workerId, txStartTs, val);
    guardedNode.MarkAsDirty();
  }

  inline static u64 ConvertToFatTupleThreshold() {
    return cr::Worker::My().mStore->mStoreOption.mNumTxWorkers;
  }

  /// Updates the value stored in FatTuple. The former newest version value is
  /// moved to the tail.
  /// @return false to fallback to chained mode
  static bool UpdateInFatTuple(BTreeExclusiveIterator& xIter, Slice key,
                               MutValCallback updateCallBack,
                               UpdateDesc& updateDesc);
};

} // namespace leanstore::storage::btree
