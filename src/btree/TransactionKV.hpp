#pragma once

#include "BasicKV.hpp"
#include "concurrency/Worker.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/Units.hpp"
#include "btree/BasicKV.hpp"
#include "btree/ChainedTuple.hpp"
#include "btree/Tuple.hpp"
#include "btree/core/BTreeGeneric.hpp"
#include "btree/core/BTreePessimisticExclusiveIterator.hpp"
#include "btree/core/BTreeWALPayload.hpp"
#include "buffer-manager/GuardedBufferFrame.hpp"

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
  void undo(const uint8_t* walEntryPtr, const uint64_t) override;

  void GarbageCollect(const uint8_t* entryPtr, WORKERID versionWorkerId,
                      TXID versionTxId, bool calledBefore) override;

  void unlock(const uint8_t* walEntryPtr) override;

private:
  template <bool asc = true>
  OpCode scan4ShortRunningTx(Slice key, ScanCallback callback);

  template <bool asc = true>
  OpCode scan4LongRunningTx(Slice key, ScanCallback callback);

  inline static bool triggerPageWiseGarbageCollection(
      GuardedBufferFrame<BTreeNode>& guardedNode) {
    return guardedNode->mHasGarbage;
  }

  inline std::tuple<OpCode, uint16_t> getVisibleTuple(Slice payload,
                                                      ValCallback callback) {
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
          LOG(ERROR) << "Unhandled tuple format: "
                     << TupleFormatUtil::ToString(tuple->mFormat);
        }
        }
      }
      JUMPMU_CATCH() {
      }
    }
  }

  void insertAfterRemove(BTreePessimisticExclusiveIterator& xIter, Slice key,
                         Slice val);

  void undoLastInsert(const WalTxInsert* walInsert);

  void undoLastUpdate(const WalTxUpdate* walUpdate);

  void undoLastRemove(const WalTxRemove* walRemove);

public:
  static TransactionKV* Create(leanstore::LeanStore* store,
                               const std::string& treeName, BTreeConfig& config,
                               BasicKV* graveyard);

  inline static void InsertToNode(GuardedBufferFrame<BTreeNode>& guardedNode,
                                  Slice key, Slice val, WORKERID workerId,
                                  TXID txStartTs,
                                  TxMode txMode [[maybe_unused]],
                                  int32_t& slotId) {
    auto totalValSize = sizeof(ChainedTuple) + val.size();
    slotId = guardedNode->insertDoNotCopyPayload(key, totalValSize, slotId);
    auto* tupleAddr = guardedNode->ValData(slotId);
    new (tupleAddr) ChainedTuple(workerId, txStartTs, val);
    guardedNode.MarkAsDirty();
  }

  inline static uint64_t ConvertToFatTupleThreshold() {
    return cr::Worker::My().mStore->mStoreOption.mNumTxWorkers;
  }

  /// Updates the value stored in FatTuple. The former newest version value is
  /// moved to the tail.
  /// @return false to fallback to chained mode
  static bool UpdateInFatTuple(BTreePessimisticExclusiveIterator& xIter,
                               Slice key, MutValCallback updateCallBack,
                               UpdateDesc& updateDesc);
};

} // namespace leanstore::storage::btree
