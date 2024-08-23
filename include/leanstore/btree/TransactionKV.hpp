#pragma once

#include "leanstore/KVInterface.hpp"
#include "leanstore/Units.hpp"
#include "leanstore/btree/BasicKV.hpp"
#include "leanstore/btree/ChainedTuple.hpp"
#include "leanstore/btree/Tuple.hpp"
#include "leanstore/btree/core/BTreeGeneric.hpp"
#include "leanstore/btree/core/PessimisticExclusiveIterator.hpp"
#include "leanstore/buffer-manager/GuardedBufferFrame.hpp"
#include "leanstore/concurrency/WorkerContext.hpp"
#include "leanstore/utils/Result.hpp"

#include <expected>
#include <string>
#include <tuple>

//! forward declarations
namespace leanstore {

class LeanStore;

} // namespace leanstore

//! forward declarations
namespace leanstore::storage::btree {

class WalTxInsert;
class WalTxUpdate;
class WalTxRemove;

} // namespace leanstore::storage::btree

namespace leanstore::storage::btree {

// Assumptions made in this implementation:
// 1. We don't insert an already removed key
// 2. Secondary Versions contain delta
//
// Keep in mind that garbage collection may leave pages completely empty
// Missing points: FatTuple::remove, garbage leaves can escape from us
class TransactionKV : public BasicKV {
public:
  //! Graveyard to store removed tuples for long-running transactions.
  BasicKV* mGraveyard;

  TransactionKV() {
    mTreeType = BTreeType::kTransactionKV;
  }

  OpCode Lookup(Slice key, ValCallback valCallback) override;

  OpCode ScanAsc(Slice startKey, ScanCallback) override;

  OpCode ScanDesc(Slice startKey, ScanCallback) override;

  OpCode Insert(Slice key, Slice val) override;

  OpCode UpdatePartial(Slice key, MutValCallback updateCallBack, UpdateDesc& updateDesc) override;

  OpCode Remove(Slice key) override;

  void Init(leanstore::LeanStore* store, TREEID treeId, BTreeConfig config, BasicKV* graveyard);

  SpaceCheckResult CheckSpaceUtilization(BufferFrame& bf) override;

  // This undo implementation works only for rollback and not for undo
  // operations during recovery
  void undo(const uint8_t* walEntryPtr, const uint64_t) override;

  void GarbageCollect(const uint8_t* entryPtr, WORKERID versionWorkerId, TXID versionTxId,
                      bool calledBefore) override;

  void unlock(const uint8_t* walEntryPtr) override;

private:
  OpCode lookupOptimistic(Slice key, ValCallback valCallback);

  template <bool asc = true>
  OpCode scan4ShortRunningTx(Slice key, ScanCallback callback);

  template <bool asc = true>
  OpCode scan4LongRunningTx(Slice key, ScanCallback callback);

  inline static bool triggerPageWiseGarbageCollection(GuardedBufferFrame<BTreeNode>& guardedNode) {
    return guardedNode->mHasGarbage;
  }

  std::tuple<OpCode, uint16_t> getVisibleTuple(Slice payload, ValCallback callback);

  void insertAfterRemove(PessimisticExclusiveIterator& xIter, Slice key, Slice val);

  void undoLastInsert(const WalTxInsert* walInsert);

  void undoLastUpdate(const WalTxUpdate* walUpdate);

  void undoLastRemove(const WalTxRemove* walRemove);

public:
  static Result<TransactionKV*> Create(leanstore::LeanStore* store, const std::string& treeName,
                                       BTreeConfig config, BasicKV* graveyard);

  inline static void InsertToNode(GuardedBufferFrame<BTreeNode>& guardedNode, Slice key, Slice val,
                                  WORKERID workerId, TXID txStartTs, int32_t& slotId) {
    auto totalValSize = sizeof(ChainedTuple) + val.size();
    slotId = guardedNode->InsertDoNotCopyPayload(key, totalValSize, slotId);
    auto* tupleAddr = guardedNode->ValData(slotId);
    new (tupleAddr) ChainedTuple(workerId, txStartTs, val);
  }

  inline static uint64_t ConvertToFatTupleThreshold() {
    return cr::WorkerContext::My().mStore->mStoreOption->mWorkerThreads;
  }

  //! Updates the value stored in FatTuple. The former newest version value is
  //! moved to the tail.
  //! @return false to fallback to chained mode
  static bool UpdateInFatTuple(PessimisticExclusiveIterator& xIter, Slice key,
                               MutValCallback updateCallBack, UpdateDesc& updateDesc);
};

} // namespace leanstore::storage::btree
