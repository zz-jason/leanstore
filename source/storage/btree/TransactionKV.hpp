#pragma once

#include "BasicKV.hpp"
#include "Config.hpp"
#include "KVInterface.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "storage/btree/BasicKV.hpp"
#include "storage/btree/ChainedTuple.hpp"
#include "storage/btree/Tuple.hpp"
#include "storage/btree/core/BTreeExclusiveIterator.hpp"
#include "storage/btree/core/BTreeSharedIterator.hpp"
#include "storage/btree/core/BTreeWALPayload.hpp"
#include "storage/buffer-manager/GuardedBufferFrame.hpp"
#include "utils/Defer.hpp"

#include <glog/logging.h>

#include <string>

namespace leanstore::storage::btree {

// Assumptions made in this implementation:
// 1. We don't insert an already removed key
// 2. Secondary Versions contain delta
//
// Keep in mind that garbage collection may leave pages completely empty
// Missing points: FatTuple::remove, garbage leaves can escape from us
class TransactionKV : public BasicKV {
public:
  /// Graveyard to store removed tuples for long-running transactions, e.g. OLAP
  /// transactions.
  BasicKV* mGraveyard;

  TransactionKV() {
    mTreeType = BTreeType::kTransactionKV;
  }

public:
  //---------------------------------------------------------------------------
  // KV Interfaces
  //---------------------------------------------------------------------------
  virtual OpCode Lookup(Slice key, ValCallback valCallback) override;

  virtual OpCode Insert(Slice key, Slice val) override;

  virtual OpCode UpdatePartial(Slice key, MutValCallback updateCallBack,
                               UpdateDesc& updateDesc) override;

  virtual OpCode Remove(Slice key) override;

  virtual OpCode ScanAsc(Slice startKey, ScanCallback) override;

  virtual OpCode ScanDesc(Slice startKey, ScanCallback) override;

public:
  //---------------------------------------------------------------------------
  // Object Utils
  //---------------------------------------------------------------------------
  void Init(TREEID treeId, Config config, BasicKV* graveyard) {
    this->mGraveyard = graveyard;
    BasicKV::Init(treeId, config);
  }

  virtual SpaceCheckResult checkSpaceUtilization(BufferFrame& bf) override {
    if (!FLAGS_xmerge) {
      return SpaceCheckResult::kNothing;
    }

    HybridGuard bfGuard(&bf.header.mLatch);
    bfGuard.toOptimisticOrJump();
    if (bf.page.mBTreeId != mTreeId) {
      jumpmu::Jump();
    }

    GuardedBufferFrame<BTreeNode> guardedNode(std::move(bfGuard), &bf);
    if (!guardedNode->mIsLeaf ||
        !triggerPageWiseGarbageCollection(guardedNode)) {
      return BTreeGeneric::checkSpaceUtilization(bf);
    }

    guardedNode.ToExclusiveMayJump();
    guardedNode.SyncGSNBeforeWrite();

    for (u16 i = 0; i < guardedNode->mNumSeps; i++) {
      auto& tuple = *Tuple::From(guardedNode->ValData(i));
      if (tuple.mFormat == TupleFormat::kFat) {
        auto& fatTuple = *FatTuple::From(guardedNode->ValData(i));
        const u32 newLength = fatTuple.mValSize + sizeof(ChainedTuple);
        fatTuple.ConvertToChained(mTreeId);
        DCHECK(newLength < guardedNode->ValSize(i));
        guardedNode->shortenPayload(i, newLength);
        DCHECK(tuple.mFormat == TupleFormat::kChained);
      }
    }
    guardedNode->mHasGarbage = false;
    guardedNode.unlock();

    const SpaceCheckResult result = BTreeGeneric::checkSpaceUtilization(bf);
    if (result == SpaceCheckResult::kPickAnotherBf) {
      return SpaceCheckResult::kPickAnotherBf;
    }
    return SpaceCheckResult::kRestartSameBf;
  }

  // This undo implementation works only for rollback and not for undo
  // operations during recovery
  void undo(const u8* walEntryPtr, const u64) override;

  virtual void todo(const u8* entryPtr, const u64 versionWorkerId,
                    const u64 versionTxId, const bool calledBefore) override {
    // Only point-gc and for removed tuples
    const auto& version = *reinterpret_cast<const RemoveVersion*>(entryPtr);
    if (version.mTxId < cr::Worker::my().cc.local_all_lwm) {
      DCHECK(version.mDanglingPointer.mBf != nullptr);
      // Optimistic fast path
      JUMPMU_TRY() {
        BTreeExclusiveIterator xIter(
            *static_cast<BTreeGeneric*>(this), version.mDanglingPointer.mBf,
            version.mDanglingPointer.mLatchVersionShouldBe);
        auto& node = xIter.mGuardedLeaf;
        auto& chainedTuple = *ChainedTuple::From(
            node->ValData(version.mDanglingPointer.mHeadSlot));
        // Being chained is implicit because we check for version, so the state
        // can not be changed after staging the todo
        ENSURE(chainedTuple.mFormat == TupleFormat::kChained &&
               !chainedTuple.IsWriteLocked() &&
               chainedTuple.mWorkerId == versionWorkerId &&
               chainedTuple.mTxId == versionTxId && chainedTuple.mIsRemoved);
        node->removeSlot(version.mDanglingPointer.mHeadSlot);
        xIter.MarkAsDirty();
        xIter.mergeIfNeeded();
        JUMPMU_RETURN;
      }
      JUMPMU_CATCH() {
      }
    }
    auto key = version.RemovedKey();
    OpCode ret;
    if (calledBefore) {
      // Delete from mGraveyard
      // ENSURE(version_tx_id < cr::Worker::my().local_all_lwm);
      JUMPMU_TRY() {
        BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(mGraveyard));
        if (xIter.SeekExact(key)) {
          ret = xIter.removeCurrent();
          ENSURE(ret == OpCode::kOK);
          xIter.MarkAsDirty();
        } else {
          UNREACHABLE();
        }
      }
      JUMPMU_CATCH() {
      }
      return;
    }
    // TODO: Corner cases if the tuple got inserted after a remove
    JUMPMU_TRY() {
      BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
      if (!xIter.SeekExact(key)) {
        JUMPMU_RETURN; // TODO:
      }
      // ENSURE(ret == OpCode::kOK);
      MutableSlice mutRawVal = xIter.MutableVal();
      // Checks
      auto& tuple = *Tuple::From(mutRawVal.Data());
      if (tuple.mFormat == TupleFormat::kFat) {
        JUMPMU_RETURN;
      }
      ChainedTuple& chainedTuple = *ChainedTuple::From(mutRawVal.Data());
      if (!chainedTuple.IsWriteLocked()) {
        if (chainedTuple.mWorkerId == versionWorkerId &&
            chainedTuple.mTxId == versionTxId && chainedTuple.mIsRemoved) {
          if (chainedTuple.mTxId < cr::Worker::my().cc.local_all_lwm) {
            ret = xIter.removeCurrent();
            xIter.MarkAsDirty();
            ENSURE(ret == OpCode::kOK);
            xIter.mergeIfNeeded();
            COUNTERS_BLOCK() {
              WorkerCounters::MyCounters().cc_todo_removed[mTreeId]++;
            }
          } else if (chainedTuple.mTxId < cr::Worker::my().cc.local_oltp_lwm) {
            // Move to mGraveyard
            {
              BTreeExclusiveIterator graveyardXIter(
                  *static_cast<BTreeGeneric*>(mGraveyard));
              OpCode g_ret = graveyardXIter.InsertKV(key, xIter.value());
              ENSURE(g_ret == OpCode::kOK);
              graveyardXIter.MarkAsDirty();
            }
            ret = xIter.removeCurrent();
            ENSURE(ret == OpCode::kOK);
            xIter.MarkAsDirty();
            xIter.mergeIfNeeded();
            COUNTERS_BLOCK() {
              WorkerCounters::MyCounters().cc_todo_moved_gy[mTreeId]++;
            }
          } else {
            UNREACHABLE();
          }
        }
      }
    }
    JUMPMU_CATCH() {
      UNREACHABLE();
    }
  }

  virtual void unlock(const u8* walEntryPtr) override {
    const WALPayload& entry = *reinterpret_cast<const WALPayload*>(walEntryPtr);
    Slice key;
    switch (entry.mType) {
    case WALPayload::TYPE::WALInsert: {
      // Assuming no insert after remove
      auto& walInsert = *reinterpret_cast<const WALInsert*>(&entry);
      key = walInsert.GetKey();
      break;
    }
    case WALPayload::TYPE::WALTxUpdate: {
      auto& walUpdate = *reinterpret_cast<const WALTxUpdate*>(&entry);
      key = walUpdate.GetKey();
      break;
    }
    case WALPayload::TYPE::WALTxRemove: {
      auto& removeEntry = *reinterpret_cast<const WALTxRemove*>(&entry);
      key = removeEntry.RemovedKey();
      break;
    }
    default: {
      return;
      break;
    }
    }

    JUMPMU_TRY() {
      BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
      auto succeed = xIter.SeekExact(key);
      DCHECK(succeed) << "Can not find key in the BTree"
                      << ", key=" << std::string((char*)key.data(), key.size());
      auto& tuple = *Tuple::From(xIter.MutableVal().Data());
      ENSURE(tuple.mFormat == TupleFormat::kChained);
      /**
       * The major work is in traversing the tree:
       *
       * if (tuple.mTxId == cr::ActiveTx().mStartTs &&
       *     tuple.mWorkerId == cr::Worker::my().mWorkerId) {
       *   auto& chainedTuple =
       *       *reinterpret_cast<ChainedTuple*>(xIter.MutableVal().data());
       *   chainedTuple.mCommitTs = cr::ActiveTx().mCommitTs | kMsb;
       * }
       */
    }
    JUMPMU_CATCH() {
      UNREACHABLE();
    }
  }

private:
  template <bool asc = true> OpCode scan(Slice key, ScanCallback callback) {
    // TODO: index range lock for serializability
    COUNTERS_BLOCK() {
      if (asc) {
        WorkerCounters::MyCounters().dt_scan_asc[mTreeId]++;
      } else {
        WorkerCounters::MyCounters().dt_scan_desc[mTreeId]++;
      }
    }

    bool keepScanning = true;
    JUMPMU_TRY() {
      BTreeSharedIterator iter(*static_cast<BTreeGeneric*>(this),
                               LatchMode::kShared);

      bool succeed = asc ? iter.Seek(key) : iter.SeekForPrev(key);
      while (succeed) {
        iter.AssembleKey();
        Slice scannedKey = iter.key();
        // auto reconstruct = GetVisibleTuple(iter.value(), [&](Slice
        // scannedVal) {
        auto [opCode, versionsRead] =
            GetVisibleTuple(iter.value(), [&](Slice scannedVal) {
              COUNTERS_BLOCK() {
                WorkerCounters::MyCounters().dt_scan_callback[mTreeId] +=
                    cr::ActiveTx().IsOLAP();
              }
              keepScanning = callback(scannedKey, scannedVal);
            });
        COUNTERS_BLOCK() {
          WorkerCounters::MyCounters().cc_read_chains[mTreeId]++;
          WorkerCounters::MyCounters().cc_read_versions_visited[mTreeId] +=
              versionsRead;
          if (opCode != OpCode::kOK) {
            WorkerCounters::MyCounters().cc_read_chains_not_found[mTreeId]++;
            WorkerCounters::MyCounters()
                .cc_read_versions_visited_not_found[mTreeId] += versionsRead;
          }
        }
        if (!keepScanning) {
          JUMPMU_RETURN OpCode::kOK;
        }

        succeed = asc ? iter.Next() : iter.Prev();
      }
      JUMPMU_RETURN OpCode::kOK;
    }
    JUMPMU_CATCH() {
      ENSURE(false);
    }
    UNREACHABLE();
    JUMPMU_RETURN OpCode::kOther;
  }

  // TODO: atm, only ascending
  template <bool asc = true> OpCode scanOLAP(Slice key, ScanCallback callback) {
    volatile bool keepScanning = true;

    JUMPMU_TRY() {
      BTreeSharedIterator iter(*static_cast<BTreeGeneric*>(this));
      OpCode o_ret;

      BTreeSharedIterator graveyardIter(
          *static_cast<BTreeGeneric*>(mGraveyard));
      OpCode g_ret;
      Slice graveyardLowerBound, graveyardUpperBound;
      graveyardLowerBound = key;

      if (!iter.Seek(key)) {
        JUMPMU_RETURN OpCode::kOK;
      }
      o_ret = OpCode::kOK;
      iter.AssembleKey();

      // Now it begins
      graveyardUpperBound = Slice(iter.mGuardedLeaf->getUpperFenceKey(),
                                  iter.mGuardedLeaf->mUpperFence.length);
      auto g_range = [&]() {
        graveyardIter.Reset();
        if (mGraveyard->IsRangeEmpty(graveyardLowerBound,
                                     graveyardUpperBound)) {
          g_ret = OpCode::kOther;
          return;
        }
        if (!graveyardIter.Seek(graveyardLowerBound)) {
          g_ret = OpCode::kNotFound;
          return;
        }

        graveyardIter.AssembleKey();
        if (graveyardIter.key() > graveyardUpperBound) {
          g_ret = OpCode::kOther;
          graveyardIter.Reset();
          return;
        }

        g_ret = OpCode::kOK;
      };

      g_range();
      auto take_from_oltp = [&]() {
        GetVisibleTuple(iter.value(), [&](Slice value) {
          COUNTERS_BLOCK() {
            WorkerCounters::MyCounters().dt_scan_callback[mTreeId] +=
                cr::ActiveTx().IsOLAP();
          }
          keepScanning = callback(iter.key(), value);
        });
        if (!keepScanning) {
          return false;
        }
        const bool is_last_one = iter.IsLastOne();
        if (is_last_one) {
          graveyardIter.Reset();
        }
        o_ret = iter.Next() ? OpCode::kOK : OpCode::kNotFound;
        if (is_last_one) {
          graveyardLowerBound = Slice(&iter.mBuffer[0], iter.mFenceSize + 1);
          graveyardUpperBound = Slice(iter.mGuardedLeaf->getUpperFenceKey(),
                                      iter.mGuardedLeaf->mUpperFence.length);
          g_range();
        }
        return true;
      };
      while (true) {
        if (g_ret != OpCode::kOK && o_ret == OpCode::kOK) {
          iter.AssembleKey();
          if (!take_from_oltp()) {
            JUMPMU_RETURN OpCode::kOK;
          }
        } else if (g_ret == OpCode::kOK && o_ret != OpCode::kOK) {
          graveyardIter.AssembleKey();
          Slice g_key = graveyardIter.key();
          GetVisibleTuple(graveyardIter.value(), [&](Slice value) {
            COUNTERS_BLOCK() {
              WorkerCounters::MyCounters().dt_scan_callback[mTreeId] +=
                  cr::ActiveTx().IsOLAP();
            }
            keepScanning = callback(g_key, value);
          });
          if (!keepScanning) {
            JUMPMU_RETURN OpCode::kOK;
          }
          g_ret = graveyardIter.Next() ? OpCode::kOK : OpCode::kNotFound;
        } else if (g_ret == OpCode::kOK && o_ret == OpCode::kOK) {
          iter.AssembleKey();
          graveyardIter.AssembleKey();
          Slice g_key = graveyardIter.key();
          Slice oltp_key = iter.key();
          if (oltp_key <= g_key) {
            if (!take_from_oltp()) {
              JUMPMU_RETURN OpCode::kOK;
            }
          } else {
            GetVisibleTuple(graveyardIter.value(), [&](Slice value) {
              COUNTERS_BLOCK() {
                WorkerCounters::MyCounters().dt_scan_callback[mTreeId] +=
                    cr::ActiveTx().IsOLAP();
              }
              keepScanning = callback(g_key, value);
            });
            if (!keepScanning) {
              JUMPMU_RETURN OpCode::kOK;
            }
            g_ret = graveyardIter.Next() ? OpCode::kOK : OpCode::kNotFound;
          }
        } else {
          JUMPMU_RETURN OpCode::kOK;
        }
      }
    }
    JUMPMU_CATCH() {
      ENSURE(false);
    }
    UNREACHABLE();
    JUMPMU_RETURN OpCode::kOther;
  }

  inline bool VisibleForMe(WORKERID workerId, TXID txId) {
    return cr::Worker::my().cc.VisibleForMe(workerId, txId);
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

  void undoLastInsert(const WALInsert* walInsert);

  void undoLastUpdate(const WALTxUpdate* walUpdate);

  void undoLastRemove(const WALTxRemove* walRemove);

public:
  inline static TransactionKV* Create(const std::string& treeName,
                                      Config& config, BasicKV* graveyard) {
    auto [treePtr, treeId] =
        TreeRegistry::sInstance->CreateTree(treeName, [&]() {
          return std::unique_ptr<BufferManagedTree>(
              static_cast<BufferManagedTree*>(new TransactionKV()));
        });
    if (treePtr == nullptr) {
      LOG(ERROR) << "Failed to create TransactionKV, treeName has been taken"
                 << ", treeName=" << treeName;
      return nullptr;
    }
    auto* tree = dynamic_cast<TransactionKV*>(treePtr);
    tree->Init(treeId, config, graveyard);

    // TODO(jian.z): record WAL
    return tree;
  }

  inline static void InsertToNode(GuardedBufferFrame<BTreeNode>& guardedNode,
                                  Slice key, Slice val, WORKERID workerId,
                                  TXID txStartTs, TxMode txMode, s32& slotId) {
    auto totalValSize = sizeof(ChainedTuple) + val.size();
    slotId = guardedNode->insertDoNotCopyPayload(key, totalValSize, slotId);
    auto* tupleAddr = guardedNode->ValData(slotId);
    auto* tuple = new (tupleAddr) ChainedTuple(workerId, txStartTs, val);
    if (txMode == TxMode::kInstantlyVisibleBulkInsert) {
      tuple->mTxId = kMsb | 0;
    }
    guardedNode.MarkAsDirty();
  }

  inline static u64 ConvertToFatTupleThreshold() {
    return FLAGS_worker_threads;
  }

  /// Updates the value stored in FatTuple. The former newest version value is
  /// moved to the tail of the delta array.
  /// @return false to fallback to chained mode
  static bool UpdateInFatTuple(BTreeExclusiveIterator& xIter, Slice key,
                               MutValCallback updateCallBack,
                               UpdateDesc& updateDesc);
};

} // namespace leanstore::storage::btree
