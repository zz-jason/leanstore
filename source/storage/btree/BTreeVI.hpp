#pragma once

#include "BTreeLL.hpp"
#include "Config.hpp"
#include "Tuple.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "storage/btree/core/BTreeExclusiveIterator.hpp"
#include "storage/btree/core/BTreeSharedIterator.hpp"
#include "storage/btree/core/BTreeWALPayload.hpp"
#include "storage/buffer-manager/GuardedBufferFrame.hpp"
#include "utils/Defer.hpp"

#include <glog/logging.h>

namespace leanstore {
namespace storage {
namespace btree {

class BTreeVI : public BTreeLL {
public:
  struct WALRemove : WALPayload {
    u16 mKeySize;

    u16 mValSize;

    WORKERID mPrevWorkerId;

    TXID mPrevTxId;

    COMMANDID mPrevCommandId;

    u8 payload[];

    WALRemove(Slice key, Slice val, WORKERID prevWorkerId, u64 prevTxId,
              u64 prevCommandId)
        : WALPayload(TYPE::WALRemove),
          mKeySize(key.size()),
          mValSize(val.size()),
          mPrevWorkerId(prevWorkerId),
          mPrevTxId(prevTxId),
          mPrevCommandId(prevCommandId) {
      std::memcpy(payload, key.data(), key.size());
      std::memcpy(payload + key.size(), val.data(), val.size());
    }

    Slice RemovedKey() const {
      return Slice(payload, mKeySize);
    }

    Slice RemovedVal() const {
      return Slice(payload + mKeySize, mValSize);
    }
  };

public:
  /// Graveyard to store removed tuples for long-running transactions, e.g. OLAP
  /// transactions.
  BTreeLL* mGraveyard;

  BTreeVI() {
    mTreeType = BTREE_TYPE::VI;
  }

public:
  //---------------------------------------------------------------------------
  // KV Interfaces
  //---------------------------------------------------------------------------
  virtual OpCode Lookup(Slice key, ValCallback valCallback) override;

  virtual OpCode insert(Slice key, Slice val) override;

  virtual OpCode updateSameSizeInPlace(Slice key, MutValCallback updateCallBack,
                                       UpdateDesc& updateDesc) override;

  virtual OpCode remove(Slice key) override;
  virtual OpCode ScanAsc(Slice startKey, ScanCallback) override;
  virtual OpCode ScanDesc(Slice startKey, ScanCallback) override;

public:
  //---------------------------------------------------------------------------
  // Object Utils
  //---------------------------------------------------------------------------
  void Init(TREEID treeId, Config config, BTreeLL* graveyard) {
    this->mGraveyard = graveyard;
    BTreeLL::Init(treeId, config);
  }

  virtual SpaceCheckResult checkSpaceUtilization(BufferFrame& bf) override {
    if (!FLAGS_xmerge) {
      return SpaceCheckResult::kNothing;
    }

    HybridGuard bfGuard(&bf.header.mLatch);
    bfGuard.toOptimisticOrJump();
    if (bf.page.mBTreeId != mTreeId) {
      jumpmu::jump();
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
      if (tuple.mFormat == TupleFormat::FAT) {
        auto& fatTuple = *FatTuple::From(guardedNode->ValData(i));
        const u32 newLength = fatTuple.mValSize + sizeof(ChainedTuple);
        fatTuple.convertToChained(mTreeId);
        DCHECK(newLength < guardedNode->ValSize(i));
        guardedNode->shortenPayload(i, newLength);
        DCHECK(tuple.mFormat == TupleFormat::CHAINED);
      }
    }
    guardedNode->mHasGarbage = false;
    guardedNode.unlock();

    const SpaceCheckResult result = BTreeGeneric::checkSpaceUtilization(bf);
    if (result == SpaceCheckResult::kPickAnotherBf) {
      return SpaceCheckResult::kPickAnotherBf;
    } else {
      return SpaceCheckResult::kRestartSameBf;
    }
  }

  // This undo implementation works only for rollback and not for undo
  // operations during recovery
  void undo(const u8* walEntryPtr, const u64) override;

  virtual void todo(const u8* entry_ptr, const u64 version_worker_id,
                    const u64 version_tx_id,
                    const bool called_before) override {
    // Only point-gc and for removed tuples
    const auto& version = *reinterpret_cast<const RemoveVersion*>(entry_ptr);
    if (version.mTxId < cr::Worker::my().cc.local_all_lwm) {
      DCHECK(version.dangling_pointer.bf != nullptr);
      // Optimistic fast path
      JUMPMU_TRY() {
        BTreeExclusiveIterator iterator(
            *static_cast<BTreeGeneric*>(this), version.dangling_pointer.bf,
            version.dangling_pointer.latch_version_should_be);
        auto& node = iterator.mGuardedLeaf;
        auto& chainedTuple = *ChainedTuple::From(
            node->ValData(version.dangling_pointer.head_slot));
        // Being chained is implicit because we check for version, so the state
        // can not be changed after staging the todo
        ENSURE(chainedTuple.mFormat == TupleFormat::CHAINED &&
               !chainedTuple.IsWriteLocked() &&
               chainedTuple.mWorkerId == version_worker_id &&
               chainedTuple.mTxId == version_tx_id && chainedTuple.mIsRemoved);
        node->removeSlot(version.dangling_pointer.head_slot);
        iterator.MarkAsDirty();
        iterator.mergeIfNeeded();
        JUMPMU_RETURN;
      }
      JUMPMU_CATCH() {
      }
    }
    Slice key(version.payload, version.mKeySize);
    OpCode ret;
    if (called_before) {
      // Delete from mGraveyard
      // ENSURE(version_tx_id < cr::Worker::my().local_all_lwm);
      JUMPMU_TRY() {
        BTreeExclusiveIterator g_iterator(
            *static_cast<BTreeGeneric*>(mGraveyard));
        ret = g_iterator.seekExact(key);
        if (ret == OpCode::kOK) {
          ret = g_iterator.removeCurrent();
          ENSURE(ret == OpCode::kOK);
          g_iterator.MarkAsDirty();
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
      ret = xIter.seekExact(key);
      if (ret != OpCode::kOK) {
        JUMPMU_RETURN; // TODO:
      }
      // ENSURE(ret == OpCode::kOK);
      MutableSlice mutRawVal = xIter.MutableVal();
      {
        // Checks
        auto& tuple = *Tuple::From(mutRawVal.Data());
        if (tuple.mFormat == TupleFormat::FAT) {
          JUMPMU_RETURN;
        }
      }
      ChainedTuple& chainedTuple = *ChainedTuple::From(mutRawVal.Data());
      if (!chainedTuple.IsWriteLocked()) {
        if (chainedTuple.mWorkerId == version_worker_id &&
            chainedTuple.mTxId == version_tx_id && chainedTuple.mIsRemoved) {
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
              BTreeExclusiveIterator g_iterator(
                  *static_cast<BTreeGeneric*>(mGraveyard));
              OpCode g_ret = g_iterator.insertKV(key, xIter.value());
              ENSURE(g_ret == OpCode::kOK);
              g_iterator.MarkAsDirty();
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
    case WALPayload::TYPE::WALRemove: {
      auto& removeEntry = *reinterpret_cast<const WALRemove*>(&entry);
      key = Slice(removeEntry.payload, removeEntry.mKeySize);
      break;
    }
    default: {
      return;
      break;
    }
    }

    JUMPMU_TRY() {
      BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
      OpCode ret = xIter.seekExact(key);
      ENSURE(ret == OpCode::kOK);
      auto& tuple = *Tuple::From(xIter.MutableVal().Data());
      ENSURE(tuple.mFormat == TupleFormat::CHAINED);
      /**
       * The major work is in traversing the tree:
       *
       * if (tuple.mTxId == cr::activeTX().mStartTs &&
       *     tuple.mWorkerId == cr::Worker::my().mWorkerId) {
       *   auto& chainedTuple =
       *       *reinterpret_cast<ChainedTuple*>(xIter.MutableVal().data());
       *   chainedTuple.mCommitTs = cr::activeTX().mCommitTs | kMsb;
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

      OpCode ret;
      if (asc) {
        ret = iter.seek(key);
      } else {
        ret = iter.seekForPrev(key);
      }

      while (ret == OpCode::kOK) {
        iter.assembleKey();
        Slice scannedKey = iter.key();
        // auto reconstruct = GetVisibleTuple(iter.value(), [&](Slice
        // scannedVal) {
        auto [opCode, versionsRead] =
            GetVisibleTuple(iter.value(), [&](Slice scannedVal) {
              COUNTERS_BLOCK() {
                WorkerCounters::MyCounters().dt_scan_callback[mTreeId] +=
                    cr::activeTX().IsOLAP();
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

        if constexpr (asc) {
          ret = iter.next();
        } else {
          ret = iter.prev();
        }
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
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      OpCode o_ret;
      BTreeSharedIterator g_iterator(*static_cast<BTreeGeneric*>(mGraveyard));
      OpCode g_ret;
      Slice g_lower_bound, g_upper_bound;
      g_lower_bound = key;

      o_ret = iterator.seek(key);
      if (o_ret != OpCode::kOK) {
        JUMPMU_RETURN OpCode::kOK;
      }
      iterator.assembleKey();

      // Now it begins
      g_upper_bound = Slice(iterator.mGuardedLeaf->getUpperFenceKey(),
                            iterator.mGuardedLeaf->mUpperFence.length);
      auto g_range = [&]() {
        g_iterator.reset();
        if (mGraveyard->isRangeSurelyEmpty(g_lower_bound, g_upper_bound)) {
          g_ret = OpCode::kOther;
        } else {
          g_ret = g_iterator.seek(g_lower_bound);
          if (g_ret == OpCode::kOK) {
            g_iterator.assembleKey();
            if (g_iterator.key() > g_upper_bound) {
              g_ret = OpCode::kOther;
              g_iterator.reset();
            }
          }
        }
      };

      g_range();
      auto take_from_oltp = [&]() {
        GetVisibleTuple(iterator.value(), [&](Slice value) {
          COUNTERS_BLOCK() {
            WorkerCounters::MyCounters().dt_scan_callback[mTreeId] +=
                cr::activeTX().IsOLAP();
          }
          keepScanning = callback(iterator.key(), value);
        });
        if (!keepScanning) {
          return false;
        }
        const bool is_last_one = iterator.isLastOne();
        if (is_last_one) {
          g_iterator.reset();
        }
        o_ret = iterator.next();
        if (is_last_one) {
          g_lower_bound = Slice(&iterator.mBuffer[0], iterator.mFenceSize + 1);
          g_upper_bound = Slice(iterator.mGuardedLeaf->getUpperFenceKey(),
                                iterator.mGuardedLeaf->mUpperFence.length);
          g_range();
        }
        return true;
      };
      while (true) {
        if (g_ret != OpCode::kOK && o_ret == OpCode::kOK) {
          iterator.assembleKey();
          if (!take_from_oltp()) {
            JUMPMU_RETURN OpCode::kOK;
          }
        } else if (g_ret == OpCode::kOK && o_ret != OpCode::kOK) {
          g_iterator.assembleKey();
          Slice g_key = g_iterator.key();
          GetVisibleTuple(g_iterator.value(), [&](Slice value) {
            COUNTERS_BLOCK() {
              WorkerCounters::MyCounters().dt_scan_callback[mTreeId] +=
                  cr::activeTX().IsOLAP();
            }
            keepScanning = callback(g_key, value);
          });
          if (!keepScanning) {
            JUMPMU_RETURN OpCode::kOK;
          }
          g_ret = g_iterator.next();
        } else if (g_ret == OpCode::kOK && o_ret == OpCode::kOK) {
          iterator.assembleKey();
          g_iterator.assembleKey();
          Slice g_key = g_iterator.key();
          Slice oltp_key = iterator.key();
          if (oltp_key <= g_key) {
            if (!take_from_oltp()) {
              JUMPMU_RETURN OpCode::kOK;
            }
          } else {
            GetVisibleTuple(g_iterator.value(), [&](Slice value) {
              COUNTERS_BLOCK() {
                WorkerCounters::MyCounters().dt_scan_callback[mTreeId] +=
                    cr::activeTX().IsOLAP();
              }
              keepScanning = callback(g_key, value);
            });
            if (!keepScanning) {
              JUMPMU_RETURN OpCode::kOK;
            }
            g_ret = g_iterator.next();
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
        case TupleFormat::CHAINED: {
          const auto* const chainedTuple = ChainedTuple::From(payload.data());
          ret = chainedTuple->GetVisibleTuple(payload, callback);
          JUMPMU_RETURN ret;
        }
        case TupleFormat::FAT: {
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

  void undoLastRemove(const WALRemove* walRemove);

public:
  inline static BTreeVI* Create(const std::string& treeName, Config& config,
                                BTreeLL* graveyard) {
    auto [treePtr, treeId] =
        TreeRegistry::sInstance->CreateTree(treeName, [&]() {
          return std::unique_ptr<BufferManagedTree>(
              static_cast<BufferManagedTree*>(new BTreeVI()));
        });
    if (treePtr == nullptr) {
      LOG(ERROR) << "Failed to create BTreeVI, treeName has been taken"
                 << ", treeName=" << treeName;
      return nullptr;
    }
    auto* tree = dynamic_cast<BTreeVI*>(treePtr);
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

  inline static u64 convertToFatTupleThreshold() {
    return FLAGS_worker_threads;
  }
};

} // namespace btree
} // namespace storage
} // namespace leanstore
