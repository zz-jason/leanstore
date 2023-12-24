#pragma once

#include "BTreeLL.hpp"
#include "Config.hpp"
#include "Tuple.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "storage/btree/core/BTreeExclusiveIterator.hpp"
#include "storage/btree/core/BTreeSharedIterator.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "storage/buffer-manager/GuardedBufferFrame.hpp"
#include "utils/RandomGenerator.hpp"

#include <glog/logging.h>

#include <set>

namespace leanstore {
namespace storage {
namespace btree {

struct __attribute__((packed)) DanglingPointer {
  BufferFrame* bf = nullptr;
  u64 latch_version_should_be = -1;
  s32 head_slot = -1;
};

struct __attribute__((packed)) Version {
  enum class TYPE : u8 { UPDATE, REMOVE };

  TYPE type;

  WORKERID mWorkerId;

  TXID mTxId;

  COMMANDID mCommandId;

  Version(TYPE type, WORKERID workerId, TXID txId, COMMANDID commandId)
      : type(type), mWorkerId(workerId), mTxId(txId), mCommandId(commandId) {
  }
};

struct __attribute__((packed)) UpdateVersion : Version {
  u8 is_delta : 1;
  u8 payload[]; // UpdateDescriptor + Diff

  UpdateVersion(WORKERID workerId, TXID txId, COMMANDID commandId, bool isDelta)
      : Version(Version::TYPE::UPDATE, workerId, txId, commandId),
        is_delta(isDelta) {
  }

  bool isFinal() const {
    return mCommandId == 0;
  }
};

struct __attribute__((packed)) RemoveVersion : Version {
  u16 mKeySize;
  u16 mValSize;
  DanglingPointer dangling_pointer;
  bool moved_to_graveway = false;
  u8 payload[]; // Key + Value
  RemoveVersion(WORKERID workerId, TXID txId, COMMANDID commandId, u16 keySize,
                u16 valSize)
      : Version(Version::TYPE::REMOVE, workerId, txId, commandId),
        mKeySize(keySize), mValSize(valSize) {
  }
};

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
        : WALPayload(TYPE::WALRemove), mKeySize(key.size()),
          mValSize(val.size()), mPrevWorkerId(prevWorkerId),
          mPrevTxId(prevTxId), mPrevCommandId(prevCommandId) {
      std::memcpy(payload, key.data(), key.size());
      std::memcpy(payload + key.size(), val.data(), val.size());
    }
  };

public:
  //---------------------------------------------------------------------------
  // Member fields
  //---------------------------------------------------------------------------
  BTreeLL* mGraveyard;

  BTreeVI() {
    mTreeType = BTREE_TYPE::VI;
  }

public:
  //---------------------------------------------------------------------------
  // KV Interfaces
  //---------------------------------------------------------------------------
  virtual OP_RESULT Lookup(Slice key, ValCallback valCallback) override;
  virtual OP_RESULT insert(Slice key, Slice val) override;
  virtual OP_RESULT updateSameSizeInPlace(
      Slice key, ValCallback valCallback,
      UpdateSameSizeInPlaceDescriptor&) override;
  virtual OP_RESULT remove(Slice key) override;
  virtual OP_RESULT scanAsc(Slice startKey, ScanCallback) override;
  virtual OP_RESULT scanDesc(Slice startKey, ScanCallback) override;

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
      return SpaceCheckResult::NOTHING;
    }

    if (!FLAGS_vi_fat_tuple_decompose) {
      return BTreeGeneric::checkSpaceUtilization(bf);
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
    guardedNode.IncPageGSN();

    for (u16 i = 0; i < guardedNode->mNumSeps; i++) {
      auto& tuple = *reinterpret_cast<Tuple*>(guardedNode->ValData(i));
      if (tuple.mFormat == TupleFormat::FAT) {
        auto& fatTuple = *reinterpret_cast<FatTuple*>(guardedNode->ValData(i));
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
    if (result == SpaceCheckResult::PICK_ANOTHER_BF) {
      return SpaceCheckResult::PICK_ANOTHER_BF;
    } else {
      return SpaceCheckResult::RESTART_SAME_BF;
    }
  }

  // This undo implementation works only for rollback and not for undo
  // operations during recovery
  virtual void undo(const u8* walEntryPtr, const u64) override {
    const WALPayload& entry = *reinterpret_cast<const WALPayload*>(walEntryPtr);
    switch (entry.type) {
    case WALPayload::TYPE::WALInsert: {
      // Assuming no insert after remove
      auto& insert_entry = *reinterpret_cast<const WALInsert*>(&entry);
      JUMPMU_TRY() {
        Slice key(insert_entry.payload, insert_entry.mKeySize);
        BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
        OP_RESULT ret = iterator.seekExact(key);
        ENSURE(ret == OP_RESULT::OK);
        ret = iterator.removeCurrent();
        ENSURE(ret == OP_RESULT::OK);
        iterator.MarkAsDirty(); // TODO: write CLS
        iterator.mergeIfNeeded();
      }
      JUMPMU_CATCH() {
      }
      break;
    }
    case WALPayload::TYPE::WALUpdate: {
      auto& update_entry = *reinterpret_cast<const WALUpdateSSIP*>(&entry);
      JUMPMU_TRY() {
        Slice key(update_entry.payload, update_entry.mKeySize);
        BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
        OP_RESULT ret = iterator.seekExact(key);
        ENSURE(ret == OP_RESULT::OK);
        auto& tuple = *reinterpret_cast<Tuple*>(iterator.MutableVal().data());
        ENSURE(!tuple.IsWriteLocked());
        if (tuple.mFormat == TupleFormat::FAT) {
          reinterpret_cast<FatTuple*>(iterator.MutableVal().data())
              ->undoLastUpdate();
        } else {
          auto& chain_head =
              *reinterpret_cast<ChainedTuple*>(iterator.MutableVal().data());
          chain_head.mWorkerId = update_entry.mPrevWorkerId;
          chain_head.mTxId = update_entry.mPrevTxId;
          chain_head.mCommandId = update_entry.mPrevCommandId;
          const auto& update_descriptor =
              *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(
                  update_entry.payload + update_entry.mKeySize);
          BTreeLL::applyXORDiff(update_descriptor, chain_head.payload,
                                update_entry.payload + update_entry.mKeySize +
                                    update_descriptor.size());
        }
        iterator.MarkAsDirty();
        JUMPMU_RETURN;
      }
      JUMPMU_CATCH() {
        UNREACHABLE();
      }
      break;
    }
    case WALPayload::TYPE::WALRemove: {
      auto& removeEntry = *reinterpret_cast<const WALRemove*>(&entry);
      Slice key(removeEntry.payload, removeEntry.mKeySize);
      JUMPMU_TRY() {
        BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
        OP_RESULT ret = iterator.seekExact(key);
        ENSURE(ret == OP_RESULT::OK);
        // Resize
        const u16 new_primary_payload_length =
            removeEntry.mValSize + sizeof(ChainedTuple);
        const Slice old_primary_payload = iterator.value();
        if (old_primary_payload.length() < new_primary_payload_length) {
          const bool did_extend =
              iterator.extendPayload(new_primary_payload_length);
          ENSURE(did_extend);
        } else {
          iterator.shorten(new_primary_payload_length);
        }
        MutableSlice primaryPayload = iterator.MutableVal();
        auto& primaryVersion = *new (primaryPayload.data()) ChainedTuple(
            removeEntry.mPrevWorkerId, removeEntry.mPrevTxId,
            Slice(removeEntry.payload + removeEntry.mKeySize,
                  removeEntry.mValSize));
        primaryVersion.mCommandId = removeEntry.mPrevCommandId;
        ENSURE(primaryVersion.is_removed == false);
        primaryVersion.WriteUnlock();
        iterator.MarkAsDirty();
      }
      JUMPMU_CATCH() {
        UNREACHABLE();
      }
      break;
    }
    default: {
      break;
    }
    }
  }

  virtual void todo(const u8* entry_ptr, const u64 version_worker_id,
                    const u64 version_tx_id,
                    const bool called_before) override {
    // Only point-gc and for removed tuples
    const auto& version = *reinterpret_cast<const RemoveVersion*>(entry_ptr);
    if (FLAGS_vi_dangling_pointer &&
        version.mTxId < cr::Worker::my().cc.local_all_lwm) {
      assert(version.dangling_pointer.bf != nullptr);
      // Optimistic fast path
      JUMPMU_TRY() {
        BTreeExclusiveIterator iterator(
            *static_cast<BTreeGeneric*>(this), version.dangling_pointer.bf,
            version.dangling_pointer.latch_version_should_be);
        auto& node = iterator.mGuardedLeaf;
        auto& head = *reinterpret_cast<ChainedTuple*>(
            node->ValData(version.dangling_pointer.head_slot));
        // Being chained is implicit because we check for version, so the state
        // can not be changed after staging the todo
        ENSURE(head.mFormat == TupleFormat::CHAINED && !head.IsWriteLocked() &&
               head.mWorkerId == version_worker_id &&
               head.mTxId == version_tx_id && head.is_removed);
        node->removeSlot(version.dangling_pointer.head_slot);
        iterator.MarkAsDirty();
        iterator.mergeIfNeeded();
        JUMPMU_RETURN;
      }
      JUMPMU_CATCH() {
      }
    }
    Slice key(version.payload, version.mKeySize);
    OP_RESULT ret;
    if (called_before) {
      // Delete from mGraveyard
      // ENSURE(version_tx_id < cr::Worker::my().local_all_lwm);
      JUMPMU_TRY() {
        BTreeExclusiveIterator g_iterator(
            *static_cast<BTreeGeneric*>(mGraveyard));
        ret = g_iterator.seekExact(key);
        if (ret == OP_RESULT::OK) {
          ret = g_iterator.removeCurrent();
          ENSURE(ret == OP_RESULT::OK);
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
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
        JUMPMU_RETURN; // TODO:
      }
      // ENSURE(ret == OP_RESULT::OK);
      MutableSlice primaryPayload = iterator.MutableVal();
      {
        // Checks
        const auto& tuple =
            *reinterpret_cast<const Tuple*>(primaryPayload.data());
        if (tuple.mFormat == TupleFormat::FAT) {
          JUMPMU_RETURN;
        }
      }
      ChainedTuple& primaryVersion =
          *reinterpret_cast<ChainedTuple*>(primaryPayload.data());
      if (!primaryVersion.IsWriteLocked()) {
        if (primaryVersion.mWorkerId == version_worker_id &&
            primaryVersion.mTxId == version_tx_id &&
            primaryVersion.is_removed) {
          if (primaryVersion.mTxId < cr::Worker::my().cc.local_all_lwm) {
            ret = iterator.removeCurrent();
            iterator.MarkAsDirty();
            ENSURE(ret == OP_RESULT::OK);
            iterator.mergeIfNeeded();
            COUNTERS_BLOCK() {
              WorkerCounters::myCounters().cc_todo_removed[mTreeId]++;
            }
          } else if (primaryVersion.mTxId <
                     cr::Worker::my().cc.local_oltp_lwm) {
            // Move to mGraveyard
            {
              BTreeExclusiveIterator g_iterator(
                  *static_cast<BTreeGeneric*>(mGraveyard));
              OP_RESULT g_ret = g_iterator.insertKV(key, iterator.value());
              ENSURE(g_ret == OP_RESULT::OK);
              g_iterator.MarkAsDirty();
            }
            ret = iterator.removeCurrent();
            ENSURE(ret == OP_RESULT::OK);
            iterator.MarkAsDirty();
            iterator.mergeIfNeeded();
            COUNTERS_BLOCK() {
              WorkerCounters::myCounters().cc_todo_moved_gy[mTreeId]++;
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
    switch (entry.type) {
    case WALPayload::TYPE::WALInsert: {
      // Assuming no insert after remove
      auto& insert_entry = *reinterpret_cast<const WALInsert*>(&entry);
      key = Slice(insert_entry.payload, insert_entry.mKeySize);
      break;
    }
    case WALPayload::TYPE::WALUpdate: {
      auto& update_entry = *reinterpret_cast<const WALUpdateSSIP*>(&entry);
      key = Slice(update_entry.payload, update_entry.mKeySize);
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
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      OP_RESULT ret = iterator.seekExact(key);
      ENSURE(ret == OP_RESULT::OK);
      auto& tuple = *reinterpret_cast<Tuple*>(iterator.MutableVal().data());
      ENSURE(tuple.mFormat == TupleFormat::CHAINED);
      /**
       * The major work is in traversing the tree:
       *
       * if (tuple.mTxId == cr::activeTX().startTS() &&
       *     tuple.mWorkerId == cr::Worker::my().mWorkerId) {
       *   auto& chain_head =
       *       *reinterpret_cast<ChainedTuple*>(iterator.MutableVal().data());
       *   chain_head.mCommitTs = cr::activeTX().commitTS() | MSB;
       * }
       */
    }
    JUMPMU_CATCH() {
      UNREACHABLE();
    }
  }

private:
  bool convertChainedToFatTuple(BTreeExclusiveIterator& iterator);

  OP_RESULT lookupPessimistic(Slice key, ValCallback valCallback);
  OP_RESULT lookupOptimistic(Slice key, ValCallback valCallback);

  template <bool asc = true> OP_RESULT scan(Slice key, ScanCallback callback) {
    // TODO: index range lock for serializability
    COUNTERS_BLOCK() {
      if (asc) {
        WorkerCounters::myCounters().dt_scan_asc[mTreeId]++;
      } else {
        WorkerCounters::myCounters().dt_scan_desc[mTreeId]++;
      }
    }
    u64 counter = 0;
    volatile bool keep_scanning = true;

    JUMPMU_TRY() {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this),
                                   LATCH_FALLBACK_MODE::SHARED);

      OP_RESULT ret;
      if (asc) {
        ret = iterator.seek(key);
      } else {
        ret = iterator.seekForPrev(key);
      }
      // -------------------------------------------------------------------------------------
      while (ret == OP_RESULT::OK) {
        iterator.assembleKey();
        Slice s_key = iterator.key();
        auto reconstruct =
            GetVisibleTuple(s_key, iterator.value(), [&](Slice value) {
              COUNTERS_BLOCK() {
                WorkerCounters::myCounters().dt_scan_callback[mTreeId] +=
                    cr::activeTX().isOLAP();
              }
              keep_scanning = callback(s_key, value);
              counter++;
            });
        const u16 chain_length = std::get<1>(reconstruct);
        COUNTERS_BLOCK() {
          WorkerCounters::myCounters().cc_read_chains[mTreeId]++;
          WorkerCounters::myCounters().cc_read_versions_visited[mTreeId] +=
              chain_length;
          if (std::get<0>(reconstruct) != OP_RESULT::OK) {
            WorkerCounters::myCounters().cc_read_chains_not_found[mTreeId]++;
            WorkerCounters::myCounters()
                .cc_read_versions_visited_not_found[mTreeId] += chain_length;
          }
        }
        if (!keep_scanning) {
          JUMPMU_RETURN OP_RESULT::OK;
        }

        if constexpr (asc) {
          ret = iterator.next();
        } else {
          ret = iterator.prev();
        }
      }
      JUMPMU_RETURN OP_RESULT::OK;
    }
    JUMPMU_CATCH() {
      ENSURE(false);
    }
    UNREACHABLE();
    JUMPMU_RETURN OP_RESULT::OTHER;
  }

  // TODO: atm, only ascending
  template <bool asc = true>
  OP_RESULT scanOLAP(Slice key, ScanCallback callback) {
    volatile bool keep_scanning = true;

    JUMPMU_TRY() {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      OP_RESULT o_ret;
      BTreeSharedIterator g_iterator(*static_cast<BTreeGeneric*>(mGraveyard));
      OP_RESULT g_ret;
      Slice g_lower_bound, g_upper_bound;
      g_lower_bound = key;

      o_ret = iterator.seek(key);
      if (o_ret != OP_RESULT::OK) {
        JUMPMU_RETURN OP_RESULT::OK;
      }
      iterator.assembleKey();

      // Now it begins
      g_upper_bound = Slice(iterator.mGuardedLeaf->getUpperFenceKey(),
                            iterator.mGuardedLeaf->mUpperFence.length);
      auto g_range = [&]() {
        g_iterator.reset();
        if (mGraveyard->isRangeSurelyEmpty(g_lower_bound, g_upper_bound)) {
          g_ret = OP_RESULT::OTHER;
        } else {
          g_ret = g_iterator.seek(g_lower_bound);
          if (g_ret == OP_RESULT::OK) {
            g_iterator.assembleKey();
            if (g_iterator.key() > g_upper_bound) {
              g_ret = OP_RESULT::OTHER;
              g_iterator.reset();
            }
          }
        }
      };

      g_range();
      auto take_from_oltp = [&]() {
        GetVisibleTuple(iterator.key(), iterator.value(), [&](Slice value) {
          COUNTERS_BLOCK() {
            WorkerCounters::myCounters().dt_scan_callback[mTreeId] +=
                cr::activeTX().isOLAP();
          }
          keep_scanning = callback(iterator.key(), value);
        });
        if (!keep_scanning) {
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
        if (g_ret != OP_RESULT::OK && o_ret == OP_RESULT::OK) {
          iterator.assembleKey();
          if (!take_from_oltp()) {
            JUMPMU_RETURN OP_RESULT::OK;
          }
        } else if (g_ret == OP_RESULT::OK && o_ret != OP_RESULT::OK) {
          g_iterator.assembleKey();
          Slice g_key = g_iterator.key();
          GetVisibleTuple(g_key, g_iterator.value(), [&](Slice value) {
            COUNTERS_BLOCK() {
              WorkerCounters::myCounters().dt_scan_callback[mTreeId] +=
                  cr::activeTX().isOLAP();
            }
            keep_scanning = callback(g_key, value);
          });
          if (!keep_scanning) {
            JUMPMU_RETURN OP_RESULT::OK;
          }
          g_ret = g_iterator.next();
        } else if (g_ret == OP_RESULT::OK && o_ret == OP_RESULT::OK) {
          iterator.assembleKey();
          g_iterator.assembleKey();
          Slice g_key = g_iterator.key();
          Slice oltp_key = iterator.key();
          if (oltp_key <= g_key) {
            if (!take_from_oltp()) {
              JUMPMU_RETURN OP_RESULT::OK;
            }
          } else {
            GetVisibleTuple(g_key, g_iterator.value(), [&](Slice value) {
              COUNTERS_BLOCK() {
                WorkerCounters::myCounters().dt_scan_callback[mTreeId] +=
                    cr::activeTX().isOLAP();
              }
              keep_scanning = callback(g_key, value);
            });
            if (!keep_scanning) {
              JUMPMU_RETURN OP_RESULT::OK;
            }
            g_ret = g_iterator.next();
          }
        } else {
          JUMPMU_RETURN OP_RESULT::OK;
        }
      }
    }
    JUMPMU_CATCH() {
      ENSURE(false);
    }
    UNREACHABLE();
    JUMPMU_RETURN OP_RESULT::OTHER;
  }

  inline bool VisibleForMe(WORKERID workerId, TXID txId, bool toWrite = true) {
    return cr::Worker::my().cc.VisibleForMe(workerId, txId, toWrite);
  }

  static inline bool triggerPageWiseGarbageCollection(
      GuardedBufferFrame<BTreeNode>& guardedNode) {
    return guardedNode->mHasGarbage;
  }

  u64 convertToFatTupleThreshold() {
    return FLAGS_worker_threads;
  }

  inline std::tuple<OP_RESULT, u16> GetVisibleTuple(Slice key, Slice payload,
                                                    ValCallback callback) {
    while (true) {
      JUMPMU_TRY() {
        const auto& tuple = *Tuple::From(payload.data());
        switch (tuple.mFormat) {
        case TupleFormat::CHAINED: {
          auto ret = reconstructChainedTuple(key, payload, callback);
          JUMPMU_RETURN ret;
          break;
        }
        case TupleFormat::FAT: {
          const auto& fatTuple = *FatTuple::From(payload.data());
          auto ret = fatTuple.GetVisibleTuple(callback);
          JUMPMU_RETURN ret;
          break;
        }
        default: {
          DCHECK(false) << "Unhandled tuple format: "
                        << TupleFormatUtil::ToString(tuple.mFormat);
        }
        }
      }
      JUMPMU_CATCH() {
      }
    }
  }

  std::tuple<OP_RESULT, u16> reconstructChainedTuple(
      Slice key, Slice payload, std::function<void(Slice val)> callback);

private:
  static inline u64 maxFatTupleLength() {
    return BTreeNode::Size() - 1000;
  }

public:
  static BTreeVI* Create(const std::string& treeName, Config& config,
                         BTreeLL* graveyard) {
    auto [treePtr, treeId] =
        TreeRegistry::sInstance->CreateTree(treeName, [&]() {
          return std::unique_ptr<BufferManagedTree>(
              static_cast<BufferManagedTree*>(new storage::btree::BTreeVI()));
        });
    if (treePtr == nullptr) {
      LOG(ERROR) << "Failed to create BTreeVI, treeName has been taken"
                 << ", treeName=" << treeName;
      return nullptr;
    }
    auto tree = dynamic_cast<storage::btree::BTreeVI*>(treePtr);
    tree->Init(treeId, config, graveyard);

    // TODO(jian.z): record WAL
    return tree;
  }

  static void InsertToNode(GuardedBufferFrame<BTreeNode>& guardedNode,
                           Slice key, Slice val, WORKERID workerId,
                           TXID txStartTs, TX_MODE txMode, s32& slotId) {
    auto totalValSize = sizeof(ChainedTuple) + val.size();
    slotId = guardedNode->insertDoNotCopyPayload(key, totalValSize, slotId);
    auto tupleAddr = guardedNode->ValData(slotId);
    auto tuple = new (tupleAddr) ChainedTuple(workerId, txStartTs, val);
    if (txMode == TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT) {
      tuple->mTxId = MSB | 0;
    }
    guardedNode.MarkAsDirty();
  }
};

} // namespace btree
} // namespace storage
} // namespace leanstore
