#pragma once

#include "BTreeLL.hpp"
#include "Config.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "storage/btree/core/BTreeExclusiveIterator.hpp"
#include "storage/btree/core/BTreeSharedIterator.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "sync-primitives/PageGuard.hpp"
#include "utils/RandomGenerator.hpp"

#include <set>

namespace leanstore {
namespace storage {
namespace btree {

class BTreeVI : public BTreeLL {
public:
  struct WALRemove : WALPayload {
    u16 key_length;
    u16 value_length;
    WORKERID before_worker_id;
    u64 before_tx_id;
    u64 before_command_id;
    u8 payload[];

    WALRemove(Slice key, Slice val, WORKERID beforeWorkerId, u64 beforeTxId,
              u64 BeforeCommandId)
        : WALPayload(TYPE::WALRemove), key_length(key.size()),
          value_length(val.size()), before_worker_id(beforeWorkerId),
          before_tx_id(beforeTxId), before_command_id(BeforeCommandId) {
      std::memcpy(payload, key.data(), key.size());
      std::memcpy(payload + key.size(), val.data(), val.size());
    }
  };

  /**
   * Plan: we should handle frequently and infrequently updated tuples
   * differently when it comes to maintaining versions in the b-tree. For
   * frequently updated tuples, we store them in a FatTuple
   *
   * Prepartion phase: iterate over the chain and check whether all updated
   * attributes are the same and whether they fit on a page If both conditions
   * are fullfiled then we can store them in a fat tuple When FatTuple runs out
   * of space, we simply crash for now (real solutions approx variable-size
   * pages or fallback to chained keys)

   * How to convert CHAINED to FAT_TUPLE: Random number generation, similar to
   * contention split, don't eagerly remove the deltas to allow concurrent
   * readers to continue without complicating the logic if we fail

   * Glossary:
   * - UpdateDescriptor: (offset, length)[]
   * - Diff: raw bytes copied from src/dst next to each other according to the
   *   descriptor Delta: WWTS + diff + (descriptor)?
   */
  enum class TupleFormat : u8 {
    CHAINED = 0,
    FAT_TUPLE_DIFFERENT_ATTRIBUTES = 1,
    FAT_TUPLE_SAME_ATTRIBUTES = 2,
    VISIBLE_FOR_ALL = 3
  };

  // NEVER SHADOW A MEMBER!!!
  class __attribute__((packed)) Tuple {
  public:
    static constexpr COMMANDID INVALID_COMMANDID =
        std::numeric_limits<COMMANDID>::max();

    TupleFormat tuple_format;
    WORKERID mWorkerId;

    union {
      TXID tx_ts; // Could be start_ts or tx_id for WT scheme
      TXID start_ts;
    };

    COMMANDID command_id;
    bool mWriteLocked : true;

    Tuple(TupleFormat tuple_format, WORKERID workerId, TXID tx_id)
        : tuple_format(tuple_format), mWorkerId(workerId), tx_ts(tx_id),
          command_id(INVALID_COMMANDID) {
      mWriteLocked = false;
    }

    bool IsWriteLocked() const {
      return mWriteLocked;
    }

    void WriteLock() {
      mWriteLocked = true;
    }

    void WriteUnlock() {
      mWriteLocked = false;
    }
  };

  // Chained: only scheduled gc todos. FatTuple: eager pgc, no scheduled gc
  // todos
  struct __attribute__((packed)) ChainedTuple : Tuple {
    u16 updates_counter = 0;
    u16 oldest_tx = 0;
    u8 is_removed : 1;

    u8 payload[]; // latest version in-place

    ChainedTuple(WORKERID workerId, TXID txId)
        : Tuple(TupleFormat::CHAINED, workerId, txId), is_removed(false) {
      reset();
    }

    bool isFinal() const {
      return command_id == INVALID_COMMANDID;
    }

    void reset() {
    }
  };

  // We always append the descriptor, one format to keep simple
  struct __attribute__((packed)) FatTupleDifferentAttributes : Tuple {
    struct __attribute__((packed)) Delta {
      WORKERID mWorkerId;
      TXID tx_ts;

      // ATTENTION: TAKE CARE OF THIS, OTHERWISE WE WOULD
      // OVERWRITE ANOTHER UNDO VERSION
      COMMANDID command_id = INVALID_COMMANDID;

      u8 payload[]; // Descriptor + Diff
      UpdateSameSizeInPlaceDescriptor& getDescriptor() {
        return *reinterpret_cast<UpdateSameSizeInPlaceDescriptor*>(payload);
      }
      const UpdateSameSizeInPlaceDescriptor& getConstantDescriptor() const {
        return *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(
            payload);
      }
      inline u32 totalLength() {
        return sizeof(Delta) + getConstantDescriptor().size() +
               getConstantDescriptor().diffLength();
      }
    };

    u16 value_length = 0;
    u32 total_space = 0; // From the payload bytes array
    u32 used_space = 0;  // does not include the struct itself
    u32 mDataOffset = 0;
    u16 deltas_count = 0; // Attention: coupled with used_space

    // value, Delta+Descriptor+Diff[] O2N
    u8 payload[];

    FatTupleDifferentAttributes(const u32 init_total_space)
        : Tuple(TupleFormat::FAT_TUPLE_DIFFERENT_ATTRIBUTES, 0, 0),
          total_space(init_total_space), mDataOffset(init_total_space) {
    }

    // returns false to fallback to chained mode
    static bool update(BTreeExclusiveIterator& iterator, Slice key, ValCallback,
                       UpdateSameSizeInPlaceDescriptor&);

    bool hasSpaceFor(const UpdateSameSizeInPlaceDescriptor&);
    void append(UpdateSameSizeInPlaceDescriptor&);
    Delta& allocateDelta(u32 delta_total_length);
    void garbageCollection();
    void undoLastUpdate();

    inline Slice GetValue() {
      return Slice(getValue(), value_length);
    }

    inline constexpr u8* getValue() {
      return payload;
    }

    inline const u8* getValueConstant() const {
      return payload;
    }

    inline u16* getDeltaOffsets() {
      return reinterpret_cast<u16*>(payload + value_length);
    }

    inline const u16* getDeltaOffsetsConstant() const {
      return reinterpret_cast<const u16*>(payload + value_length);
    }

    inline Delta& getDelta(u16 d_i) {
      assert(reinterpret_cast<u8*>(getDeltaOffsets() + d_i) <
             reinterpret_cast<u8*>(payload + getDeltaOffsets()[d_i]));
      return *reinterpret_cast<Delta*>(payload + getDeltaOffsets()[d_i]);
    }

    inline const Delta& getDeltaConstant(u16 d_i) const {
      return *reinterpret_cast<const Delta*>(payload +
                                             getDeltaOffsetsConstant()[d_i]);
    }

    std::tuple<OP_RESULT, u16> reconstructTuple(ValCallback valCallback) const;

    void convertToChained(TREEID treeId);

    void resize(u32 new_length);
  };

  static_assert(sizeof(ChainedTuple) <= sizeof(FatTupleDifferentAttributes),
                "");

  struct __attribute__((packed)) DanglingPointer {
    BufferFrame* bf = nullptr;
    u64 latch_version_should_be = -1;
    s32 head_slot = -1;
  };

  struct __attribute__((packed)) Version {
    enum class TYPE : u8 { UPDATE, REMOVE };
    TYPE type;
    WORKERID mWorkerId;
    TXID tx_id;
    COMMANDID command_id;
    Version(TYPE type, WORKERID workerId, TXID txId, COMMANDID commandId)
        : type(type), mWorkerId(workerId), tx_id(txId), command_id(commandId) {
    }
  };

  struct __attribute__((packed)) UpdateVersion : Version {
    u8 is_delta : 1;
    u8 payload[]; // UpdateDescriptor + Diff

    UpdateVersion(WORKERID workerId, TXID txId, COMMANDID commandId,
                  bool isDelta)
        : Version(Version::TYPE::UPDATE, workerId, txId, commandId),
          is_delta(isDelta) {
    }

    bool isFinal() const {
      return command_id == 0;
    }
  };

  struct __attribute__((packed)) RemoveVersion : Version {
    u16 key_length;
    u16 value_length;
    DanglingPointer dangling_pointer;
    bool moved_to_graveway = false;
    u8 payload[]; // Key + Value
    RemoveVersion(WORKERID workerId, TXID tx_id, COMMANDID command_id,
                  u16 key_length, u16 value_length)
        : Version(Version::TYPE::REMOVE, workerId, tx_id, command_id),
          key_length(key_length), value_length(value_length) {
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
  void create(TREEID treeId, Config config, BTreeLL* graveyard) {
    this->mGraveyard = graveyard;
    BTreeLL::create(treeId, config);
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

    HybridPageGuard<BTreeNode> guardedNode(std::move(bfGuard), &bf);
    if (!guardedNode->mIsLeaf ||
        !triggerPageWiseGarbageCollection(guardedNode)) {
      return BTreeGeneric::checkSpaceUtilization(bf);
    }

    guardedNode.ToExclusiveMayJump();
    guardedNode.incrementGSN();

    for (u16 i = 0; i < guardedNode->mNumSeps; i++) {
      auto& tuple = *reinterpret_cast<Tuple*>(guardedNode->ValData(i));
      if (tuple.tuple_format == TupleFormat::FAT_TUPLE_DIFFERENT_ATTRIBUTES) {
        auto& fatTuple = *reinterpret_cast<FatTupleDifferentAttributes*>(
            guardedNode->ValData(i));
        const u32 newLength = fatTuple.value_length + sizeof(ChainedTuple);
        fatTuple.convertToChained(mTreeId);
        DCHECK(newLength < guardedNode->ValSize(i));
        guardedNode->shortenPayload(i, newLength);
        DCHECK(tuple.tuple_format == TupleFormat::CHAINED);
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
        Slice key(insert_entry.payload, insert_entry.key_length);
        BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
        OP_RESULT ret = iterator.seekExact(key);
        ENSURE(ret == OP_RESULT::OK);
        ret = iterator.removeCurrent();
        ENSURE(ret == OP_RESULT::OK);
        iterator.markAsDirty(); // TODO: write CLS
        iterator.mergeIfNeeded();
      }
      JUMPMU_CATCH() {
      }
      break;
    }
    case WALPayload::TYPE::WALUpdate: {
      auto& update_entry = *reinterpret_cast<const WALUpdateSSIP*>(&entry);
      JUMPMU_TRY() {
        Slice key(update_entry.payload, update_entry.key_length);
        BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
        OP_RESULT ret = iterator.seekExact(key);
        ENSURE(ret == OP_RESULT::OK);
        auto& tuple = *reinterpret_cast<Tuple*>(iterator.mutableValue().data());
        ENSURE(!tuple.IsWriteLocked());
        if (tuple.tuple_format == TupleFormat::FAT_TUPLE_DIFFERENT_ATTRIBUTES) {
          reinterpret_cast<FatTupleDifferentAttributes*>(
              iterator.mutableValue().data())
              ->undoLastUpdate();
        } else {
          auto& chain_head =
              *reinterpret_cast<ChainedTuple*>(iterator.mutableValue().data());
          chain_head.mWorkerId = update_entry.before_worker_id;
          chain_head.tx_ts = update_entry.before_tx_id;
          chain_head.command_id = update_entry.before_command_id;
          const auto& update_descriptor =
              *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(
                  update_entry.payload + update_entry.key_length);
          BTreeLL::applyXORDiff(update_descriptor, chain_head.payload,
                                update_entry.payload + update_entry.key_length +
                                    update_descriptor.size());
        }
        iterator.markAsDirty();
        JUMPMU_RETURN;
      }
      JUMPMU_CATCH() {
        UNREACHABLE();
      }
      break;
    }
    case WALPayload::TYPE::WALRemove: {
      auto& remove_entry = *reinterpret_cast<const WALRemove*>(&entry);
      Slice key(remove_entry.payload, remove_entry.key_length);
      JUMPMU_TRY() {
        BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
        OP_RESULT ret = iterator.seekExact(key);
        ENSURE(ret == OP_RESULT::OK);
        // Resize
        const u16 new_primary_payload_length =
            remove_entry.value_length + sizeof(ChainedTuple);
        const Slice old_primary_payload = iterator.value();
        if (old_primary_payload.length() < new_primary_payload_length) {
          const bool did_extend =
              iterator.extendPayload(new_primary_payload_length);
          ENSURE(did_extend);
        } else {
          iterator.shorten(new_primary_payload_length);
        }
        MutableSlice primaryPayload = iterator.mutableValue();
        auto& primaryVersion = *new (primaryPayload.data()) ChainedTuple(
            remove_entry.before_worker_id, remove_entry.before_tx_id);
        std::memcpy(primaryVersion.payload,
                    remove_entry.payload + remove_entry.key_length,
                    remove_entry.value_length);
        primaryVersion.command_id = remove_entry.before_command_id;
        ENSURE(primaryVersion.is_removed == false);
        primaryVersion.WriteUnlock();
        iterator.markAsDirty();
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
        version.tx_id < cr::Worker::my().cc.local_all_lwm) {
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
        ENSURE(head.tuple_format == TupleFormat::CHAINED &&
               !head.IsWriteLocked() && head.mWorkerId == version_worker_id &&
               head.tx_ts == version_tx_id && head.is_removed);
        node->removeSlot(version.dangling_pointer.head_slot);
        iterator.markAsDirty();
        iterator.mergeIfNeeded();
        JUMPMU_RETURN;
      }
      JUMPMU_CATCH() {
      }
    }
    Slice key(version.payload, version.key_length);
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
          g_iterator.markAsDirty();
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
      MutableSlice primaryPayload = iterator.mutableValue();
      {
        // Checks
        const auto& tuple =
            *reinterpret_cast<const Tuple*>(primaryPayload.data());
        if (tuple.tuple_format == TupleFormat::FAT_TUPLE_DIFFERENT_ATTRIBUTES) {
          JUMPMU_RETURN;
        }
      }
      ChainedTuple& primaryVersion =
          *reinterpret_cast<ChainedTuple*>(primaryPayload.data());
      if (!primaryVersion.IsWriteLocked()) {
        if (primaryVersion.mWorkerId == version_worker_id &&
            primaryVersion.tx_ts == version_tx_id &&
            primaryVersion.is_removed) {
          if (primaryVersion.tx_ts < cr::Worker::my().cc.local_all_lwm) {
            ret = iterator.removeCurrent();
            iterator.markAsDirty();
            ENSURE(ret == OP_RESULT::OK);
            iterator.mergeIfNeeded();
            COUNTERS_BLOCK() {
              WorkerCounters::myCounters().cc_todo_removed[mTreeId]++;
            }
          } else if (primaryVersion.tx_ts <
                     cr::Worker::my().cc.local_oltp_lwm) {
            // Move to mGraveyard
            {
              BTreeExclusiveIterator g_iterator(
                  *static_cast<BTreeGeneric*>(mGraveyard));
              OP_RESULT g_ret = g_iterator.insertKV(key, iterator.value());
              ENSURE(g_ret == OP_RESULT::OK);
              g_iterator.markAsDirty();
            }
            ret = iterator.removeCurrent();
            ENSURE(ret == OP_RESULT::OK);
            iterator.markAsDirty();
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
      key = Slice(insert_entry.payload, insert_entry.key_length);
      break;
    }
    case WALPayload::TYPE::WALUpdate: {
      auto& update_entry = *reinterpret_cast<const WALUpdateSSIP*>(&entry);
      key = Slice(update_entry.payload, update_entry.key_length);
      break;
    }
    case WALPayload::TYPE::WALRemove: {
      auto& remove_entry = *reinterpret_cast<const WALRemove*>(&entry);
      key = Slice(remove_entry.payload, remove_entry.key_length);
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
      auto& tuple = *reinterpret_cast<Tuple*>(iterator.mutableValue().data());
      ENSURE(tuple.tuple_format == TupleFormat::CHAINED);
      /**
       * The major work is in traversing the tree:
       *
       * if (tuple.tx_ts == cr::activeTX().startTS() &&
       *     tuple.mWorkerId == cr::Worker::my().mWorkerId) {
       *   auto& chain_head =
       *       *reinterpret_cast<ChainedTuple*>(iterator.mutableValue().data());
       *   chain_head.mCommitTs = cr::activeTX().commitTS() | MSB;
       * }
       */
    }
    JUMPMU_CATCH() {
      UNREACHABLE();
    }
  }

private:
  bool convertChainedToFatTupleDifferentAttributes(
      BTreeExclusiveIterator& iterator);

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
            reconstructTuple(s_key, iterator.value(), [&](Slice value) {
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
        reconstructTuple(iterator.key(), iterator.value(), [&](Slice value) {
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
          g_lower_bound = Slice(iterator.mBuffer, iterator.mFenceSize + 1);
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
          reconstructTuple(g_key, g_iterator.value(), [&](Slice value) {
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
            reconstructTuple(g_key, g_iterator.value(), [&](Slice value) {
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

  inline bool isVisibleForMe(WORKERID workerId, TXID txId,
                             bool toWrite = true) {
    return cr::Worker::my().cc.isVisibleForMe(workerId, txId, toWrite);
  }

  static inline bool triggerPageWiseGarbageCollection(
      HybridPageGuard<BTreeNode>& guard) {
    return guard->mHasGarbage;
  }

  u64 convertToFatTupleThreshold() {
    return FLAGS_worker_threads;
  }

  inline std::tuple<OP_RESULT, u16> reconstructTuple(
      Slice key, Slice payload, std::function<void(Slice value)> callback) {
    while (true) {
      JUMPMU_TRY() {
        if (reinterpret_cast<const Tuple*>(payload.data())->tuple_format ==
            TupleFormat::CHAINED) {
          const ChainedTuple& primary_version =
              *reinterpret_cast<const ChainedTuple*>(payload.data());
          if (isVisibleForMe(primary_version.mWorkerId, primary_version.tx_ts,
                             false)) {
            if (primary_version.is_removed) {
              JUMPMU_RETURN{OP_RESULT::NOT_FOUND, 1};
            }
            callback(Slice(primary_version.payload,
                           payload.length() - sizeof(ChainedTuple)));
            JUMPMU_RETURN{OP_RESULT::OK, 1};
          } else {
            if (primary_version.isFinal()) {
              JUMPMU_RETURN{OP_RESULT::NOT_FOUND, 1};
            } else {
              auto ret = reconstructChainedTuple(key, payload, callback);
              if (FLAGS_tmp6)
                RAISE_WHEN(std::get<0>(ret) == OP_RESULT::NOT_FOUND);
              JUMPMU_RETURN ret;
            }
          }
        } else {
          auto ret = reinterpret_cast<const FatTupleDifferentAttributes*>(
                         payload.data())
                         ->reconstructTuple(callback);
          RAISE_WHEN(std::get<0>(ret) == OP_RESULT::NOT_FOUND);
          JUMPMU_RETURN ret;
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
    return EFFECTIVE_PAGE_SIZE - 1000;
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
      LOG(ERROR) << "Failed to create BTreeVI"
                 << ", treeName has been taken"
                 << ", treeName=" << treeName;
      return nullptr;
    }
    auto tree = dynamic_cast<storage::btree::BTreeVI*>(treePtr);
    tree->create(treeId, config, graveyard);
    return tree;
  }
};

} // namespace btree
} // namespace storage
} // namespace leanstore
