#include "BTreeLL.hpp"

#include "concurrency-recovery/CRMG.hpp"
#include "storage/btree/core/BTreeExclusiveIterator.hpp"
#include "storage/btree/core/BTreeSharedIterator.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <alloca.h>
#include <signal.h>

using namespace std;
using namespace leanstore::storage;

namespace leanstore {
namespace storage {
namespace btree {

#define ARRAY_ON_STACK(varName, T, N) T* varName = (T*)alloca((N) * sizeof(T));

OP_RESULT BTreeLL::Lookup(Slice key, ValCallback valCallback) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> leaf;
      FindLeafCanJump(key, leaf);

      DEBUG_BLOCK() {
        s16 sanity_check_result = leaf->compareKeyWithBoundaries(key);
        leaf.JumpIfModifiedByOthers();
        if (sanity_check_result != 0) {
          cout << leaf->mNumSeps << endl;
        }
        ENSURE(sanity_check_result == 0);
      }

      s16 slotId = leaf->lowerBound<true>(key);
      if (slotId != -1) {
        valCallback(leaf->Value(slotId));
        leaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OP_RESULT::OK;
      } else {
        leaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OP_RESULT::NOT_FOUND;
      }
    }
    JUMPMU_CATCH() {
      DLOG(WARNING) << "BTreeLL::Lookup retried";
      WorkerCounters::myCounters().dt_restarts_read[mTreeId]++;
    }
  }
  UNREACHABLE();
  return OP_RESULT::OTHER;
}

bool BTreeLL::isRangeSurelyEmpty(Slice startKey, Slice endKey) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> leaf;
      FindLeafCanJump(startKey, leaf);

      Slice upperFence = leaf->GetUpperFence();
      Slice lowerFence = leaf->GetLowerFence();
      assert(startKey >= lowerFence);

      if ((leaf->mUpperFence.offset == 0 || endKey <= upperFence) &&
          leaf->mNumSeps == 0) {
        s32 pos = leaf->lowerBound<false>(startKey);
        if (pos == leaf->mNumSeps) {
          leaf.JumpIfModifiedByOthers();
          JUMPMU_RETURN true;
        } else {
          leaf.JumpIfModifiedByOthers();
          JUMPMU_RETURN false;
        }
      } else {
        leaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN false;
      }
    }
    JUMPMU_CATCH() {
    }
  }
  UNREACHABLE();
  return false;
}

OP_RESULT BTreeLL::scanAsc(Slice startKey, ScanCallback callback) {
  COUNTERS_BLOCK() {
    WorkerCounters::myCounters().dt_scan_asc[mTreeId]++;
  }

  JUMPMU_TRY() {
    BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
    OP_RESULT ret = iterator.seek(startKey);
    while (ret == OP_RESULT::OK) {
      iterator.assembleKey();
      auto key = iterator.key();
      auto value = iterator.value();
      if (!callback(key, value)) {
        break;
      }
      ret = iterator.next();
    }
    JUMPMU_RETURN OP_RESULT::OK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OP_RESULT::OTHER;
}

OP_RESULT BTreeLL::scanDesc(Slice scanKey, ScanCallback callback) {
  COUNTERS_BLOCK() {
    WorkerCounters::myCounters().dt_scan_desc[mTreeId]++;
  }
  JUMPMU_TRY() {
    BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
    auto ret = iterator.seekForPrev(scanKey);
    if (ret != OP_RESULT::OK) {
      JUMPMU_RETURN ret;
    }
    while (true) {
      iterator.assembleKey();
      auto key = iterator.key();
      auto value = iterator.value();
      if (!callback(key, value)) {
        JUMPMU_RETURN OP_RESULT::OK;
      } else {
        if (iterator.prev() != OP_RESULT::OK) {
          JUMPMU_RETURN OP_RESULT::NOT_FOUND;
        }
      }
    }
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OP_RESULT::OTHER;
}

OP_RESULT BTreeLL::insert(Slice key, Slice val) {
  cr::activeTX().markAsWrite();
  if (config.mEnableWal) {
    cr::Worker::my().mLogging.walEnsureEnoughSpace(PAGE_SIZE * 1);
  }

  JUMPMU_TRY() {
    BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
    OP_RESULT ret = iterator.insertKV(key, val);
    ENSURE(ret == OP_RESULT::OK);
    if (config.mEnableWal) {
      auto walSize = key.length() + val.length();
      auto walHandler =
          iterator.mGuardedLeaf.ReserveWALPayload<WALInsert>(walSize, key, val);
      walHandler.SubmitWal();
    } else {
      iterator.markAsDirty();
    }
    JUMPMU_RETURN OP_RESULT::OK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OP_RESULT::OTHER;
}

OP_RESULT BTreeLL::prefixLookup(Slice key, PrefixLookupCallback callback) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> leaf;
      FindLeafCanJump(key, leaf);

      bool isEqual = false;
      s16 cur = leaf->lowerBound<false>(key, &isEqual);
      if (isEqual) {
        callback(key, leaf->Value(cur));
        leaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OP_RESULT::OK;
      } else if (cur < leaf->mNumSeps) {
        u16 fullKeySize = leaf->getFullKeyLen(cur);
        ARRAY_ON_STACK(fullKeyBuf, u8, fullKeySize);
        leaf->copyFullKey(cur, fullKeyBuf);
        leaf.JumpIfModifiedByOthers();

        callback(Slice(fullKeyBuf, fullKeySize), leaf->Value(cur));
        leaf.JumpIfModifiedByOthers();

        JUMPMU_RETURN OP_RESULT::OK;
      } else {
        OP_RESULT ret = scanAsc(key, [&](Slice scannedKey, Slice scannedVal) {
          callback(scannedKey, scannedVal);
          return false;
        });
        JUMPMU_RETURN ret;
      }
    }
    JUMPMU_CATCH() {
      WorkerCounters::myCounters().dt_restarts_read[mTreeId]++;
    }
  }

  UNREACHABLE();
  return OP_RESULT::OTHER;
}

OP_RESULT BTreeLL::prefixLookupForPrev(Slice key,
                                       PrefixLookupCallback callback) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> leaf;
      FindLeafCanJump(key, leaf);

      bool isEqual = false;
      s16 cur = leaf->lowerBound<false>(key, &isEqual);
      if (isEqual == true) {
        callback(key, leaf->Value(cur));
        leaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OP_RESULT::OK;
      } else if (cur > 0) {
        cur -= 1;

        u16 fullKeySize = leaf->getFullKeyLen(cur);
        ARRAY_ON_STACK(fullKeyBuf, u8, fullKeySize);
        leaf->copyFullKey(cur, fullKeyBuf);
        leaf.JumpIfModifiedByOthers();

        callback(Slice(fullKeyBuf, fullKeySize), leaf->Value(cur));
        leaf.JumpIfModifiedByOthers();

        JUMPMU_RETURN OP_RESULT::OK;
      } else {
        OP_RESULT ret = scanDesc(key, [&](Slice scannedKey, Slice scannedVal) {
          callback(scannedKey, scannedVal);
          return false;
        });
        JUMPMU_RETURN ret;
      }
    }
    JUMPMU_CATCH() {
      WorkerCounters::myCounters().dt_restarts_read[mTreeId]++;
    }
  }

  UNREACHABLE();
  return OP_RESULT::OTHER;
}

OP_RESULT BTreeLL::append(std::function<void(u8*)> o_key, u16 o_key_length,
                          std::function<void(u8*)> o_value, u16 o_value_length,
                          std::unique_ptr<u8[]>& session_ptr) {
  struct alignas(64) Session {
    BufferFrame* bf;
  };

  if (session_ptr.get()) {
    auto session = reinterpret_cast<Session*>(session_ptr.get());
    JUMPMU_TRY() {
      HybridGuard opt_guard(&session->bf->header.mLatch);
      opt_guard.toOptimisticOrJump();
      {
        BTreeNode& node =
            *reinterpret_cast<BTreeNode*>(session->bf->page.mPayload);
        if (session->bf->page.mBTreeId != mTreeId || !node.mIsLeaf ||
            node.mUpperFence.length != 0 || node.mUpperFence.offset != 0) {
          jumpmu::jump();
        }
      }
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this),
                                      session->bf, opt_guard.mVersion);
      // -------------------------------------------------------------------------------------
      OP_RESULT ret =
          iterator.enoughSpaceInCurrentNode(o_key_length, o_value_length);
      if (ret == OP_RESULT::OK) {
        auto key_buffer = (u8*)alloca(o_key_length * sizeof(u8));
        o_key(key_buffer);
        const s32 pos = iterator.mGuardedLeaf->mNumSeps;
        iterator.mGuardedLeaf->insertDoNotCopyPayload(
            Slice(key_buffer, o_key_length), o_value_length, pos);
        iterator.mSlotId = pos;
        o_value(iterator.mutableValue().data());
        iterator.markAsDirty();
        COUNTERS_BLOCK() {
          WorkerCounters::myCounters().dt_append_opt[mTreeId]++;
        }
        JUMPMU_RETURN OP_RESULT::OK;
      }
    }
    JUMPMU_CATCH() {
    }
  }
  // -------------------------------------------------------------------------------------
  while (true) {
    JUMPMU_TRY() {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto key_buffer = (u8*)alloca(o_key_length * sizeof(u8));
      for (u64 i = 0; i < o_key_length; i++) {
        key_buffer[i] = 255;
      }
      const Slice key(key_buffer, o_key_length);
      OP_RESULT ret = iterator.seekToInsert(key);
      RAISE_WHEN(ret == OP_RESULT::DUPLICATE);
      ret = iterator.enoughSpaceInCurrentNode(key, o_value_length);
      if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
        iterator.splitForKey(key);
        JUMPMU_CONTINUE;
      }
      o_key(key_buffer);
      iterator.insertInCurrentNode(key, o_value_length);
      o_value(iterator.mutableValue().data());
      iterator.markAsDirty();
      // -------------------------------------------------------------------------------------
      Session* session = nullptr;
      if (session_ptr) {
        session = reinterpret_cast<Session*>(session_ptr.get());
      } else {
        session_ptr = std::make_unique<u8[]>(sizeof(Session));
        session = new (session_ptr.get()) Session();
      }
      session->bf = iterator.mGuardedLeaf.mBf;
      // -------------------------------------------------------------------------------------
      COUNTERS_BLOCK() {
        WorkerCounters::myCounters().dt_append[mTreeId]++;
      }
      JUMPMU_RETURN OP_RESULT::OK;
    }
    JUMPMU_CATCH() {
    }
  }
}

OP_RESULT BTreeLL::updateSameSizeInPlace(
    Slice key, ValCallback callback,
    UpdateSameSizeInPlaceDescriptor& update_descriptor) {
  cr::activeTX().markAsWrite();
  if (config.mEnableWal) {
    cr::Worker::my().mLogging.walEnsureEnoughSpace(PAGE_SIZE * 1);
  }

  JUMPMU_TRY() {
    BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
    auto ret = iterator.seekExact(key);
    if (ret != OP_RESULT::OK) {
      JUMPMU_RETURN ret;
    }
    auto current_value = iterator.mutableValue();
    if (config.mEnableWal) {
      // if it is a secondary index, then we can not use updateSameSize
      assert(update_descriptor.count > 0);

      const u16 delta_length =
          update_descriptor.size() + update_descriptor.diffLength();
      auto walHandler = iterator.mGuardedLeaf.ReserveWALPayload<WALUpdate>(
          key.length() + delta_length);
      walHandler->type = WALPayload::TYPE::WALUpdate;
      walHandler->key_length = key.length();
      walHandler->delta_length = delta_length;
      u8* wal_ptr = walHandler->payload;
      std::memcpy(wal_ptr, key.data(), key.length());
      wal_ptr += key.length();
      std::memcpy(wal_ptr, &update_descriptor, update_descriptor.size());
      wal_ptr += update_descriptor.size();
      generateDiff(update_descriptor, wal_ptr, current_value.data());
      // The actual update by the client
      callback(current_value.Immutable());
      generateXORDiff(update_descriptor, wal_ptr, current_value.data());
      walHandler.SubmitWal();
    } else {
      callback(current_value.Immutable());
      iterator.markAsDirty();
    }
    iterator.UpdateContentionStats();
    JUMPMU_RETURN OP_RESULT::OK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OP_RESULT::OTHER;
}

OP_RESULT BTreeLL::remove(Slice key) {
  cr::activeTX().markAsWrite();
  if (config.mEnableWal) {
    cr::Worker::my().mLogging.walEnsureEnoughSpace(PAGE_SIZE * 1);
  }
  JUMPMU_TRY() {
    BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
    auto ret = iterator.seekExact(key);
    if (ret != OP_RESULT::OK) {
      JUMPMU_RETURN ret;
    }

    Slice value = iterator.value();
    if (config.mEnableWal) {
      auto walHandler = iterator.mGuardedLeaf.ReserveWALPayload<WALRemove>(
          key.size() + value.size(), key, value);
      walHandler.SubmitWal();
      iterator.markAsDirty();
    } else {
      iterator.markAsDirty();
    }
    ret = iterator.removeCurrent();
    ENSURE(ret == OP_RESULT::OK);
    iterator.mergeIfNeeded();
    JUMPMU_RETURN OP_RESULT::OK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OP_RESULT::OTHER;
}

OP_RESULT BTreeLL::rangeRemove(Slice startKey, Slice endKey, bool page_wise) {
  JUMPMU_TRY() {
    BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
    iterator.exitLeafCallback([&](GuardedBufferFrame<BTreeNode>& leaf) {
      if (leaf->freeSpaceAfterCompaction() >= BTreeNodeHeader::sUnderFullSize) {
        iterator.cleanUpCallback([&, to_find = leaf.mBf] {
          JUMPMU_TRY() {
            this->tryMerge(*to_find);
          }
          JUMPMU_CATCH() {
          }
        });
      }
    });

    ENSURE(config.mEnableWal == false);
    if (!page_wise) {
      auto ret = iterator.seek(startKey);
      if (ret != OP_RESULT::OK) {
        JUMPMU_RETURN ret;
      }
      while (true) {
        iterator.assembleKey();
        auto c_key = iterator.key();
        if (c_key >= startKey && c_key <= endKey) {
          COUNTERS_BLOCK() {
            WorkerCounters::myCounters().dt_range_removed[mTreeId]++;
          }
          ret = iterator.removeCurrent();
          ENSURE(ret == OP_RESULT::OK);
          iterator.markAsDirty();
          if (iterator.mSlotId == iterator.mGuardedLeaf->mNumSeps) {
            ret = iterator.next();
          }
        } else {
          break;
        }
      }
      JUMPMU_RETURN OP_RESULT::OK;
    } else {
      bool did_purge_full_page = false;
      iterator.enterLeafCallback([&](GuardedBufferFrame<BTreeNode>& leaf) {
        if (leaf->mNumSeps == 0) {
          return;
        }

        // page start key
        ARRAY_ON_STACK(firstKey, u8, leaf->getFullKeyLen(0));
        leaf->copyFullKey(0, firstKey);
        Slice pageStartKey(firstKey, leaf->getFullKeyLen(0));

        // page end key
        ARRAY_ON_STACK(lastKey, u8, leaf->getFullKeyLen(leaf->mNumSeps - 1));
        leaf->copyFullKey(leaf->mNumSeps - 1, lastKey);
        Slice pageEndKey(lastKey, leaf->getFullKeyLen(leaf->mNumSeps - 1));

        if (pageStartKey >= startKey && pageEndKey <= endKey) {
          // Purge the whole page
          COUNTERS_BLOCK() {
            WorkerCounters::myCounters().dt_range_removed[mTreeId] +=
                leaf->mNumSeps;
          }
          leaf->reset();
          iterator.markAsDirty();
          did_purge_full_page = true;
        }
      });

      while (true) {
        iterator.seek(startKey);
        if (did_purge_full_page) {
          did_purge_full_page = false;
          continue;
        } else {
          break;
        }
      }
    }
  }
  JUMPMU_CATCH() {
  }
  return OP_RESULT::OK;
}

u64 BTreeLL::countEntries() {
  return BTreeGeneric::countEntries();
}

u64 BTreeLL::countPages() {
  return BTreeGeneric::countPages();
}

u64 BTreeLL::getHeight() {
  return BTreeGeneric::getHeight();
}

void BTreeLL::generateDiff(
    const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst,
    const u8* src) {
  u64 dst_offset = 0;
  for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
    const auto& slot = update_descriptor.slots[a_i];
    std::memcpy(dst + dst_offset, src + slot.offset, slot.length);
    dst_offset += slot.length;
  }
}

void BTreeLL::applyDiff(
    const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst,
    const u8* src) {
  u64 src_offset = 0;
  for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
    const auto& slot = update_descriptor.slots[a_i];
    std::memcpy(dst + slot.offset, src + src_offset, slot.length);
    src_offset += slot.length;
  }
}

void BTreeLL::generateXORDiff(
    const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst,
    const u8* src) {
  u64 dst_offset = 0;
  for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
    const auto& slot = update_descriptor.slots[a_i];
    for (u64 b_i = 0; b_i < slot.length; b_i++) {
      *(dst + dst_offset + b_i) ^= *(src + slot.offset + b_i);
    }
    dst_offset += slot.length;
  }
}

void BTreeLL::applyXORDiff(
    const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst,
    const u8* src) {
  u64 src_offset = 0;
  for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
    const auto& slot = update_descriptor.slots[a_i];
    for (u64 b_i = 0; b_i < slot.length; b_i++) {
      *(dst + slot.offset + b_i) ^= *(src + src_offset + b_i);
    }
    src_offset += slot.length;
  }
}

} // namespace btree
} // namespace storage
} // namespace leanstore
