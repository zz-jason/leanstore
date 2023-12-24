#include "BTreeLL.hpp"

#include "concurrency-recovery/CRMG.hpp"
#include "storage/btree/core/BTreeExclusiveIterator.hpp"
#include "storage/btree/core/BTreeSharedIterator.hpp"
#include "utils/Misc.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <signal.h>

using namespace std;
using namespace leanstore::storage;

namespace leanstore {
namespace storage {
namespace btree {

OP_RESULT BTreeLL::Lookup(Slice key, ValCallback valCallback) {
  DCHECK(cr::Worker::my().IsTxStarted());
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> guardedLeaf;
      FindLeafCanJump(key, guardedLeaf);

      DEBUG_BLOCK() {
        s16 sanity_check_result = guardedLeaf->compareKeyWithBoundaries(key);
        guardedLeaf.JumpIfModifiedByOthers();
        if (sanity_check_result != 0) {
          cout << guardedLeaf->mNumSeps << endl;
        }
        ENSURE(sanity_check_result == 0);
      }

      s16 slotId = guardedLeaf->lowerBound<true>(key);
      if (slotId != -1) {
        valCallback(guardedLeaf->Value(slotId));
        guardedLeaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OP_RESULT::OK;
      } else {
        guardedLeaf.JumpIfModifiedByOthers();
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
      GuardedBufferFrame<BTreeNode> guardedLeaf;
      FindLeafCanJump(startKey, guardedLeaf);

      Slice upperFence = guardedLeaf->GetUpperFence();
      Slice lowerFence = guardedLeaf->GetLowerFence();
      assert(startKey >= lowerFence);

      if ((guardedLeaf->mUpperFence.offset == 0 || endKey <= upperFence) &&
          guardedLeaf->mNumSeps == 0) {
        s32 pos = guardedLeaf->lowerBound<false>(startKey);
        if (pos == guardedLeaf->mNumSeps) {
          guardedLeaf.JumpIfModifiedByOthers();
          JUMPMU_RETURN true;
        } else {
          guardedLeaf.JumpIfModifiedByOthers();
          JUMPMU_RETURN false;
        }
      } else {
        guardedLeaf.JumpIfModifiedByOthers();
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
  DCHECK(cr::Worker::my().IsTxStarted());
  cr::activeTX().markAsWrite();
  if (config.mEnableWal) {
    cr::Worker::my().mLogging.walEnsureEnoughSpace(FLAGS_page_size * 1);
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
      iterator.MarkAsDirty();
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
      GuardedBufferFrame<BTreeNode> guardedLeaf;
      FindLeafCanJump(key, guardedLeaf);

      bool isEqual = false;
      s16 cur = guardedLeaf->lowerBound<false>(key, &isEqual);
      if (isEqual) {
        callback(key, guardedLeaf->Value(cur));
        guardedLeaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OP_RESULT::OK;
      } else if (cur < guardedLeaf->mNumSeps) {
        auto fullKeySize = guardedLeaf->getFullKeyLen(cur);
        auto fullKeyBuf = utils::ArrayOnStack<u8>(fullKeySize);
        guardedLeaf->copyFullKey(cur, fullKeyBuf);
        guardedLeaf.JumpIfModifiedByOthers();

        callback(Slice(fullKeyBuf, fullKeySize), guardedLeaf->Value(cur));
        guardedLeaf.JumpIfModifiedByOthers();

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
      GuardedBufferFrame<BTreeNode> guardedLeaf;
      FindLeafCanJump(key, guardedLeaf);

      bool isEqual = false;
      s16 cur = guardedLeaf->lowerBound<false>(key, &isEqual);
      if (isEqual == true) {
        callback(key, guardedLeaf->Value(cur));
        guardedLeaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OP_RESULT::OK;
      } else if (cur > 0) {
        cur -= 1;
        auto fullKeySize = guardedLeaf->getFullKeyLen(cur);
        auto fullKeyBuf = utils::ArrayOnStack<u8>(fullKeySize);
        guardedLeaf->copyFullKey(cur, fullKeyBuf);
        guardedLeaf.JumpIfModifiedByOthers();

        callback(Slice(fullKeyBuf, fullKeySize), guardedLeaf->Value(cur));
        guardedLeaf.JumpIfModifiedByOthers();

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
        auto keyBuffer = utils::ArrayOnStack<u8>(o_key_length);
        o_key(keyBuffer);
        const s32 pos = iterator.mGuardedLeaf->mNumSeps;
        iterator.mGuardedLeaf->insertDoNotCopyPayload(
            Slice(keyBuffer, o_key_length), o_value_length, pos);
        iterator.mSlotId = pos;
        o_value(iterator.MutableVal().data());
        iterator.MarkAsDirty();
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
      auto keyBuffer = utils::ArrayOnStack<u8>(o_key_length);
      for (u64 i = 0; i < o_key_length; i++) {
        keyBuffer[i] = 255;
      }
      const Slice key(keyBuffer, o_key_length);
      OP_RESULT ret = iterator.seekToInsert(key);
      RAISE_WHEN(ret == OP_RESULT::DUPLICATE);
      ret = iterator.enoughSpaceInCurrentNode(key, o_value_length);
      if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
        iterator.splitForKey(key);
        JUMPMU_CONTINUE;
      }
      o_key(keyBuffer);
      iterator.insertInCurrentNode(key, o_value_length);
      o_value(iterator.MutableVal().data());
      iterator.MarkAsDirty();
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
    UpdateSameSizeInPlaceDescriptor& updateDescriptor) {
  cr::activeTX().markAsWrite();
  if (config.mEnableWal) {
    cr::Worker::my().mLogging.walEnsureEnoughSpace(FLAGS_page_size * 1);
  }

  JUMPMU_TRY() {
    BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
    auto ret = iterator.seekExact(key);
    if (ret != OP_RESULT::OK) {
      JUMPMU_RETURN ret;
    }
    auto current_value = iterator.MutableVal();
    if (config.mEnableWal) {
      // if it is a secondary index, then we can not use updateSameSize
      assert(updateDescriptor.count > 0);

      const u16 delta_length = updateDescriptor.TotalSize();
      auto walHandler = iterator.mGuardedLeaf.ReserveWALPayload<WALUpdate>(
          key.length() + delta_length);
      walHandler->type = WALPayload::TYPE::WALUpdate;
      walHandler->mKeySize = key.length();
      walHandler->delta_length = delta_length;
      u8* wal_ptr = walHandler->payload;
      std::memcpy(wal_ptr, key.data(), key.length());
      wal_ptr += key.length();
      std::memcpy(wal_ptr, &updateDescriptor, updateDescriptor.size());
      wal_ptr += updateDescriptor.size();
      generateDiff(updateDescriptor, wal_ptr, current_value.data());
      // The actual update by the client
      callback(current_value.Immutable());
      generateXORDiff(updateDescriptor, wal_ptr, current_value.data());
      walHandler.SubmitWal();
    } else {
      callback(current_value.Immutable());
      iterator.MarkAsDirty();
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
    cr::Worker::my().mLogging.walEnsureEnoughSpace(FLAGS_page_size * 1);
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
      iterator.MarkAsDirty();
    } else {
      iterator.MarkAsDirty();
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
    iterator.exitLeafCallback([&](GuardedBufferFrame<BTreeNode>& guardedLeaf) {
      if (guardedLeaf->freeSpaceAfterCompaction() >=
          BTreeNode::UnderFullSize()) {
        iterator.cleanUpCallback([&, to_find = guardedLeaf.mBf] {
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
          iterator.MarkAsDirty();
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
      iterator.enterLeafCallback(
          [&](GuardedBufferFrame<BTreeNode>& guardedLeaf) {
            if (guardedLeaf->mNumSeps == 0) {
              return;
            }

            // page start key
            auto firstKeySize = guardedLeaf->getFullKeyLen(0);
            auto firstKey = utils::ArrayOnStack<u8>(firstKeySize);
            guardedLeaf->copyFullKey(0, firstKey);
            Slice pageStartKey(firstKey, firstKeySize);

            // page end key
            auto lastKeySize =
                guardedLeaf->getFullKeyLen(guardedLeaf->mNumSeps - 1);
            auto lastKey = utils::ArrayOnStack<u8>(lastKeySize);
            guardedLeaf->copyFullKey(guardedLeaf->mNumSeps - 1, lastKey);
            Slice pageEndKey(lastKey, lastKeySize);

            if (pageStartKey >= startKey && pageEndKey <= endKey) {
              // Purge the whole page
              COUNTERS_BLOCK() {
                WorkerCounters::myCounters().dt_range_removed[mTreeId] +=
                    guardedLeaf->mNumSeps;
              }
              guardedLeaf->reset();
              iterator.MarkAsDirty();
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
    const UpdateSameSizeInPlaceDescriptor& updateDescriptor, u8* dst,
    const u8* src) {
  u64 dstOffset = 0;
  for (u64 i = 0; i < updateDescriptor.count; i++) {
    const auto& slot = updateDescriptor.mDiffSlots[i];
    std::memcpy(dst + dstOffset, src + slot.offset, slot.length);
    dstOffset += slot.length;
  }
}

void BTreeLL::applyDiff(const UpdateSameSizeInPlaceDescriptor& updateDescriptor,
                        u8* dst, const u8* src) {
  u64 srcOffset = 0;
  for (u64 i = 0; i < updateDescriptor.count; i++) {
    const auto& slot = updateDescriptor.mDiffSlots[i];
    std::memcpy(dst + slot.offset, src + srcOffset, slot.length);
    srcOffset += slot.length;
  }
}

void BTreeLL::generateXORDiff(
    const UpdateSameSizeInPlaceDescriptor& updateDescriptor, u8* dst,
    const u8* src) {
  u64 dstOffset = 0;
  for (u64 i = 0; i < updateDescriptor.count; i++) {
    const auto& slot = updateDescriptor.mDiffSlots[i];
    for (u64 j = 0; j < slot.length; j++) {
      dst[dstOffset + j] ^= src[slot.offset + j];
    }
    dstOffset += slot.length;
  }
}

void BTreeLL::applyXORDiff(
    const UpdateSameSizeInPlaceDescriptor& updateDescriptor, u8* dst,
    const u8* src) {
  u64 srcOffset = 0;
  for (u64 i = 0; i < updateDescriptor.count; i++) {
    const auto& slot = updateDescriptor.mDiffSlots[i];
    for (u64 j = 0; j < slot.length; j++) {
      dst[slot.offset + j] ^= src[srcOffset + j];
    }
    srcOffset += slot.length;
  }
}

} // namespace btree
} // namespace storage
} // namespace leanstore
