#include "BTreeLL.hpp"

#include "KVInterface.hpp"
#include "storage/btree/core/BTreeExclusiveIterator.hpp"
#include "storage/btree/core/BTreeSharedIterator.hpp"
#include "utils/Misc.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

using namespace std;
using namespace leanstore::storage;

namespace leanstore {
namespace storage {
namespace btree {

OpCode BTreeLL::Lookup(Slice key, ValCallback valCallback) {
  DCHECK(cr::Worker::my().IsTxStarted());
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> guardedLeaf;
      FindLeafCanJump(key, guardedLeaf);
      s16 slotId = guardedLeaf->lowerBound<true>(key);
      if (slotId != -1) {
        valCallback(guardedLeaf->Value(slotId));
        guardedLeaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OpCode::kOK;
      }

      guardedLeaf.JumpIfModifiedByOthers();
      JUMPMU_RETURN OpCode::kNotFound;
    }
    JUMPMU_CATCH() {
      DLOG(WARNING) << "BTreeLL::Lookup retried";
      WorkerCounters::MyCounters().dt_restarts_read[mTreeId]++;
    }
  }
  UNREACHABLE();
  return OpCode::kOther;
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

OpCode BTreeLL::ScanAsc(Slice startKey, ScanCallback callback) {
  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().dt_scan_asc[mTreeId]++;
  }

  JUMPMU_TRY() {
    BTreeSharedIterator iter(*static_cast<BTreeGeneric*>(this));
    for (bool succeed = iter.Seek(startKey); succeed; succeed = iter.Next()) {
      iter.AssembleKey();
      auto key = iter.key();
      auto value = iter.value();
      if (!callback(key, value)) {
        break;
      }
    }
    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BTreeLL::ScanDesc(Slice scanKey, ScanCallback callback) {
  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().dt_scan_desc[mTreeId]++;
  }
  JUMPMU_TRY() {
    BTreeSharedIterator iter(*static_cast<BTreeGeneric*>(this));
    if (!iter.SeekForPrev(scanKey)) {
      JUMPMU_RETURN OpCode::kNotFound;
    }
    while (true) {
      iter.AssembleKey();
      auto key = iter.key();
      auto value = iter.value();
      if (!callback(key, value)) {
        JUMPMU_RETURN OpCode::kOK;
      }
      if (!iter.Prev()) {
        JUMPMU_RETURN OpCode::kNotFound;
      }
    }
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BTreeLL::insert(Slice key, Slice val) {
  DCHECK(cr::Worker::my().IsTxStarted());
  if (config.mEnableWal) {
    cr::Worker::my().mLogging.WalEnsureEnoughSpace(FLAGS_page_size * 1);
  }

  JUMPMU_TRY() {
    BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
    OpCode ret = iterator.InsertKV(key, val);
    ENSURE(ret == OpCode::kOK);
    if (config.mEnableWal) {
      auto walSize = key.length() + val.length();
      auto walHandler =
          iterator.mGuardedLeaf.ReserveWALPayload<WALInsert>(walSize, key, val);
      walHandler.SubmitWal();
    } else {
      iterator.MarkAsDirty();
    }
    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BTreeLL::prefixLookup(Slice key, PrefixLookupCallback callback) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> guardedLeaf;
      FindLeafCanJump(key, guardedLeaf);

      bool isEqual = false;
      s16 cur = guardedLeaf->lowerBound<false>(key, &isEqual);
      if (isEqual) {
        callback(key, guardedLeaf->Value(cur));
        guardedLeaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OpCode::kOK;
      }

      if (cur < guardedLeaf->mNumSeps) {
        auto fullKeySize = guardedLeaf->getFullKeyLen(cur);
        auto fullKeyBuf = utils::JumpScopedArray<u8>(fullKeySize);
        guardedLeaf->copyFullKey(cur, fullKeyBuf->get());
        guardedLeaf.JumpIfModifiedByOthers();

        callback(Slice(fullKeyBuf->get(), fullKeySize),
                 guardedLeaf->Value(cur));
        guardedLeaf.JumpIfModifiedByOthers();

        JUMPMU_RETURN OpCode::kOK;
      }

      OpCode ret = ScanAsc(key, [&](Slice scannedKey, Slice scannedVal) {
        callback(scannedKey, scannedVal);
        return false;
      });
      JUMPMU_RETURN ret;
    }
    JUMPMU_CATCH() {
      WorkerCounters::MyCounters().dt_restarts_read[mTreeId]++;
    }
  }

  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BTreeLL::prefixLookupForPrev(Slice key, PrefixLookupCallback callback) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> guardedLeaf;
      FindLeafCanJump(key, guardedLeaf);

      bool isEqual = false;
      s16 cur = guardedLeaf->lowerBound<false>(key, &isEqual);
      if (isEqual == true) {
        callback(key, guardedLeaf->Value(cur));
        guardedLeaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OpCode::kOK;
      }

      if (cur > 0) {
        cur -= 1;
        auto fullKeySize = guardedLeaf->getFullKeyLen(cur);
        auto fullKeyBuf = utils::JumpScopedArray<u8>(fullKeySize);
        guardedLeaf->copyFullKey(cur, fullKeyBuf->get());
        guardedLeaf.JumpIfModifiedByOthers();

        callback(Slice(fullKeyBuf->get(), fullKeySize),
                 guardedLeaf->Value(cur));
        guardedLeaf.JumpIfModifiedByOthers();

        JUMPMU_RETURN OpCode::kOK;
      }

      OpCode ret = ScanDesc(key, [&](Slice scannedKey, Slice scannedVal) {
        callback(scannedKey, scannedVal);
        return false;
      });
      JUMPMU_RETURN ret;
    }
    JUMPMU_CATCH() {
      WorkerCounters::MyCounters().dt_restarts_read[mTreeId]++;
    }
  }

  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BTreeLL::updateSameSizeInPlace(Slice key, MutValCallback updateCallBack,
                                      UpdateDesc& updateDesc) {
  DCHECK(cr::Worker::my().IsTxStarted());
  if (config.mEnableWal) {
    cr::Worker::my().mLogging.WalEnsureEnoughSpace(FLAGS_page_size);
  }

  JUMPMU_TRY() {
    BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
    if (!xIter.SeekExact(key)) {
      JUMPMU_RETURN OpCode::kNotFound;
    }
    auto currentVal = xIter.MutableVal();
    if (config.mEnableWal) {
      DCHECK(updateDesc.mNumSlots > 0);
      auto sizeOfDescAndDelta = updateDesc.SizeWithDelta();
      auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WALUpdate>(
          key.length() + sizeOfDescAndDelta);
      walHandler->mType = WALPayload::TYPE::WALUpdate;
      walHandler->mKeySize = key.length();
      walHandler->mDeltaLength = sizeOfDescAndDelta;
      auto* walPtr = walHandler->payload;
      std::memcpy(walPtr, key.data(), key.length());
      walPtr += key.length();
      std::memcpy(walPtr, &updateDesc, updateDesc.Size());
      walPtr += updateDesc.Size();

      // 1. copy old value to wal buffer
      BTreeLL::CopyToBuffer(updateDesc, currentVal.Data(), walPtr);

      // 2. update with the new value
      updateCallBack(currentVal);

      // 3. xor with new value, store the result in wal buffer
      BTreeLL::XorToBuffer(updateDesc, currentVal.Data(), walPtr);
      walHandler.SubmitWal();
    } else {
      // The actual update by the client
      updateCallBack(currentVal);
    }

    xIter.MarkAsDirty();
    xIter.UpdateContentionStats();
    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BTreeLL::remove(Slice key) {
  if (config.mEnableWal) {
    cr::Worker::my().mLogging.WalEnsureEnoughSpace(FLAGS_page_size);
  }
  JUMPMU_TRY() {
    BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
    if (!xIter.SeekExact(key)) {
      JUMPMU_RETURN OpCode::kNotFound;
    }

    Slice value = xIter.value();
    if (config.mEnableWal) {
      auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WALRemove>(
          key.size() + value.size(), key, value);
      walHandler.SubmitWal();
    }
    xIter.MarkAsDirty();
    auto ret = xIter.removeCurrent();
    ENSURE(ret == OpCode::kOK);
    xIter.mergeIfNeeded();
    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BTreeLL::rangeRemove(Slice startKey, Slice endKey, bool pageWise) {
  JUMPMU_TRY() {
    BTreeExclusiveIterator xIter(*static_cast<BTreeGeneric*>(this));
    xIter.exitLeafCallback([&](GuardedBufferFrame<BTreeNode>& guardedLeaf) {
      if (guardedLeaf->freeSpaceAfterCompaction() >=
          BTreeNode::UnderFullSize()) {
        xIter.cleanUpCallback([&, toMerge = guardedLeaf.mBf] {
          JUMPMU_TRY() {
            this->TryMergeMayJump(*toMerge);
          }
          JUMPMU_CATCH() {
          }
        });
      }
    });

    ENSURE(config.mEnableWal == false);
    if (!pageWise) {
      if (!xIter.Seek(startKey)) {
        JUMPMU_RETURN OpCode::kNotFound;
      }
      while (true) {
        xIter.AssembleKey();
        auto currentKey = xIter.key();
        if (currentKey >= startKey && currentKey <= endKey) {
          COUNTERS_BLOCK() {
            WorkerCounters::MyCounters().dt_range_removed[mTreeId]++;
          }
          auto ret = xIter.removeCurrent();
          ENSURE(ret == OpCode::kOK);
          xIter.MarkAsDirty();
          if (xIter.mSlotId == xIter.mGuardedLeaf->mNumSeps) {
            ret = xIter.Next() ? OpCode::kOK : OpCode::kNotFound;
          }
        } else {
          break;
        }
      }
      JUMPMU_RETURN OpCode::kOK;
    }

    bool didPurgeFullPage = false;
    xIter.enterLeafCallback([&](GuardedBufferFrame<BTreeNode>& guardedLeaf) {
      if (guardedLeaf->mNumSeps == 0) {
        return;
      }

      // page start key
      auto firstKeySize = guardedLeaf->getFullKeyLen(0);
      auto firstKey = utils::JumpScopedArray<u8>(firstKeySize);
      guardedLeaf->copyFullKey(0, firstKey->get());
      Slice pageStartKey(firstKey->get(), firstKeySize);

      // page end key
      auto lastKeySize = guardedLeaf->getFullKeyLen(guardedLeaf->mNumSeps - 1);
      auto lastKey = utils::JumpScopedArray<u8>(lastKeySize);
      guardedLeaf->copyFullKey(guardedLeaf->mNumSeps - 1, lastKey->get());
      Slice pageEndKey(lastKey->get(), lastKeySize);

      if (pageStartKey >= startKey && pageEndKey <= endKey) {
        // Purge the whole page
        COUNTERS_BLOCK() {
          WorkerCounters::MyCounters().dt_range_removed[mTreeId] +=
              guardedLeaf->mNumSeps;
        }
        guardedLeaf->reset();
        xIter.MarkAsDirty();
        didPurgeFullPage = true;
      }
    });

    while (true) {
      xIter.Seek(startKey);
      if (didPurgeFullPage) {
        didPurgeFullPage = false;
        continue;
      }
      break;
    }
  }

  JUMPMU_CATCH() {
  }
  return OpCode::kOK;
}

u64 BTreeLL::countEntries() {
  return BTreeGeneric::countEntries();
}

} // namespace btree
} // namespace storage
} // namespace leanstore
