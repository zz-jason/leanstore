#include "BasicKV.hpp"

#include "btree/core/BTreeGeneric.hpp"
#include "btree/core/BTreePessimisticExclusiveIterator.hpp"
#include "btree/core/BTreePessimisticSharedIterator.hpp"
#include "btree/core/BTreeWalPayload.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/LeanStore.hpp"
#include "sync/HybridLatch.hpp"
#include "utils/Log.hpp"
#include "utils/Misc.hpp"

#include <format>

using namespace std;
using namespace leanstore::storage;

namespace leanstore::storage::btree {

Result<BasicKV*> BasicKV::Create(leanstore::LeanStore* store,
                                 const std::string& treeName,
                                 BTreeConfig config) {
  auto [treePtr, treeId] = store->mTreeRegistry->CreateTree(treeName, [&]() {
    return std::unique_ptr<BufferManagedTree>(
        static_cast<BufferManagedTree*>(new BasicKV()));
  });
  if (treePtr == nullptr) {
    return std::unexpected<utils::Error>(
        utils::Error::General("Tree name has been taken"));
  }
  auto* tree = DownCast<BasicKV*>(treePtr);
  tree->Init(store, treeId, std::move(config));
  return tree;
}

OpCode BasicKV::Lookup(Slice key, ValCallback valCallback) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> guardedLeaf;
      FindLeafCanJump(key, guardedLeaf, LatchMode::kPessimisticShared);
      auto slotId = guardedLeaf->lowerBound<true>(key);
      if (slotId != -1) {
        valCallback(guardedLeaf->Value(slotId));
        guardedLeaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OpCode::kOK;
      }

      guardedLeaf.JumpIfModifiedByOthers();
      JUMPMU_RETURN OpCode::kNotFound;
    }
    JUMPMU_CATCH() {
      WorkerCounters::MyCounters().dt_restarts_read[mTreeId]++;
    }
  }
  UNREACHABLE();
  return OpCode::kOther;
}

bool BasicKV::IsRangeEmpty(Slice startKey, Slice endKey) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> guardedLeaf;
      FindLeafCanJump(startKey, guardedLeaf);

      Slice upperFence = guardedLeaf->GetUpperFence();
      LS_DCHECK(startKey >= guardedLeaf->GetLowerFence());

      if ((guardedLeaf->mUpperFence.mOffset == 0 || endKey <= upperFence) &&
          guardedLeaf->mNumSeps == 0) {
        int32_t pos = guardedLeaf->lowerBound<false>(startKey);
        if (pos == guardedLeaf->mNumSeps) {
          guardedLeaf.JumpIfModifiedByOthers();
          JUMPMU_RETURN true;
        }

        guardedLeaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN false;
      }

      guardedLeaf.JumpIfModifiedByOthers();
      JUMPMU_RETURN false;
    }
    JUMPMU_CATCH() {
    }
  }
  UNREACHABLE();
  return false;
}

OpCode BasicKV::ScanAsc(Slice startKey, ScanCallback callback) {
  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().dt_scan_asc[mTreeId]++;
  }

  JUMPMU_TRY() {
    auto iter = GetIterator();
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

OpCode BasicKV::ScanDesc(Slice scanKey, ScanCallback callback) {
  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().dt_scan_desc[mTreeId]++;
  }
  JUMPMU_TRY() {
    auto iter = GetIterator();
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

OpCode BasicKV::Insert(Slice key, Slice val) {
  JUMPMU_TRY() {
    auto xIter = GetExclusiveIterator();
    auto ret = xIter.InsertKV(key, val);

    if (ret == OpCode::kDuplicated) {
      Log::Info("Insert duplicated, workerId={}, key={}, treeId={}",
                cr::Worker::My().mWorkerId, key.ToString(), mTreeId);
      JUMPMU_RETURN OpCode::kDuplicated;
    }

    if (ret != OpCode::kOK) {
      Log::Info("Insert failed, workerId={}, key={}, ret={}",
                cr::Worker::My().mWorkerId, key.ToString(), ToString(ret));
      JUMPMU_RETURN ret;
    }

    if (mConfig.mEnableWal) {
      auto walSize = key.length() + val.length();
      xIter.mGuardedLeaf.WriteWal<WalInsert>(walSize, key, val);
    }
  }
  JUMPMU_CATCH() {
  }
  return OpCode::kOK;
}

OpCode BasicKV::PrefixLookup(Slice key, PrefixLookupCallback callback) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> guardedLeaf;
      FindLeafCanJump(key, guardedLeaf);

      bool isEqual = false;
      int16_t cur = guardedLeaf->lowerBound<false>(key, &isEqual);
      if (isEqual) {
        callback(key, guardedLeaf->Value(cur));
        guardedLeaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OpCode::kOK;
      }

      if (cur < guardedLeaf->mNumSeps) {
        auto fullKeySize = guardedLeaf->getFullKeyLen(cur);
        auto fullKeyBuf = utils::JumpScopedArray<uint8_t>(fullKeySize);
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

OpCode BasicKV::PrefixLookupForPrev(Slice key, PrefixLookupCallback callback) {
  while (true) {
    JUMPMU_TRY() {
      GuardedBufferFrame<BTreeNode> guardedLeaf;
      FindLeafCanJump(key, guardedLeaf);

      bool isEqual = false;
      int16_t cur = guardedLeaf->lowerBound<false>(key, &isEqual);
      if (isEqual == true) {
        callback(key, guardedLeaf->Value(cur));
        guardedLeaf.JumpIfModifiedByOthers();
        JUMPMU_RETURN OpCode::kOK;
      }

      if (cur > 0) {
        cur -= 1;
        auto fullKeySize = guardedLeaf->getFullKeyLen(cur);
        auto fullKeyBuf = utils::JumpScopedArray<uint8_t>(fullKeySize);
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

OpCode BasicKV::UpdatePartial(Slice key, MutValCallback updateCallBack,
                              UpdateDesc& updateDesc) {
  JUMPMU_TRY() {
    auto xIter = GetExclusiveIterator();
    if (!xIter.SeekExact(key)) {
      JUMPMU_RETURN OpCode::kNotFound;
    }
    auto currentVal = xIter.MutableVal();
    if (mConfig.mEnableWal) {
      LS_DCHECK(updateDesc.mNumSlots > 0);
      auto sizeOfDescAndDelta = updateDesc.SizeWithDelta();
      auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WalUpdate>(
          key.length() + sizeOfDescAndDelta);
      walHandler->mType = WalPayload::Type::kWalUpdate;
      walHandler->mKeySize = key.length();
      walHandler->mDeltaLength = sizeOfDescAndDelta;
      auto* walPtr = walHandler->mPayload;
      std::memcpy(walPtr, key.data(), key.length());
      walPtr += key.length();
      std::memcpy(walPtr, &updateDesc, updateDesc.Size());
      walPtr += updateDesc.Size();

      // 1. copy old value to wal buffer
      BasicKV::CopyToBuffer(updateDesc, currentVal.Data(), walPtr);

      // 2. update with the new value
      updateCallBack(currentVal);

      // 3. xor with new value, store the result in wal buffer
      BasicKV::XorToBuffer(updateDesc, currentVal.Data(), walPtr);
      walHandler.SubmitWal();
    } else {
      // The actual update by the client
      updateCallBack(currentVal);
    }

    xIter.UpdateContentionStats();
    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BasicKV::Remove(Slice key) {
  JUMPMU_TRY() {
    auto xIter = GetExclusiveIterator();
    if (!xIter.SeekExact(key)) {
      JUMPMU_RETURN OpCode::kNotFound;
    }

    Slice value = xIter.value();
    if (mConfig.mEnableWal) {
      auto walHandler = xIter.mGuardedLeaf.ReserveWALPayload<WalRemove>(
          key.size() + value.size(), key, value);
      walHandler.SubmitWal();
    }
    auto ret = xIter.RemoveCurrent();
    ENSURE(ret == OpCode::kOK);
    xIter.TryMergeIfNeeded();
    JUMPMU_RETURN OpCode::kOK;
  }
  JUMPMU_CATCH() {
  }
  UNREACHABLE();
  return OpCode::kOther;
}

OpCode BasicKV::RangeRemove(Slice startKey, Slice endKey, bool pageWise) {
  JUMPMU_TRY() {
    auto xIter = GetExclusiveIterator();
    xIter.SetExitLeafCallback([&](GuardedBufferFrame<BTreeNode>& guardedLeaf) {
      if (guardedLeaf->FreeSpaceAfterCompaction() >=
          BTreeNode::UnderFullSize()) {
        xIter.SetCleanUpCallback([&, toMerge = guardedLeaf.mBf] {
          JUMPMU_TRY() {
            this->TryMergeMayJump(*toMerge);
          }
          JUMPMU_CATCH() {
          }
        });
      }
    });

    ENSURE(mConfig.mEnableWal == false);
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
          auto ret = xIter.RemoveCurrent();
          ENSURE(ret == OpCode::kOK);
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
    xIter.SetEnterLeafCallback([&](GuardedBufferFrame<BTreeNode>& guardedLeaf) {
      if (guardedLeaf->mNumSeps == 0) {
        return;
      }

      // page start key
      auto firstKeySize = guardedLeaf->getFullKeyLen(0);
      auto firstKey = utils::JumpScopedArray<uint8_t>(firstKeySize);
      guardedLeaf->copyFullKey(0, firstKey->get());
      Slice pageStartKey(firstKey->get(), firstKeySize);

      // page end key
      auto lastKeySize = guardedLeaf->getFullKeyLen(guardedLeaf->mNumSeps - 1);
      auto lastKey = utils::JumpScopedArray<uint8_t>(lastKeySize);
      guardedLeaf->copyFullKey(guardedLeaf->mNumSeps - 1, lastKey->get());
      Slice pageEndKey(lastKey->get(), lastKeySize);

      if (pageStartKey >= startKey && pageEndKey <= endKey) {
        // Purge the whole page
        COUNTERS_BLOCK() {
          WorkerCounters::MyCounters().dt_range_removed[mTreeId] +=
              guardedLeaf->mNumSeps;
        }
        guardedLeaf->Reset();
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

uint64_t BasicKV::CountEntries() {
  return BTreeGeneric::CountEntries();
}

} // namespace leanstore::storage::btree
