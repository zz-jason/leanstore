#include "Recovery.hpp"

#include "concurrency-recovery/WALEntry.hpp"
#include "storage/btree/TransactionKV.hpp"
#include "storage/btree/core/BTreeNode.hpp"
#include "storage/btree/core/BTreeWALPayload.hpp"
#include "storage/buffer-manager/GuardedBufferFrame.hpp"
#include "sync/HybridGuard.hpp"

#include <utility>

namespace leanstore {
namespace cr {

using namespace leanstore::storage;
using namespace leanstore::utils;
using namespace leanstore::storage::btree;

std::expected<void, utils::Error> Recovery::analysis() {
  // asume that each WALEntry is smaller than the page size
  utils::AlignedBuffer<512> alignedBuffer(mStore->mStoreOption.mPageSize);
  u8* walEntryPtr = alignedBuffer.Get();
  u64 walEntrySize = sizeof(WALEntry);
  for (auto offset = mWalStartOffset; offset < mWalSize;) {
    u64 bytesRead = 0;
    if (auto res = readWalEntry(offset, walEntrySize, walEntryPtr); !res) {
      return std::unexpected(res.error());
    }
    bytesRead += walEntrySize;

    auto* walEntry = reinterpret_cast<WALEntry*>(walEntryPtr);
    switch (walEntry->type) {
    case WALEntry::TYPE::TX_START: {
      DCHECK_EQ(bytesRead, walEntry->size);
      DCHECK(mActiveTxTable.find(walEntry->mTxId) == mActiveTxTable.end());
      auto txId = walEntry->mTxId;
      mActiveTxTable.emplace(txId, offset);
      offset += bytesRead;
      continue;
    }
    case WALEntry::TYPE::TX_COMMIT: {
      DCHECK_EQ(bytesRead, walEntry->size);
      DCHECK(mActiveTxTable.find(walEntry->mTxId) != mActiveTxTable.end());
      mActiveTxTable[walEntry->mTxId] = offset;
      offset += bytesRead;
      continue;
    }
    case WALEntry::TYPE::TX_ABORT: {
      DCHECK_EQ(bytesRead, walEntry->size);
      DCHECK(mActiveTxTable.find(walEntry->mTxId) != mActiveTxTable.end());
      mActiveTxTable[walEntry->mTxId] = offset;
      offset += bytesRead;
      continue;
    }
    case WALEntry::TYPE::TX_FINISH: {
      DCHECK_EQ(bytesRead, walEntry->size);
      DCHECK(mActiveTxTable.find(walEntry->mTxId) != mActiveTxTable.end());
      mActiveTxTable.erase(walEntry->mTxId);
      offset += bytesRead;
      continue;
    }
    case WALEntry::TYPE::COMPLEX: {
      auto leftOffset = offset + bytesRead;
      auto leftSize = walEntry->size - bytesRead;
      auto* leftDest = walEntryPtr + bytesRead;
      if (auto res = readWalEntry(leftOffset, leftSize, leftDest); !res) {
        return std::unexpected(res.error());
      }
      bytesRead += leftSize;

      auto* complexEntry = reinterpret_cast<WALEntryComplex*>(walEntryPtr);
      DCHECK_EQ(bytesRead, complexEntry->size);
      DCHECK(mActiveTxTable.find(walEntry->mTxId) != mActiveTxTable.end());
      mActiveTxTable[walEntry->mTxId] = offset;

      auto& bf = resolvePage(complexEntry->mPageId);

      if (complexEntry->mPSN >= bf.page.mPSN &&
          mDirtyPageTable.find(complexEntry->mPageId) ==
              mDirtyPageTable.end()) {
        // record the first WALEntry that makes the page dirty
        auto pageId = complexEntry->mPageId;
        mDirtyPageTable.emplace(pageId, offset);
      }

      offset += bytesRead;
      continue;
    }
    default: {
      LOG(FATAL) << "Unrecognized WALEntry type: " << walEntry->TypeName();
    }
    }
  }
  return {};
}

std::expected<void, utils::Error> Recovery::redo() {
  // asume that each WALEntry is smaller than the page size
  utils::AlignedBuffer<512> alignedBuffer(mStore->mStoreOption.mPageSize);
  u8* walEntryPtr = alignedBuffer.Get();
  u64 walEntrySize = sizeof(WALEntry);

  for (auto offset = mWalStartOffset; offset < mWalSize;) {
    auto bytesRead = 0;
    if (auto res = readWalEntry(offset, walEntrySize, walEntryPtr); !res) {
      return std::unexpected(res.error());
    }
    bytesRead += walEntrySize;

    auto* walEntry = reinterpret_cast<WALEntry*>(walEntryPtr);
    if (walEntry->type != WALEntry::TYPE::COMPLEX) {
      offset += bytesRead;
      continue;
    }

    auto leftOffset = offset + bytesRead;
    auto leftSize = walEntry->size - bytesRead;
    auto* leftDest = walEntryPtr + bytesRead;
    if (auto res = readWalEntry(leftOffset, leftSize, leftDest); !res) {
      return std::unexpected(res.error());
    }
    bytesRead += leftSize;

    auto* complexEntry = reinterpret_cast<WALEntryComplex*>(walEntryPtr);
    DCHECK(bytesRead == complexEntry->size);
    if (mDirtyPageTable.find(complexEntry->mPageId) == mDirtyPageTable.end() ||
        offset < mDirtyPageTable[complexEntry->mPageId]) {
      offset += bytesRead;
      continue;
    }

    // TODO(jian.z): redo previous operations on the page
    auto& bf = resolvePage(complexEntry->mPageId);
    SCOPED_DEFER(bf.header.mKeepInMemory = false);

    auto* walPayload = reinterpret_cast<WALPayload*>(complexEntry->payload);
    switch (walPayload->mType) {
    case WALPayload::TYPE::kWalInsert: {
      auto* walInsert = reinterpret_cast<WALInsert*>(complexEntry->payload);
      HybridGuard guard(&bf.header.mLatch);
      GuardedBufferFrame<BTreeNode> guardedNode(mStore->mBufferManager.get(),
                                                std::move(guard), &bf);

      s32 slotId = -1;
      TransactionKV::InsertToNode(guardedNode, walInsert->GetKey(),
                                  walInsert->GetVal(), complexEntry->mWorkerId,
                                  complexEntry->mTxId, complexEntry->mTxMode,
                                  slotId);
      break;
    }
    case WALPayload::TYPE::kWalTxInsert: {
      auto* walInsert = reinterpret_cast<WALTxInsert*>(complexEntry->payload);
      HybridGuard guard(&bf.header.mLatch);
      GuardedBufferFrame<BTreeNode> guardedNode(mStore->mBufferManager.get(),
                                                std::move(guard), &bf);

      s32 slotId = -1;
      TransactionKV::InsertToNode(guardedNode, walInsert->GetKey(),
                                  walInsert->GetVal(), complexEntry->mWorkerId,
                                  complexEntry->mTxId, complexEntry->mTxMode,
                                  slotId);
      break;
    }
    case WALPayload::TYPE::WALTxUpdate: {
      DCHECK(false) << "Unhandled WALPayload::TYPE: "
                    << std::to_string(static_cast<u64>(walPayload->mType));
      break;
    }
    case WALPayload::TYPE::WALRemove: {
      DCHECK(false) << "Unhandled WALPayload::TYPE: "
                    << std::to_string(static_cast<u64>(walPayload->mType));
      break;
    }
    case WALPayload::TYPE::WALTxRemove: {
      DCHECK(false) << "Unhandled WALPayload::TYPE: "
                    << std::to_string(static_cast<u64>(walPayload->mType));
      break;
    }
    case WALPayload::TYPE::kWalSplitNonRoot: {
      DCHECK(false) << "Unhandled WALPayload::TYPE: "
                    << std::to_string(static_cast<u64>(walPayload->mType));
      break;
    }
    case WALPayload::TYPE::kWalInitPage: {
      auto* walInitPage = reinterpret_cast<WALInitPage*>(complexEntry->payload);
      HybridGuard guard(&bf.header.mLatch);
      GuardedBufferFrame<BTreeNode> guardedNode(mStore->mBufferManager.get(),
                                                std::move(guard), &bf);
      auto xGuardedNode =
          ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNode));
      xGuardedNode.InitPayload(walInitPage->mIsLeaf);
      bf.page.mBTreeId = complexEntry->mTreeId;
      break;
    }
    case WALPayload::TYPE::kWalSplitRoot: {
      auto* wal = reinterpret_cast<WalSplitRoot*>(complexEntry->payload);

      // Resolve the old root
      auto oldRootGuard = HybridGuard(&bf.header.mLatch);
      auto guardedOldRoot = GuardedBufferFrame<BTreeNode>(
          mStore->mBufferManager.get(), std::move(oldRootGuard), &bf);
      auto xGuardedOldRoot =
          ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedOldRoot));

      // Resolve the new left
      auto newLeftPageId = wal->mNewLeft;
      auto& newLeftBf = resolvePage(newLeftPageId);
      auto newLeftGuard = HybridGuard(&newLeftBf.header.mLatch);
      auto guardedNewLeft = GuardedBufferFrame<BTreeNode>(
          mStore->mBufferManager.get(), std::move(newLeftGuard), &newLeftBf);
      auto xGuardedNewLeft =
          ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNewLeft));

      // Resolve the new root
      auto newRootPageId = wal->mNewRoot;
      auto& newRootBf = resolvePage(newRootPageId);
      auto newRootGuard = HybridGuard(&newRootBf.header.mLatch);
      auto guardedNewRoot = GuardedBufferFrame<BTreeNode>(
          mStore->mBufferManager.get(), std::move(newRootGuard), &newRootBf);
      auto xGuardedNewRoot =
          ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNewRoot));

      // Resolve the meta node
      auto metaPageId = wal->mMetaNode;
      auto& metaBf = resolvePage(metaPageId);
      auto metaGuard = HybridGuard(&metaBf.header.mLatch);
      auto guardedMeta = GuardedBufferFrame<BTreeNode>(
          mStore->mBufferManager.get(), std::move(metaGuard), &metaBf);
      auto xGuardedMeta =
          ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedMeta));

      // Resolve sepInfo
      auto sepInfo = BTreeNode::SeparatorInfo(
          wal->mSeparatorSize, wal->mSplitSlot, wal->mSeparatorTruncated);

      // move half of the old root to the new left,
      // insert separator key into new root,
      // update meta node to point to new root
      xGuardedNewRoot->mRightMostChildSwip = xGuardedOldRoot.bf();
      xGuardedOldRoot->Split(xGuardedNewRoot, xGuardedNewLeft, sepInfo);
      xGuardedMeta->mRightMostChildSwip = xGuardedNewRoot.bf();
      break;
    }
    default: {
      DCHECK(false) << "Unhandled WALPayload::TYPE: "
                    << std::to_string(static_cast<u64>(walPayload->mType));
    }
    }

    offset += bytesRead;
    continue;
  }

  // Write all the resolved pages to disk
  for (auto it = mResolvedPages.begin(); it != mResolvedPages.end(); it++) {
    mStore->mBufferManager->WritePageSync(*it->second);
  }

  return {};
}

} // namespace cr
} // namespace leanstore