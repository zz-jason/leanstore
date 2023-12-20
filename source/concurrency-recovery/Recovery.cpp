#include "Recovery.hpp"

namespace leanstore {
namespace cr {

using namespace leanstore::storage;
using namespace leanstore::storage::btree;

void Recovery::Analysis() {
  Page page; // asume that each WALEntry is smaller than the page size
  u8* walEntryPtr = (u8*)&page;
  u64 walEntrySize = sizeof(WALEntry);
  for (auto offset = mWalStartOffset; offset < mWalSize;) {
    auto bytesRead = ReadWalEntry(offset, walEntrySize, &page);
    auto walEntry = reinterpret_cast<WALEntry*>(&page);
    switch (walEntry->type) {
    case WALEntry::TYPE::TX_START: {
      DCHECK(bytesRead == walEntry->size);
      DCHECK(mActiveTxTable.find(walEntry->mTxId) == mActiveTxTable.end());
      mActiveTxTable.emplace(std::make_pair(walEntry->mTxId, offset));
      offset += bytesRead;
      continue;
    }
    case WALEntry::TYPE::TX_COMMIT: {
      DCHECK(bytesRead == walEntry->size);
      DCHECK(mActiveTxTable.find(walEntry->mTxId) != mActiveTxTable.end());
      mActiveTxTable[walEntry->mTxId] = offset;
      offset += bytesRead;
      continue;
    }
    case WALEntry::TYPE::TX_ABORT: {
      DCHECK(bytesRead == walEntry->size);
      DCHECK(mActiveTxTable.find(walEntry->mTxId) != mActiveTxTable.end());
      mActiveTxTable[walEntry->mTxId] = offset;
      offset += bytesRead;
      continue;
    }
    case WALEntry::TYPE::TX_FINISH: {
      DCHECK(bytesRead == walEntry->size);
      DCHECK(mActiveTxTable.find(walEntry->mTxId) != mActiveTxTable.end());
      mActiveTxTable.erase(walEntry->mTxId);
      offset += bytesRead;
      continue;
    }
    case WALEntry::TYPE::COMPLEX: {
      auto leftOffset = offset + bytesRead;
      auto leftSize = walEntry->size - bytesRead;
      auto leftDest = walEntryPtr + bytesRead;
      bytesRead += ReadWalEntry(leftOffset, leftSize, leftDest);

      auto complexEntry = reinterpret_cast<WALEntryComplex*>(walEntryPtr);
      DCHECK(bytesRead == complexEntry->size);
      DCHECK(mActiveTxTable.find(walEntry->mTxId) != mActiveTxTable.end());
      mActiveTxTable[walEntry->mTxId] = offset;

      auto& bf = ResolvePage(complexEntry->mPageId);

      if (complexEntry->mPSN >= bf.page.mPSN &&
          mDirtyPageTable.find(complexEntry->mPageId) ==
              mDirtyPageTable.end()) {
        // record the first WALEntry that makes the page dirty
        mDirtyPageTable.emplace(std::make_pair(complexEntry->mPageId, offset));
      }

      offset += bytesRead;
      continue;
    }
    default: {
      LOG(FATAL) << "Unrecognized WALEntry type: " << walEntry->TypeName();
    }
    }
  }
}

void Recovery::Redo() {
  Page page; // asume that each WALEntry is smaller than the page size
  u8* walEntryPtr = (u8*)&page;
  u64 walEntrySize = sizeof(WALEntry);

  for (auto offset = mWalStartOffset; offset < mWalSize;) {
    auto bytesRead = ReadWalEntry(offset, walEntrySize, walEntryPtr);
    auto walEntry = reinterpret_cast<WALEntry*>(walEntryPtr);
    if (walEntry->type != WALEntry::TYPE::COMPLEX) {
      offset += bytesRead;
      continue;
    }

    auto leftOffset = offset + bytesRead;
    auto leftSize = walEntry->size - bytesRead;
    auto leftDest = walEntryPtr + bytesRead;
    bytesRead += ReadWalEntry(leftOffset, leftSize, leftDest);

    auto complexEntry = reinterpret_cast<WALEntryComplex*>(walEntryPtr);
    DCHECK(bytesRead == complexEntry->size);
    if (mDirtyPageTable.find(complexEntry->mPageId) == mDirtyPageTable.end() ||
        offset < mDirtyPageTable[complexEntry->mPageId]) {
      offset += bytesRead;
      continue;
    }

    // TODO(jian.z): redo previous operations on the page
    auto& bf = ResolvePage(complexEntry->mPageId);
    SCOPED_DEFER(bf.header.mKeepInMemory = false);

    auto walPayload = reinterpret_cast<WALPayload*>(complexEntry->payload);
    switch (walPayload->type) {
    case WALPayload::TYPE::WALInsert: {
      auto walInsert = reinterpret_cast<WALInsert*>(complexEntry->payload);
      HybridGuard guard(&bf.header.mLatch);
      GuardedBufferFrame<BTreeNode> guardedNode(std::move(guard), &bf);

      s32 slotId = -1;
      BTreeVI::InsertToNode(guardedNode, walInsert->GetKey(),
                            walInsert->GetVal(), complexEntry->mWorkerId,
                            complexEntry->mTxId, complexEntry->mTxMode, slotId);
      break;
    }
    case WALPayload::TYPE::WALUpdate: {
      DCHECK(false) << "Unhandled WALPayload::TYPE: "
                    << std::to_string(static_cast<u64>(walPayload->type));
      break;
    }
    case WALPayload::TYPE::WALRemove: {
      DCHECK(false) << "Unhandled WALPayload::TYPE: "
                    << std::to_string(static_cast<u64>(walPayload->type));
      break;
    }
    case WALPayload::TYPE::WALAfterBeforeImage: {
      DCHECK(false) << "Unhandled WALPayload::TYPE: "
                    << std::to_string(static_cast<u64>(walPayload->type));
      break;
    }
    case WALPayload::TYPE::WALAfterImage: {
      DCHECK(false) << "Unhandled WALPayload::TYPE: "
                    << std::to_string(static_cast<u64>(walPayload->type));
      break;
    }
    case WALPayload::TYPE::WALLogicalSplit: {
      DCHECK(false) << "Unhandled WALPayload::TYPE: "
                    << std::to_string(static_cast<u64>(walPayload->type));
      break;
    }
    case WALPayload::TYPE::WALInitPage: {
      auto walInitPage = reinterpret_cast<WALInitPage*>(complexEntry->payload);
      HybridGuard guard(&bf.header.mLatch);
      GuardedBufferFrame<BTreeNode> guardedNode(std::move(guard), &bf);
      auto xGuardedNode =
          ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNode));
      xGuardedNode.InitPayload(walInitPage->mIsLeaf);
      bf.page.mBTreeId = complexEntry->mTreeId;
      break;
    }
    default: {
      DCHECK(false) << "Unhandled WALPayload::TYPE: "
                    << std::to_string(static_cast<u64>(walPayload->type));
    }
    }

    offset += bytesRead;
    continue;
  }

  // Write all the resolved pages to disk
  for (auto it = mResolvedPages.begin(); it != mResolvedPages.end(); it++) {
    BufferManager::sInstance->WritePageSync(*it->second);
  }
}

} // namespace cr
} // namespace leanstore