#include "Recovery.hpp"

#include "btree/TransactionKV.hpp"
#include "btree/core/BTreeNode.hpp"
#include "btree/core/BTreeWALPayload.hpp"
#include "buffer-manager/GuardedBufferFrame.hpp"
#include "concurrency-recovery/WALEntry.hpp"
#include "leanstore/LeanStore.hpp"
#include "sync/HybridGuard.hpp"

#include <expected>
#include <utility>

namespace leanstore::cr {

using namespace leanstore::storage;
using namespace leanstore::utils;
using namespace leanstore::storage::btree;

std::expected<void, utils::Error> Recovery::analysis() {
  // asume that each WALEntry is smaller than the page size
  utils::AlignedBuffer<512> alignedBuffer(mStore->mStoreOption.mPageSize);
  uint8_t* walEntryPtr = alignedBuffer.Get();
  uint64_t walEntrySize = sizeof(WALEntry);
  for (auto offset = mWalStartOffset; offset < mWalSize;) {
    uint64_t bytesRead = 0;
    if (auto res = readFromWalFile(offset, walEntrySize, walEntryPtr); !res) {
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
      if (auto res = readFromWalFile(leftOffset, leftSize, leftDest); !res) {
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
  auto* complexEntry = reinterpret_cast<WALEntryComplex*>(alignedBuffer.Get());

  for (auto offset = mWalStartOffset; offset < mWalSize;) {
    if (auto res = nextWalComplexToRedo(offset, complexEntry); !res) {
      return std::unexpected(res.error());
    }

    // get a buffer frame for the corresponding dirty page
    auto& bf = resolvePage(complexEntry->mPageId);
    SCOPED_DEFER(bf.header.mKeepInMemory = false);

    auto* walPayload = reinterpret_cast<WALPayload*>(complexEntry->payload);
    switch (walPayload->mType) {
    case WALPayload::TYPE::kWalInsert: {
      redoInsert(bf, complexEntry);
      break;
    }
    case WALPayload::TYPE::kWalTxInsert: {
      redoTxInsert(bf, complexEntry);
      break;
    }
    case WALPayload::TYPE::WALUpdate: {
      redoUpdate(bf, complexEntry);
      break;
    }
    case WALPayload::TYPE::WALTxUpdate: {
      redoTxUpdate(bf, complexEntry);
      break;
    }
    case WALPayload::TYPE::WALRemove: {
      redoRemove(bf, complexEntry);
      break;
    }
    case WALPayload::TYPE::WALTxRemove: {
      redoTxRemove(bf, complexEntry);
      break;
    }
    case WALPayload::TYPE::kWalInitPage: {
      redoInitPage(bf, complexEntry);
      break;
    }
    case WALPayload::TYPE::kWalSplitRoot: {
      redoSplitRoot(bf, complexEntry);
      break;
    }
    case WALPayload::TYPE::kWalSplitNonRoot: {
      redoSplitNonRoot(bf, complexEntry);
      break;
    }
    default: {
      DCHECK(false) << "Unhandled WALPayload::TYPE: "
                    << std::to_string(static_cast<uint64_t>(walPayload->mType));
    }
    }

    continue;
  }

  // Write all the resolved pages to disk
  for (auto it = mResolvedPages.begin(); it != mResolvedPages.end(); it++) {
    mStore->mBufferManager->WritePageSync(*it->second);
  }

  return {};
}

std::expected<void, utils::Error> Recovery::nextWalComplexToRedo(
    uint64_t& offset, WALEntryComplex* complexEntry) {
  const uint64_t walEntrySize = sizeof(WALEntry);
  auto* buff = reinterpret_cast<uint8_t*>(complexEntry);

  while (offset < mWalSize) {
    uint64_t bytesRead = 0;

    // read the header of the complex entry
    if (auto res = readFromWalFile(offset, walEntrySize, buff); !res) {
      return std::unexpected(res.error());
    }
    bytesRead += walEntrySize;

    // skip if not a complex entry
    auto* walEntry = reinterpret_cast<WALEntry*>(buff);
    if (walEntry->type != WALEntry::TYPE::COMPLEX) {
      offset += bytesRead;
      continue;
    }

    // read the rest of the complex entry
    auto leftOffset = offset + bytesRead;
    auto leftSize = walEntry->size - bytesRead;
    auto* leftDest = buff + bytesRead;
    if (auto res = readFromWalFile(leftOffset, leftSize, leftDest); !res) {
      return std::unexpected(res.error());
    }
    bytesRead += leftSize;
    offset += bytesRead;
    DCHECK(bytesRead == complexEntry->size)
        << "bytesRead should be equal to complexEntry->size"
        << ", offset=" << offset << ", bytesRead=" << bytesRead
        << ", complexEntry->size=" << complexEntry->size;

    // skip if the page is not dirty
    if (mDirtyPageTable.find(complexEntry->mPageId) == mDirtyPageTable.end() ||
        offset < mDirtyPageTable[complexEntry->mPageId]) {
      continue;
    }

    return {};
  }

  return {};
}

void Recovery::redoInsert(storage::BufferFrame& bf,
                          WALEntryComplex* complexEntry) {
  auto* walInsert = reinterpret_cast<WALInsert*>(complexEntry->payload);
  HybridGuard guard(&bf.header.mLatch);
  GuardedBufferFrame<BTreeNode> guardedNode(mStore->mBufferManager.get(),
                                            std::move(guard), &bf);

  int32_t slotId = -1;
  TransactionKV::InsertToNode(guardedNode, walInsert->GetKey(),
                              walInsert->GetVal(), complexEntry->mWorkerId,
                              complexEntry->mTxId, complexEntry->mTxMode,
                              slotId);
}

void Recovery::redoTxInsert(storage::BufferFrame& bf,
                            WALEntryComplex* complexEntry) {
  auto* walInsert = reinterpret_cast<WALTxInsert*>(complexEntry->payload);
  HybridGuard guard(&bf.header.mLatch);
  GuardedBufferFrame<BTreeNode> guardedNode(mStore->mBufferManager.get(),
                                            std::move(guard), &bf);

  int32_t slotId = -1;
  TransactionKV::InsertToNode(guardedNode, walInsert->GetKey(),
                              walInsert->GetVal(), complexEntry->mWorkerId,
                              complexEntry->mTxId, complexEntry->mTxMode,
                              slotId);
}

void Recovery::redoUpdate(storage::BufferFrame& bf [[maybe_unused]],
                          WALEntryComplex* complexEntry [[maybe_unused]]) {
  DCHECK(false) << "Unsupported";
}

void Recovery::redoTxUpdate(storage::BufferFrame& bf,
                            WALEntryComplex* complexEntry) {
  auto* wal = reinterpret_cast<WALTxUpdate*>(complexEntry->payload);
  HybridGuard guard(&bf.header.mLatch);
  GuardedBufferFrame<BTreeNode> guardedNode(mStore->mBufferManager.get(),
                                            std::move(guard), &bf);
  auto* updateDesc = wal->GetUpdateDesc();
  auto key = wal->GetKey();
  auto slotId = guardedNode->lowerBound<true>(key);
  DCHECK(slotId != -1) << "Key not found in WALTxUpdate";

  auto* mutRawVal = guardedNode->ValData(slotId);
  DCHECK(Tuple::From(mutRawVal)->mFormat == TupleFormat::kChained)
      << "Only chained tuple is supported";
  auto* chainedTuple = ChainedTuple::From(mutRawVal);

  // update the chained tuple
  chainedTuple->mWorkerId = complexEntry->mWorkerId;
  chainedTuple->mTxId = complexEntry->mTxId;
  chainedTuple->mCommandId ^= wal->mXorCommandId;

  // 1. copy xor result to buff
  auto deltaSize = wal->GetDeltaSize();
  uint8_t buff[deltaSize];
  std::memcpy(buff, wal->GetDeltaPtr(), deltaSize);

  // 2. calculate new value based on xor result and old value
  BasicKV::XorToBuffer(*updateDesc, chainedTuple->mPayload, buff);

  // 3. replace with the new value
  BasicKV::CopyToValue(*updateDesc, buff, chainedTuple->mPayload);
}

void Recovery::redoRemove(storage::BufferFrame& bf [[maybe_unused]],
                          WALEntryComplex* complexEntry [[maybe_unused]]) {
  DCHECK(false) << "Unsupported";
}

void Recovery::redoTxRemove(storage::BufferFrame& bf,
                            WALEntryComplex* complexEntry) {
  auto* wal = reinterpret_cast<WALTxRemove*>(complexEntry->payload);
  HybridGuard guard(&bf.header.mLatch);
  GuardedBufferFrame<BTreeNode> guardedNode(mStore->mBufferManager.get(),
                                            std::move(guard), &bf);
  auto key = wal->RemovedKey();
  auto slotId = guardedNode->lowerBound<true>(key);
  DCHECK(slotId != -1) << "Key not found, key="
                       << std::string((char*)key.data(), key.size());

  auto* mutRawVal = guardedNode->ValData(slotId);
  DCHECK(Tuple::From(mutRawVal)->mFormat == TupleFormat::kChained)
      << "Only chained tuple is supported";
  auto* chainedTuple = ChainedTuple::From(mutRawVal);

  // remove the chained tuple
  if (guardedNode->ValSize(slotId) > sizeof(ChainedTuple)) {
    guardedNode->shortenPayload(slotId, sizeof(ChainedTuple));
  }
  chainedTuple->mWorkerId = complexEntry->mWorkerId;
  chainedTuple->mTxId = complexEntry->mTxId;
  chainedTuple->mCommandId ^= wal->mPrevCommandId;
  chainedTuple->mIsTombstone = true;
}

void Recovery::redoInitPage(storage::BufferFrame& bf,
                            WALEntryComplex* complexEntry) {
  auto* walInitPage = reinterpret_cast<WALInitPage*>(complexEntry->payload);
  HybridGuard guard(&bf.header.mLatch);
  GuardedBufferFrame<BTreeNode> guardedNode(mStore->mBufferManager.get(),
                                            std::move(guard), &bf);
  auto xGuardedNode =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNode));
  xGuardedNode.InitPayload(walInitPage->mIsLeaf);
  bf.page.mBTreeId = complexEntry->mTreeId;
}

void Recovery::redoSplitRoot(storage::BufferFrame& bf,
                             WALEntryComplex* complexEntry) {
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
  auto sepInfo = BTreeNode::SeparatorInfo(wal->mSeparatorSize, wal->mSplitSlot,
                                          wal->mSeparatorTruncated);

  // move half of the old root to the new left,
  // insert separator key into new root,
  // update meta node to point to new root
  xGuardedNewRoot->mRightMostChildSwip = xGuardedOldRoot.bf();
  xGuardedOldRoot->Split(xGuardedNewRoot, xGuardedNewLeft, sepInfo);
  xGuardedMeta->mRightMostChildSwip = xGuardedNewRoot.bf();
}

void Recovery::redoSplitNonRoot(storage::BufferFrame& bf,
                                WALEntryComplex* complexEntry) {
  auto* wal = reinterpret_cast<WalSplitNonRoot*>(complexEntry->payload);

  // Resolve the old root
  auto childGuard = HybridGuard(&bf.header.mLatch);
  auto guardedChild = GuardedBufferFrame<BTreeNode>(
      mStore->mBufferManager.get(), std::move(childGuard), &bf);
  auto xGuardedChild =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedChild));

  // Resolve the new left
  auto newLeftPageId = wal->mNewLeft;
  auto& newLeftBf = resolvePage(newLeftPageId);
  auto newLeftGuard = HybridGuard(&newLeftBf.header.mLatch);
  auto guardedNewLeft = GuardedBufferFrame<BTreeNode>(
      mStore->mBufferManager.get(), std::move(newLeftGuard), &newLeftBf);
  auto xGuardedNewLeft =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNewLeft));

  // Resolve the parent node
  auto parentPageId = wal->mParentPageId;
  auto& parentBf = resolvePage(parentPageId);
  auto parentGuard = HybridGuard(&parentBf.header.mLatch);
  auto guardedParent = GuardedBufferFrame<BTreeNode>(
      mStore->mBufferManager.get(), std::move(parentGuard), &parentBf);
  auto xGuardedParent =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedParent));

  // Resolve sepInfo
  auto sepInfo = BTreeNode::SeparatorInfo(wal->mSeparatorSize, wal->mSplitSlot,
                                          wal->mSeparatorTruncated);

  const uint16_t spaceNeededForSeparator =
      guardedParent->spaceNeeded(sepInfo.mSize, sizeof(Swip));
  xGuardedParent->requestSpaceFor(spaceNeededForSeparator);
  xGuardedChild->Split(xGuardedParent, xGuardedNewLeft, sepInfo);
}

} // namespace leanstore::cr