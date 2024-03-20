#include "concurrency/Recovery.hpp"

#include "btree/TransactionKV.hpp"
#include "btree/core/BTreeNode.hpp"
#include "btree/core/BTreeWalPayload.hpp"
#include "buffer-manager/GuardedBufferFrame.hpp"
#include "concurrency/WalEntry.hpp"
#include "leanstore/LeanStore.hpp"
#include "sync/HybridGuard.hpp"

#include <cstdint>
#include <expected>
#include <utility>

namespace leanstore::cr {

using namespace leanstore::storage;
using namespace leanstore::utils;
using namespace leanstore::storage::btree;

std::expected<void, utils::Error> Recovery::analysis() {
  // asume that each WalEntry is smaller than the page size
  utils::AlignedBuffer<512> alignedBuffer(mStore->mStoreOption.mPageSize);
  uint8_t* walEntryPtr = alignedBuffer.Get();
  for (auto offset = mWalStartOffset; offset < mWalSize;) {
    if (auto res = readWalEntry(offset, walEntryPtr); !res) {
      return std::unexpected(res.error());
    }
    auto* walEntry = reinterpret_cast<WalEntry*>(walEntryPtr);
    switch (walEntry->mType) {
    case WalEntry::Type::kTxAbort: {
      auto* wal = reinterpret_cast<WalTxAbort*>(walEntryPtr);
      DCHECK(mActiveTxTable.find(wal->mTxId) != mActiveTxTable.end());
      mActiveTxTable[wal->mTxId] = offset;
      continue;
    }
    case WalEntry::Type::kTxFinish: {
      auto* wal = reinterpret_cast<WalTxFinish*>(walEntryPtr);
      DCHECK(mActiveTxTable.find(wal->mTxId) != mActiveTxTable.end());
      mActiveTxTable.erase(wal->mTxId);
      continue;
    }
    case WalEntry::Type::kCarriageReturn: {
      // do nothing
      continue;
    }
    case WalEntry::Type::kComplex: {
      auto* wal = reinterpret_cast<WalEntryComplex*>(walEntryPtr);
      mActiveTxTable[wal->mTxId] = offset;
      auto& bf = resolvePage(wal->mPageId);
      if (wal->mGsn >= bf.mPage.mGSN &&
          mDirtyPageTable.find(wal->mPageId) == mDirtyPageTable.end()) {
        // record the first WalEntry that makes the page dirty
        auto pageId = wal->mPageId;
        mDirtyPageTable.emplace(pageId, offset);
      }
      continue;
    }
    default: {
      LOG(FATAL) << "Unrecognized WalEntry type: " << walEntry->TypeName();
    }
    }
  }
  return {};
}

std::expected<void, utils::Error> Recovery::redo() {
  // asume that each WalEntry is smaller than the page size
  utils::AlignedBuffer<512> alignedBuffer(mStore->mStoreOption.mPageSize);
  auto* complexEntry = reinterpret_cast<WalEntryComplex*>(alignedBuffer.Get());

  for (auto offset = mWalStartOffset; offset < mWalSize;) {
    auto res = nextWalComplexToRedo(offset, complexEntry);
    if (!res) {
      // met error
      return std::unexpected(res.error());
    }

    if (!res.value()) {
      // no more complex entry to redo
      break;
    }

    // get a buffer frame for the corresponding dirty page
    auto& bf = resolvePage(complexEntry->mPageId);
    SCOPED_DEFER(bf.mHeader.mKeepInMemory = false);

    auto* walPayload = reinterpret_cast<WalPayload*>(complexEntry->mPayload);
    switch (walPayload->mType) {
    case WalPayload::Type::kWalInsert: {
      redoInsert(bf, complexEntry);
      break;
    }
    case WalPayload::Type::kWalTxInsert: {
      redoTxInsert(bf, complexEntry);
      break;
    }
    case WalPayload::Type::kWalUpdate: {
      redoUpdate(bf, complexEntry);
      break;
    }
    case WalPayload::Type::kWalTxUpdate: {
      redoTxUpdate(bf, complexEntry);
      break;
    }
    case WalPayload::Type::kWalRemove: {
      redoRemove(bf, complexEntry);
      break;
    }
    case WalPayload::Type::kWalTxRemove: {
      redoTxRemove(bf, complexEntry);
      break;
    }
    case WalPayload::Type::kWalInitPage: {
      redoInitPage(bf, complexEntry);
      break;
    }
    case WalPayload::Type::kWalSplitRoot: {
      redoSplitRoot(bf, complexEntry);
      break;
    }
    case WalPayload::Type::kWalSplitNonRoot: {
      redoSplitNonRoot(bf, complexEntry);
      break;
    }
    default: {
      DCHECK(false) << "Unhandled WalPayload::Type: "
                    << std::to_string(static_cast<uint64_t>(walPayload->mType));
    }
    }
  }

  // Write all the resolved pages to disk
  for (auto it = mResolvedPages.begin(); it != mResolvedPages.end(); it++) {
    auto res = mStore->mBufferManager->WritePageSync(*it->second);
    if (!res) {
      return res;
    }
  }

  return {};
}

std::expected<bool, utils::Error> Recovery::nextWalComplexToRedo(
    uint64_t& offset, WalEntryComplex* complexEntry) {
  auto* buff = reinterpret_cast<uint8_t*>(complexEntry);
  while (offset < mWalSize) {
    // read a WalEntry
    if (auto res = readWalEntry(offset, buff); !res) {
      return std::unexpected(res.error());
    }

    // skip if not a complex entry
    auto* walEntry = reinterpret_cast<WalEntry*>(buff);
    if (walEntry->mType != WalEntry::Type::kComplex) {
      continue;
    }

    // skip if the page is not dirty
    if (mDirtyPageTable.find(complexEntry->mPageId) == mDirtyPageTable.end() ||
        offset < mDirtyPageTable[complexEntry->mPageId]) {
      continue;
    }

    // found a complex entry to redo
    return true;
  }

  // no more complex entry to redo
  return false;
}

void Recovery::redoInsert(storage::BufferFrame& bf,
                          WalEntryComplex* complexEntry) {
  auto* walInsert = reinterpret_cast<WalInsert*>(complexEntry->mPayload);
  HybridGuard guard(&bf.mHeader.mLatch);
  GuardedBufferFrame<BTreeNode> guardedNode(mStore->mBufferManager.get(),
                                            std::move(guard), &bf);

  int32_t slotId = -1;
  TransactionKV::InsertToNode(guardedNode, walInsert->GetKey(),
                              walInsert->GetVal(), complexEntry->mWorkerId,
                              complexEntry->mTxId, slotId);
}

void Recovery::redoTxInsert(storage::BufferFrame& bf,
                            WalEntryComplex* complexEntry) {
  auto* walInsert = reinterpret_cast<WalTxInsert*>(complexEntry->mPayload);
  HybridGuard guard(&bf.mHeader.mLatch);
  GuardedBufferFrame<BTreeNode> guardedNode(mStore->mBufferManager.get(),
                                            std::move(guard), &bf);

  int32_t slotId = -1;
  TransactionKV::InsertToNode(guardedNode, walInsert->GetKey(),
                              walInsert->GetVal(), complexEntry->mWorkerId,
                              complexEntry->mTxId, slotId);
}

void Recovery::redoUpdate(storage::BufferFrame& bf [[maybe_unused]],
                          WalEntryComplex* complexEntry [[maybe_unused]]) {
  DCHECK(false) << "Unsupported";
}

void Recovery::redoTxUpdate(storage::BufferFrame& bf,
                            WalEntryComplex* complexEntry) {
  auto* wal = reinterpret_cast<WalTxUpdate*>(complexEntry->mPayload);
  HybridGuard guard(&bf.mHeader.mLatch);
  GuardedBufferFrame<BTreeNode> guardedNode(mStore->mBufferManager.get(),
                                            std::move(guard), &bf);
  auto* updateDesc = wal->GetUpdateDesc();
  auto key = wal->GetKey();
  auto slotId = guardedNode->lowerBound<true>(key);
  DCHECK(slotId != -1) << "Key not found in WalTxUpdate";

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
                          WalEntryComplex* complexEntry [[maybe_unused]]) {
  DCHECK(false) << "Unsupported";
}

void Recovery::redoTxRemove(storage::BufferFrame& bf,
                            WalEntryComplex* complexEntry) {
  auto* wal = reinterpret_cast<WalTxRemove*>(complexEntry->mPayload);
  HybridGuard guard(&bf.mHeader.mLatch);
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
                            WalEntryComplex* complexEntry) {
  auto* walInitPage = reinterpret_cast<WalInitPage*>(complexEntry->mPayload);
  HybridGuard guard(&bf.mHeader.mLatch);
  GuardedBufferFrame<BTreeNode> guardedNode(mStore->mBufferManager.get(),
                                            std::move(guard), &bf);
  auto xGuardedNode =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNode));
  xGuardedNode.InitPayload(walInitPage->mIsLeaf);
  bf.mPage.mBTreeId = complexEntry->mTreeId;
}

void Recovery::redoSplitRoot(storage::BufferFrame& bf,
                             WalEntryComplex* complexEntry) {
  auto* wal = reinterpret_cast<WalSplitRoot*>(complexEntry->mPayload);

  // Resolve the old root
  auto oldRootGuard = HybridGuard(&bf.mHeader.mLatch);
  auto guardedOldRoot = GuardedBufferFrame<BTreeNode>(
      mStore->mBufferManager.get(), std::move(oldRootGuard), &bf);
  auto xGuardedOldRoot =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedOldRoot));

  // Resolve the new left
  auto newLeftPageId = wal->mNewLeft;
  auto& newLeftBf = resolvePage(newLeftPageId);
  auto newLeftGuard = HybridGuard(&newLeftBf.mHeader.mLatch);
  auto guardedNewLeft = GuardedBufferFrame<BTreeNode>(
      mStore->mBufferManager.get(), std::move(newLeftGuard), &newLeftBf);
  auto xGuardedNewLeft =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNewLeft));

  // Resolve the new root
  auto newRootPageId = wal->mNewRoot;
  auto& newRootBf = resolvePage(newRootPageId);
  auto newRootGuard = HybridGuard(&newRootBf.mHeader.mLatch);
  auto guardedNewRoot = GuardedBufferFrame<BTreeNode>(
      mStore->mBufferManager.get(), std::move(newRootGuard), &newRootBf);
  auto xGuardedNewRoot =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNewRoot));

  // Resolve the meta node
  auto metaPageId = wal->mMetaNode;
  auto& metaBf = resolvePage(metaPageId);
  auto metaGuard = HybridGuard(&metaBf.mHeader.mLatch);
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
                                WalEntryComplex* complexEntry) {
  auto* wal = reinterpret_cast<WalSplitNonRoot*>(complexEntry->mPayload);

  // Resolve the old root
  auto childGuard = HybridGuard(&bf.mHeader.mLatch);
  auto guardedChild = GuardedBufferFrame<BTreeNode>(
      mStore->mBufferManager.get(), std::move(childGuard), &bf);
  auto xGuardedChild =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedChild));

  // Resolve the new left
  auto newLeftPageId = wal->mNewLeft;
  auto& newLeftBf = resolvePage(newLeftPageId);
  auto newLeftGuard = HybridGuard(&newLeftBf.mHeader.mLatch);
  auto guardedNewLeft = GuardedBufferFrame<BTreeNode>(
      mStore->mBufferManager.get(), std::move(newLeftGuard), &newLeftBf);
  auto xGuardedNewLeft =
      ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guardedNewLeft));

  // Resolve the parent node
  auto parentPageId = wal->mParentPageId;
  auto& parentBf = resolvePage(parentPageId);
  auto parentGuard = HybridGuard(&parentBf.mHeader.mLatch);
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

/// Read a WalEntry from the WAL file
std::expected<void, utils::Error> Recovery::readWalEntry(uint64_t& offset,
                                                         uint8_t* dest) {
  // read the WalEntry
  auto walEntrySize = sizeof(WalEntry);
  if (auto res = readFromWalFile(offset, walEntrySize, dest); !res) {
    return std::unexpected(res.error());
  }

  switch (reinterpret_cast<WalEntry*>(dest)->mType) {
  case leanstore::cr::WalEntry::Type::kTxAbort: {
    auto left = sizeof(WalTxAbort) - walEntrySize;
    auto res =
        readFromWalFile(offset + walEntrySize, left, dest + walEntrySize);
    if (!res) {
      return std::unexpected(res.error());
    }
    offset += sizeof(WalTxAbort);
    return {};
  }
  case leanstore::cr::WalEntry::Type::kTxFinish: {
    auto left = sizeof(WalTxFinish) - walEntrySize;
    auto res =
        readFromWalFile(offset + walEntrySize, left, dest + walEntrySize);
    if (!res) {
      return std::unexpected(res.error());
    }
    offset += sizeof(WalTxFinish);
    return {};
  }
  case leanstore::cr::WalEntry::Type::kCarriageReturn: {
    auto left = sizeof(WalCarriageReturn) - walEntrySize;
    auto res =
        readFromWalFile(offset + walEntrySize, left, dest + walEntrySize);
    if (!res) {
      return std::unexpected(res.error());
    }
    offset += reinterpret_cast<WalCarriageReturn*>(dest)->mSize;
    return {};
  }
  case leanstore::cr::WalEntry::Type::kComplex: {
    // read the body of WalEntryComplex
    auto left = sizeof(WalEntryComplex) - walEntrySize;
    auto res =
        readFromWalFile(offset + walEntrySize, left, dest + walEntrySize);
    if (!res) {
      return std::unexpected(res.error());
    }

    // read the payload of WalEntryComplex
    left = reinterpret_cast<WalEntryComplex*>(dest)->mSize -
           sizeof(WalEntryComplex);
    res = readFromWalFile(offset + sizeof(WalEntryComplex), left,
                          dest + sizeof(WalEntryComplex));
    if (!res) {
      return std::unexpected(res.error());
    }

    // advance the offset
    offset += reinterpret_cast<WalEntryComplex*>(dest)->mSize;
    return {};
  }
  }
  return {};
}

} // namespace leanstore::cr