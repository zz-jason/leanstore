#pragma once

#include "buffer-manager/BufferFrame.hpp"
#include "buffer-manager/BufferManager.hpp"
#include "concurrency/WALEntry.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "utils/Defer.hpp"
#include "utils/Error.hpp"

#include <glog/logging.h>

#include <expected>
#include <map>

namespace leanstore {
namespace cr {

class Recovery {
private:
  leanstore::LeanStore* mStore;

  /// The offset of WAL in the underlying data file.
  uint64_t mWalStartOffset;

  /// Size of the written WAL file.
  uint64_t mWalSize;

  /// Stores the dirty page ID and the offset to the first WALEntry that caused
  /// that page to become dirty.
  std::map<PID, uint64_t> mDirtyPageTable;

  /// Stores the active transaction and the offset to the last created WALEntry.
  std::map<TXID, uint64_t> mActiveTxTable;

  /// Stores all the pages read from disk during the recovery process.
  std::map<PID, storage::BufferFrame*> mResolvedPages;

public:
  Recovery(leanstore::LeanStore* store, uint64_t offset, uint64_t size)
      : mStore(store),
        mWalStartOffset(offset),
        mWalSize(size) {
  }

  ~Recovery() = default;

  // no copy and assign
  Recovery& operator=(const Recovery&) = delete;
  Recovery(const Recovery&) = delete;

public:
  /// The ARIES algorithm relies on logging of all database operations with
  /// ascending sequence numbers. The resulting logfile is stored on so-called
  /// “stable storage”, which is a storage medium that is assumed to survive
  /// crashes and hardware failures. To gather the necessary information for
  /// the logs, two data structures have to be maintained: the dirty page table
  /// (DPT) and the transaction table (TT). The dirty page table keeps record of
  /// all the pages that have been modified, and not yet written to disk, and
  /// the first sequence number that caused that page to become dirty. The
  /// transaction table contains all currently running transactions and the
  /// sequence number of the last log entry they created.
  ///
  /// The recovery works in three phases: analysis, redo, and undo. During the
  /// analysis phase, all the necessary information is computed from the
  /// logfile. During the redo phase, ARIES retraces the actions of a database
  /// before the crash and brings the system back to the exact state that it was
  /// in before the crash. During the undo phase, ARIES undoes the transactions
  /// still active at crash time.
  bool Run();

private:
  /// During the analysis phase, the DPT and TT are restored to their state at
  /// the time of the crash. The logfile is scanned from the beginning or the
  /// last checkpoint, and all transactions for which we encounter Begin
  /// Transaction entries are added to the TT. Whenever an End Log entry is
  /// found, the corresponding transaction is removed.
  std::expected<void, utils::Error> analysis();

  /// During the redo phase, the DPT is used to find the set of pages in the
  /// buffer pool that were dirty at the time of the crash. All these pages are
  /// read from disk and redone from the first log record that makes them dirty.
  std::expected<void, utils::Error> redo();

  std::expected<bool, utils::Error> nextWalComplexToRedo(
      uint64_t& offset, WALEntryComplex* walEntryPtr);

  void redoInsert(storage::BufferFrame& bf, WALEntryComplex* complexEntry);

  void redoTxInsert(storage::BufferFrame& bf, WALEntryComplex* complexEntry);

  void redoUpdate(storage::BufferFrame& bf, WALEntryComplex* complexEntry);

  void redoTxUpdate(storage::BufferFrame& bf, WALEntryComplex* complexEntry);

  void redoRemove(storage::BufferFrame& bf, WALEntryComplex* complexEntry);

  void redoTxRemove(storage::BufferFrame& bf, WALEntryComplex* complexEntry);

  void redoInitPage(storage::BufferFrame& bf, WALEntryComplex* complexEntry);

  void redoSplitRoot(storage::BufferFrame& bf, WALEntryComplex* complexEntry);

  void redoSplitNonRoot(storage::BufferFrame& bf,
                        WALEntryComplex* complexEntry);

  /// During the undo phase, the TT is used to undo the transactions still
  /// active at crash time. In the case of an aborted transaction, it’s possible
  /// to traverse the log file in reverse order using the previous sequence
  /// numbers, undoing all actions taken within the specific transaction.
  bool undo();

  /// Return the buffer frame containing the required dirty page
  storage::BufferFrame& resolvePage(PID pageId);

  std::expected<void, utils::Error> readFromWalFile(int64_t entryOffset,
                                                    size_t entrySize,
                                                    void* destination);
};

inline bool Recovery::Run() {
  bool error(false);

  analysis();
  for (auto it = mResolvedPages.begin(); it != mResolvedPages.end(); ++it) {
    if (it->second->IsFree()) {
      continue;
    }
    DLOG(INFO) << "Resolved page after analysis"
               << ", address: " << it->second << ", pageId: " << it->first
               << ", btreeId: " << it->second->mPage.mBTreeId;
  }
  // print resulting active transaction table
  DLOG(INFO) << "Active transaction table size: " << mActiveTxTable.size();
  for (auto it = mActiveTxTable.begin(); it != mActiveTxTable.end(); ++it) {
    DLOG(INFO) << "Active transaction table after analysis"
               << ", txId: " << it->first << ", offset: " << it->second;
  }
  // print dirty page table
  DLOG(INFO) << "Dirty page table size: " << mDirtyPageTable.size();
  for (auto it = mDirtyPageTable.begin(); it != mDirtyPageTable.end(); ++it) {
    DLOG(INFO) << "Dirty page table after analysis"
               << ", pageId: " << it->first << ", offset: " << it->second;
  }

  redo();

  // if (Undo()) {
  //   return true;
  // }

  return error;
}

inline bool Recovery::undo() {
  return false;
}

inline storage::BufferFrame& Recovery::resolvePage(PID pageId) {
  auto it = mResolvedPages.find(pageId);
  if (it != mResolvedPages.end()) {
    return *it->second;
  }

  auto& bf = mStore->mBufferManager->ReadPageSync(pageId);
  // prevent the buffer frame from being evicted by buffer frame providers
  bf.mHeader.mKeepInMemory = true;
  mResolvedPages.emplace(pageId, &bf);
  return bf;
}

inline auto Recovery::readFromWalFile(int64_t offset, size_t nbytes,
                                      void* destination)
    -> std::expected<void, utils::Error> {
  auto fileName = GetWALFilePath();
  FILE* fp = fopen(fileName.c_str(), "rb");
  if (fp == nullptr) {
    return std::unexpected(
        utils::Error::FileOpen(fileName, errno, strerror(errno)));
  }
  SCOPED_DEFER(fclose(fp));

  if (fseek(fp, offset, SEEK_SET) != 0) {
    return std::unexpected(
        utils::Error::FileSeek(fileName, errno, strerror(errno)));
  }

  if (fread(destination, 1, nbytes, fp) != nbytes) {
    return std::unexpected(
        utils::Error::FileRead(fileName, errno, strerror(errno)));
  }

  return {};
}

} // namespace cr
} // namespace leanstore
