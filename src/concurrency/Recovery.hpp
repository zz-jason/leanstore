#pragma once

#include "buffer-manager/BufferFrame.hpp"
#include "buffer-manager/BufferManager.hpp"
#include "concurrency/WalEntry.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "utils/Result.hpp"

#include <cstring>
#include <expected>
#include <map>

#include <unistd.h>

namespace leanstore::cr {

class Recovery {
private:
  leanstore::LeanStore* mStore;

  //! The offset of WAL in the underlying data file.
  uint64_t mWalStartOffset;

  //! Size of the written WAL file.
  uint64_t mWalSize;

  //! Stores the dirty page ID and the offset to the first WalEntry that caused
  //! that page to become dirty.
  std::map<PID, uint64_t> mDirtyPageTable;

  //! Stores the active transaction and the offset to the last created WalEntry.
  std::map<TXID, uint64_t> mActiveTxTable;

  //! Stores all the pages read from disk during the recovery process.
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
  //! The ARIES algorithm relies on logging of all database operations with
  //! ascending sequence numbers. The resulting logfile is stored on so-called
  //! “stable storage”, which is a storage medium that is assumed to survive
  //! crashes and hardware failures. To gather the necessary information for
  //! the logs, two data structures have to be maintained: the dirty page table
  //! (DPT) and the transaction table (TT). The dirty page table keeps record of
  //! all the pages that have been modified, and not yet written to disk, and
  //! the first sequence number that caused that page to become dirty. The
  //! transaction table contains all currently running transactions and the
  //! sequence number of the last log entry they created.
  ///
  //! The recovery works in three phases: analysis, redo, and undo. During the
  //! analysis phase, all the necessary information is computed from the
  //! logfile. During the redo phase, ARIES retraces the actions of a database
  //! before the crash and brings the system back to the exact state that it was
  //! in before the crash. During the undo phase, ARIES undoes the transactions
  //! still active at crash time.
  bool Run();

private:
  //! During the analysis phase, the DPT and TT are restored to their state at
  //! the time of the crash. The logfile is scanned from the beginning or the
  //! last checkpoint, and all transactions for which we encounter Begin
  //! Transaction entries are added to the TT. Whenever an End Log entry is
  //! found, the corresponding transaction is removed.
  Result<void> analysis();

  //! During the redo phase, the DPT is used to find the set of pages in the
  //! buffer pool that were dirty at the time of the crash. All these pages are
  //! read from disk and redone from the first log record that makes them dirty.
  Result<void> redo();

  Result<bool> nextWalComplexToRedo(uint64_t& offset, WalEntryComplex* walEntryPtr);

  void redoInsert(storage::BufferFrame& bf, WalEntryComplex* complexEntry);

  void redoTxInsert(storage::BufferFrame& bf, WalEntryComplex* complexEntry);

  void redoUpdate(storage::BufferFrame& bf, WalEntryComplex* complexEntry);

  void redoTxUpdate(storage::BufferFrame& bf, WalEntryComplex* complexEntry);

  void redoRemove(storage::BufferFrame& bf, WalEntryComplex* complexEntry);

  void redoTxRemove(storage::BufferFrame& bf, WalEntryComplex* complexEntry);

  void redoInitPage(storage::BufferFrame& bf, WalEntryComplex* complexEntry);

  void redoSplitRoot(storage::BufferFrame& bf, WalEntryComplex* complexEntry);

  void redoSplitNonRoot(storage::BufferFrame& bf, WalEntryComplex* complexEntry);

  //! During the undo phase, the TT is used to undo the transactions still
  //! active at crash time. In the case of an aborted transaction, it’s possible
  //! to traverse the log file in reverse order using the previous sequence
  //! numbers, undoing all actions taken within the specific transaction.
  void undo() {
  }

  //! Return the buffer frame containing the required dirty page
  storage::BufferFrame& resolvePage(PID pageId);

  //! Read a WalEntry from the WAL file to the destination buffer.
  Result<void> readWalEntry(uint64_t& offset, uint8_t* dest);

  Result<void> readFromWalFile(int64_t entryOffset, size_t entrySize, void* destination);
};

} // namespace leanstore::cr
