#pragma once

#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/common/types.h"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/result.hpp"

#include <cstring>
#include <expected>
#include <map>

#include <unistd.h>

/// forward declarations
namespace leanstore::cr {

class WalEntryComplex;

} // namespace leanstore::cr

namespace leanstore::cr {

class Recovery {
private:
  leanstore::LeanStore* store_;

  /// The offset of WAL in the underlying data file.
  uint64_t wal_start_offset_;

  /// Size of the written WAL file.
  uint64_t wal_size_;

  /// Stores the dirty page ID and the offset to the first WalEntry that caused that page to become
  /// dirty.
  std::map<lean_pid_t, uint64_t> dirty_page_table_;

  /// Stores the active transaction and the offset to the last created WalEntry.
  std::map<lean_txid_t, uint64_t> active_tx_table_;

  /// Stores all the pages read from disk during the recovery process.
  std::map<lean_pid_t, storage::BufferFrame*> resolved_pages_;

public:
  Recovery(leanstore::LeanStore* store, uint64_t offset, uint64_t size)
      : store_(store),
        wal_start_offset_(offset),
        wal_size_(size) {
  }

  ~Recovery() = default;

  // no copy and assign
  Recovery& operator=(const Recovery&) = delete;
  Recovery(const Recovery&) = delete;

public:
  /// The ARIES algorithm relies on logging of all database operations with ascending sequence
  /// numbers. The resulting logfile is stored on so-called “stable storage”, which is a storage
  /// medium that is assumed to survive crashes and hardware failures. To gather the necessary
  /// information for the logs, two data structures have to be maintained: the dirty page table
  /// (DPT) and the transaction table (TT). The dirty page table keeps record of all the pages that
  /// have been modified, and not yet written to disk, and the first sequence number that caused
  /// that page to become dirty. The transaction table contains all currently running transactions
  /// and the sequence number of the last log entry they created.
  ///
  /// The recovery works in three phases: analysis, redo, and undo. During the analysis phase, all
  /// the necessary information is computed from the logfile. During the redo phase, ARIES retraces
  /// the actions of a database before the crash and brings the system back to the exact state that
  /// it was in before the crash. During the undo phase, ARIES undoes the transactions still active
  /// at crash time.
  bool Run();

private:
  /// During the analysis phase, the DPT and TT are restored to their state at the time of the
  /// crash. The logfile is scanned from the beginning or the last checkpoint, and all transactions
  /// for which we encounter begin transaction entries are added to the TT. Whenever an End Log
  /// entry is found, the corresponding transaction is removed.
  Result<void> analysis();

  /// During the redo phase, the DPT is used to find the set of pages in the buffer pool that were
  /// dirty at the time of the crash. All these pages are read from disk and redone from the first
  /// log record that makes them dirty.
  Result<void> redo();

  Result<bool> next_wal_complex_to_redo(uint64_t& offset, WalEntryComplex* wal_entry_ptr);

  void redo_insert(storage::BufferFrame& bf, WalEntryComplex* complex_entry);

  void redo_tx_insert(storage::BufferFrame& bf, WalEntryComplex* complex_entry);

  void redo_update(storage::BufferFrame& bf, WalEntryComplex* complex_entry);

  void redo_tx_update(storage::BufferFrame& bf, WalEntryComplex* complex_entry);

  void redo_remove(storage::BufferFrame& bf, WalEntryComplex* complex_entry);

  void redo_tx_remove(storage::BufferFrame& bf, WalEntryComplex* complex_entry);

  void redo_init_page(storage::BufferFrame& bf, WalEntryComplex* complex_entry);

  void redo_split_root(storage::BufferFrame& bf, WalEntryComplex* complex_entry);

  void redo_split_non_root(storage::BufferFrame& bf, WalEntryComplex* complex_entry);

  /// During the undo phase, the TT is used to undo the transactions still active at crash time. In
  /// the case of an aborted transaction, it’s possible to traverse the log file in reverse order
  /// using the previous sequence numbers, undoing all actions taken within the specific
  /// transaction.
  void undo() {
  }

  /// Return the buffer frame containing the required dirty page
  storage::BufferFrame& resolve_page(lean_pid_t page_id);

  /// Read a WalEntry from the WAL file to the destination buffer.
  Result<void> read_wal_entry(uint64_t& offset, uint8_t* dest);

  Result<void> read_from_wal_file(int64_t entry_offset, size_t entry_size, void* destination);
};

} // namespace leanstore::cr
