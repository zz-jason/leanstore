#pragma once

#include "Units.hpp"
#include "storage/btree/BTreeLL.hpp"
#include "storage/btree/BTreeVI.hpp"
#include "utils/Defer.hpp"

#include <glog/logging.h>

#include <memory>
#include <string>
#include <unordered_map>

namespace leanstore {
namespace cr {

class Recovery {
private:
  /// The file descriptor of the underlying DB file, which contains all the
  /// pages and WAL records.
  s32 mWalFd;

  /// The offset of WAL in the underlying data file.
  u64 mWalStartOffset;

  /// Size of the written WAL file.
  u64 mWalSize;

  /// Stores the dirty page ID and the offset to the first WALEntry that caused
  /// that page to become dirty.
  std::map<PID, u64> mDirtyPageTable;

  /// Stores the active transaction and the offset to the last created WALEntry.
  std::map<TXID, u64> mActiveTxTable;

public:
  Recovery(s32 fd, u64 offset, u64 size)
      : mWalFd(fd), mWalStartOffset(offset), mWalSize(size) {
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
  void Analysis();

  /// During the redo phase, the DPT is used to find the set of pages in the
  /// buffer pool that were dirty at the time of the crash. All these pages are
  /// read from disk and redone from the first log record that makes them dirty.
  void Redo();

  /// During the undo phase, the TT is used to undo the transactions still
  /// active at crash time. In the case of an aborted transaction, it’s possible
  /// to traverse the log file in reverse order using the previous sequence
  /// numbers, undoing all actions taken within the specific transaction.
  bool Undo();

  ssize_t ReadWalEntry(s64 entryOffset, s64 entrySize, void* destination);
};

inline bool Recovery::Run() {
  bool error(false);

  Analysis();

  // Redo();

  // if (Undo()) {
  //   return true;
  // }

  return error;
}

inline void Recovery::Analysis() {
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

      // TODO(jian.z): how to read the same page from disk only once?
      auto& bf = BufferManager::sInstance->ReadPageToRecover(complexEntry->pid);
      DCHECK(bf.header.pid == complexEntry->pid);
      DCHECK(bf.page.mBTreeId == complexEntry->mTreeId);

      SCOPED_DEFER(bf.header.mKeepInMemory = false);

      if (complexEntry->gsn > bf.page.mGSN &&
          mDirtyPageTable.find(complexEntry->pid) == mDirtyPageTable.end()) {
        // record the first WALEntry that makes the page dirty
        mDirtyPageTable.emplace(std::make_pair(complexEntry->pid, offset));
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

inline void Recovery::Redo() {
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
    if (mDirtyPageTable.find(complexEntry->pid) == mDirtyPageTable.end() ||
        complexEntry->gsn < mDirtyPageTable[complexEntry->pid]) {
      offset += bytesRead;
      continue;
    }

    // TODO(jian.z): The page should keep in memory during the redo phase,
    // otherwise the previous redo operation on that page in memory is lost
    //
    // TODO(jian.z): redo the changes on the page
    //
    auto& bf = BufferManager::sInstance->ReadPageToRecover(complexEntry->pid);
    DCHECK(bf.header.pid == complexEntry->pid);
    DCHECK(bf.page.mBTreeId == complexEntry->mTreeId);
    SCOPED_DEFER(bf.header.mKeepInMemory = false);

    offset += bytesRead;
    continue;
  }
}

inline bool Recovery::Undo() {
  return false;
}

inline ssize_t Recovery::ReadWalEntry(s64 offset, s64 nbytes,
                                      void* destination) {

  FILE* fp = fopen(GetWALFilePath().c_str(), "rb");
  if (fp == nullptr) {
    perror("Error opening file");
    return -1;
  }
  SCOPED_DEFER(fclose(fp));

  if (fseek(fp, offset, SEEK_SET) != 0) {
    perror("Error seeking file");
    return -1;
  }

  if (fread(destination, 1, nbytes, fp) != nbytes) {
    perror("Error reading file");
    DLOG(FATAL) << "WAL file name=" << GetWALFilePath() << ", offset=" << offset
                << ", nbytes=" << nbytes;
    return -1;
  }

  return nbytes;
}

/*
inline ssize_t Recovery::ReadWalEntry(s64 entryOffset, s64 entrySize,
                                      void* destination) {
  auto bytesLeft = entrySize;
  do {
    auto bytesRead = pread(mWalFd, destination, bytesLeft,
                           entryOffset + (entrySize - bytesLeft));
    // pread (int __fd, void *__buf, size_t __nbytes, __off64_t __offset)
    if (bytesRead < 0) {
      perror("ReadWalEntry read failed");

      // whether the fd is valid
      {
        int flags = fcntl(mWalFd, F_GETFD);
        if (flags == -1) {
          perror("fcntl");
          exit(EXIT_FAILURE);
        }
        if (flags & FD_CLOEXEC) {
          LOG(INFO) << "mWalFd(" << mWalFd << ") is valid";
        } else {
          LOG(INFO) << "mWalFd(" << mWalFd << ") is invalid";
        }
      }

      // whether the file offset+nbytes is valid
      {
        off_t size = lseek(mWalFd, 0, SEEK_END);
        if (size == -1) {
          perror("lseek");
          exit(EXIT_FAILURE);
        }
        LOG(INFO) << "mWalFd(" << mWalFd << ") file size: " << size;
      }

      LOG(FATAL) << "pread failed"
                 << ", error=" << errno << ", fd=" << mWalFd
                 << ", buf=" << destination << ", nbytes=" << bytesLeft
                 << ", offset=" << entryOffset + (entrySize - bytesLeft);
      return bytesRead;
    }
    bytesLeft -= bytesRead;
  } while (bytesLeft > 0);

  return entrySize;
}
*/

} // namespace cr
} // namespace leanstore
