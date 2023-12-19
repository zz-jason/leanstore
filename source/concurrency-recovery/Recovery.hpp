#pragma once

#include "Units.hpp"
#include "storage/btree/BTreeLL.hpp"
#include "storage/btree/BTreeVI.hpp"
#include "storage/btree/core/BTreeExclusiveIterator.hpp"
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

  /// Stores all the pages read from disk during the recovery process.
  std::map<PID, BufferFrame*> mResolvedPages;

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

  /// Return the buffer frame containing the required dirty page
  BufferFrame& ResolvePage(PID pageId);

  ssize_t ReadWalEntry(s64 entryOffset, size_t entrySize, void* destination);
};

inline bool Recovery::Run() {
  bool error(false);

  Analysis();

  Redo();

  // if (Undo()) {
  //   return true;
  // }

  return error;
}

inline bool Recovery::Undo() {
  return false;
}

inline BufferFrame& Recovery::ResolvePage(PID pageId) {
  auto it = mResolvedPages.find(pageId);
  if (it != mResolvedPages.end()) {
    return *it->second;
  }

  auto& bf = BufferManager::sInstance->ReadPageSync(pageId);
  // prevent the buffer frame from being evicted by buffer frame providers
  bf.header.mKeepInMemory = true;
  mResolvedPages.emplace(pageId, &bf);
  return bf;
}

inline ssize_t Recovery::ReadWalEntry(s64 offset, size_t nbytes,
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
