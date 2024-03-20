#include "concurrency/Logging.hpp"

#include "concurrency/WalEntry.hpp"
#include "concurrency/Worker.hpp"
#include "leanstore/Exceptions.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "utils/Defer.hpp"

#include <glog/logging.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cstring>
#include <format>

namespace leanstore::cr {

/// @brief Calculate the continuous free space left in the wal ring buffer.
/// @return Size of the contiguous free space
uint32_t Logging::walContiguousFreeSpace() {
  const auto flushed = mWalFlushed.load();
  if (flushed <= mWalBuffered) {
    return mWalBufferSize - mWalBuffered;
  }
  return flushed - mWalBuffered;
}

void Logging::ReserveContiguousBuffer(uint32_t bytesRequired) {
  // Spin until there is enough space. The wal ring buffer space is reclaimed
  // when the group commit thread commits the written wal entries.
  while (true) {
    const auto flushed = mWalFlushed.load();
    if (flushed <= mWalBuffered) {
      // carraige return, consume the last bytes from mWalBuffered to the end
      if (mWalBufferSize - mWalBuffered < bytesRequired) {
        WriteWalCarriageReturn();
        continue;
      }
      // Have enough space from mWalBuffered to the end
      return;
    }

    if (flushed - mWalBuffered < bytesRequired) {
      // wait for group commit thread to commit the written wal entries
      continue;
    }
    return;
  }
}

void Logging::WriteWalTxAbort() {
  WalEntry* entry;
  SCOPED_DEFER({
    DLOG(INFO) << std::format(
        "WriteWalTxAbort, workerId={}, startTs={}, curGSN={}, walJson={}",
        Worker::My().mWorkerId, Worker::My().mActiveTx.mStartTs,
        GetCurrentGsn(), WalEntry::ToJsonString(entry));
  });

  // Reserve space
  auto size = sizeof(WalTxAbort);
  ReserveContiguousBuffer(size);

  // Initialize a WalTxAbort
  auto* data = mWalBuffer + mWalBuffered;
  std::memset(data, 0, size);
  entry = new (data) WalTxAbort(size);

  // Submit the WalTxAbort to group committer
  mWalBuffered += size;
  publishWalFlushReq();
}

void Logging::WriteWalTxFinish() {
  WalEntry* entry;
  SCOPED_DEFER({
    DLOG(INFO) << std::format(
        "WriteWalTxFinish, workerId={}, startTs={}, curGSN={}, walJson={}",
        Worker::My().mWorkerId, Worker::My().mActiveTx.mStartTs,
        GetCurrentGsn(), WalEntry::ToJsonString(entry));
  });

  // Reserve space
  auto size = sizeof(WalTxFinish);
  ReserveContiguousBuffer(size);

  // Initialize a WalTxFinish
  auto* data = mWalBuffer + mWalBuffered;
  std::memset(data, 0, size);
  entry = new (data) WalTxFinish(Worker::My().mActiveTx.mStartTs);

  // Submit the WalTxAbort to group committer
  mWalBuffered += size;
  publishWalFlushReq();
}

void Logging::WriteWalCarriageReturn() {
  DCHECK(mWalFlushed <= mWalBuffered)
      << "CarriageReturn should only used for the last bytes in the wal buffer";
  auto entrySize = mWalBufferSize - mWalBuffered;
  auto* entryPtr = mWalBuffer + mWalBuffered;
  new (entryPtr) WalCarriageReturn(entrySize);
  mWalBuffered = 0;
  publishWalBufferedOffset();
}

/// Submits the wal record to group committer when it is ready to flush to disk.
/// @param totalSize is the size of the wal record to be flush.
void Logging::SubmitWALEntryComplex(uint64_t totalSize) {
  SCOPED_DEFER({
    DLOG(INFO) << std::format(
        "SubmitWal, workerId={}, startTs={}, curGSN={}, walJson={}",
        Worker::My().mWorkerId, Worker::My().mActiveTx.mStartTs,
        GetCurrentGsn(), WalEntry::ToJsonString(mActiveWALEntryComplex));
  });

  mActiveWALEntryComplex->mCrc32 = mActiveWALEntryComplex->ComputeCRC32();
  mWalBuffered += totalSize;
  publishWalFlushReq();

  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().wal_write_bytes += totalSize;
  }
}

void Logging::publishWalBufferedOffset() {
  mWalFlushReq.UpdateAttribute(&WalFlushReq::mWalBuffered, mWalBuffered);
}

void Logging::publishWalFlushReq() {
  WalFlushReq current(mWalBuffered, GetCurrentGsn(),
                      Worker::My().mActiveTx.mStartTs);
  mWalFlushReq.Set(current);
}

// Called by worker, so concurrent writes on the buffer
void Logging::IterateCurrentTxWALs(
    std::function<void(const WalEntry& entry)> callback) {
  uint64_t cursor = mTxWalBegin;
  while (cursor != mWalBuffered) {
    const WalEntry& entry = *reinterpret_cast<WalEntry*>(mWalBuffer + cursor);
    DEBUG_BLOCK() {
      if (entry.mType == WalEntry::Type::kComplex) {
        reinterpret_cast<const WalEntryComplex*>(&entry)->CheckCRC();
      }
    }

    if (entry.mType == WalEntry::Type::kCarriageReturn) {
      cursor = 0;
    } else {
      callback(entry);
      cursor += WalEntry::Size(&entry);
    }
  }
}

} // namespace leanstore::cr
