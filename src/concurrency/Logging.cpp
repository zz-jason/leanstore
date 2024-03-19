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

namespace leanstore {
namespace cr {

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
  SCOPED_DEFER({
    DCHECK(bytesRequired <= walContiguousFreeSpace())
        << "bytesRequired=" << bytesRequired
        << ", walContiguousFreeSpace()=" << walContiguousFreeSpace();
  });

  // Spin until there is enough space. The wal ring buffer space is reclaimed
  // when the group commit thread commits the written wal entries.
  while (true) {
    const auto flushed = mWalFlushed.load();
    if (flushed <= mWalBuffered) {
      // carraige return, consume the last bytes from mWalBuffered to the end
      if (mWalBufferSize - mWalBuffered < bytesRequired) {
        auto entrySize = mWalBufferSize - mWalBuffered;
        auto entryType = WalEntry::Type::kCarriageReturn;
        auto* entryPtr = mWalBuffer + mWalBuffered;
        auto* entry = new (entryPtr) WalEntrySimple(0, entrySize, entryType);
        entry->mCrc32 = entry->ComputeCRC32();
        mWalBuffered = 0;
        publishWalBufferedOffset();
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

/// Reserve space and initialize a WalEntrySimple when a transaction is started,
/// committed, or aborted.
WalEntrySimple& Logging::ReserveWALEntrySimple(WalEntry::Type type) {
  SCOPED_DEFER(mPrevLSN = mActiveWALEntrySimple->mLsn;);

  ReserveContiguousBuffer(sizeof(WalEntrySimple));
  auto* entryPtr = mWalBuffer + mWalBuffered;
  auto entrySize = sizeof(WalEntrySimple);
  std::memset(entryPtr, 0, entrySize);
  mActiveWALEntrySimple =
      new (entryPtr) WalEntrySimple(mLsnClock++, entrySize, type);

  mActiveWALEntrySimple->mPrevLSN = mPrevLSN;
  auto& curWorker = leanstore::cr::Worker::My();
  mActiveWALEntrySimple->InitTxInfo(&curWorker.mActiveTx, curWorker.mWorkerId);
  return *mActiveWALEntrySimple;
}

/// Submits the WalEntrySimple to group committer when transaction is started,
/// committed, or aborted. It updates mCurrTxId to notify the group commit
/// thread to flush WAL records for finished (committed or aborted)
/// transactions.
///
/// NOTE: users should call ReserveWALEntrySimple() firstly to initialize the
/// WalEntrySimple to be flushed in the wal ring buffer.
void Logging::SubmitWALEntrySimple() {
  SCOPED_DEFER(DEBUG_BLOCK() {
    LOG(INFO) << "Submit WalEntrySimple"
              << ", workerId=" << Worker::My().mWorkerId
              << ", startTs=" << Worker::My().mActiveTx.mStartTs
              << ", curGSN=" << GetCurrentGsn()
              << ", walJson=" << WalEntry::ToJsonString(mActiveWALEntrySimple);
  });

  if (!((mWalBuffered >= mTxWalBegin) ||
        (mWalBuffered + sizeof(WalEntrySimple) < mTxWalBegin))) {
    Worker::My().mActiveTx.mWalExceedBuffer = true;
  }
  mActiveWALEntrySimple->mCrc32 = mActiveWALEntrySimple->ComputeCRC32();
  mWalBuffered += sizeof(WalEntrySimple);
  publishWalFlushReq();
}

void Logging::WriteSimpleWal(WalEntry::Type type) {
  ReserveWALEntrySimple(type);
  SubmitWALEntrySimple();
}

/// Submits the wal record to group committer when it is ready to flush to disk.
/// @param totalSize is the size of the wal record to be flush.
void Logging::SubmitWALEntryComplex(uint64_t totalSize) {
  SCOPED_DEFER(DEBUG_BLOCK() {
    LOG(INFO) << "SubmitWal"
              << ", workerId=" << Worker::My().mWorkerId
              << ", startTs=" << Worker::My().mActiveTx.mStartTs
              << ", curGSN=" << Worker::My().mLogging.GetCurrentGsn()
              << ", walJson=" << WalEntry::ToJsonString(mActiveWALEntryComplex);
  });

  if (!((mWalBuffered >= mTxWalBegin) ||
        (mWalBuffered + totalSize < mTxWalBegin))) {
    Worker::My().mActiveTx.mWalExceedBuffer = true;
  }
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
    ENSURE(entry.mSize > 0);
    DEBUG_BLOCK() {
      if (entry.mType != WalEntry::Type::kCarriageReturn) {
        entry.CheckCRC();
      }
    }
    if (entry.mType == WalEntry::Type::kCarriageReturn) {
      cursor = 0;
    } else {
      callback(entry);
      cursor += entry.mSize;
    }
  }
}

} // namespace cr
} // namespace leanstore
