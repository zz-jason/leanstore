#include "Worker.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "shared-headers/Exceptions.hpp"
#include "utils/Defer.hpp"

#include <glog/logging.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cstring>

namespace leanstore {
namespace cr {

/// @brief Calculate the continuous free space left in the wal ring buffer.
/// @return Size of the contiguous free space
u32 Logging::walContiguousFreeSpace() {
  const auto flushed = mWalFlushed.load();
  if (flushed <= mWalBuffered) {
    return mWalBufferSize - mWalBuffered;
  }
  return flushed - mWalBuffered;
}

void Logging::ReserveContiguousBuffer(u32 bytesRequired) {
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
        auto entryType = WALEntry::TYPE::CARRIAGE_RETURN;
        auto* entryPtr = mWalBuffer + mWalBuffered;
        auto* entry = new (entryPtr) WALEntrySimple(0, entrySize, entryType);
        entry->mCRC32 = entry->ComputeCRC32();
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

/// Reserve space and initialize a WALEntrySimple when a transaction is started,
/// committed, or aborted.
/// TODO(jian.z): set previous LSN for the WALEntry.
WALEntrySimple& Logging::ReserveWALEntrySimple(WALEntry::TYPE type) {
  SCOPED_DEFER(mPrevLSN = mActiveWALEntrySimple->lsn;);

  ReserveContiguousBuffer(sizeof(WALEntrySimple));
  auto* entryPtr = mWalBuffer + mWalBuffered;
  auto entrySize = sizeof(WALEntrySimple);
  std::memset(entryPtr, 0, entrySize);
  mActiveWALEntrySimple =
      new (entryPtr) WALEntrySimple(mLsnClock++, entrySize, type);

  // set previous LSN on demand.
  if (type != WALEntry::TYPE::TX_START) {
    mActiveWALEntrySimple->mPrevLSN = mPrevLSN;
  }
  auto& curWorker = leanstore::cr::Worker::My();
  mActiveWALEntrySimple->InitTxInfo(&curWorker.mActiveTx, curWorker.mWorkerId);
  return *mActiveWALEntrySimple;
}

/// Submits the WALEntrySimple to group committer when transaction is started,
/// committed, or aborted. It updates mCurrTxId to notify the group commit
/// thread to flush WAL records for finished (committed or aborted)
/// transactions.
///
/// NOTE: users should call ReserveWALEntrySimple() firstly to initialize the
/// WALEntrySimple to be flushed in the wal ring buffer.
void Logging::SubmitWALEntrySimple() {
  SCOPED_DEFER(DEBUG_BLOCK() {
    auto doc = mActiveWALEntrySimple->ToJson();
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc->Accept(writer);
    LOG(INFO) << "Submit WALEntrySimple"
              << ", workerId=" << Worker::My().mWorkerId
              << ", startTs=" << Worker::My().mActiveTx.mStartTs
              << ", curGSN=" << GetCurrentGsn()
              << ", walJson=" << buffer.GetString();
  });

  if (!((mWalBuffered >= mTxWalBegin) ||
        (mWalBuffered + sizeof(WALEntrySimple) < mTxWalBegin))) {
    Worker::My().mActiveTx.mWalExceedBuffer = true;
  }
  mActiveWALEntrySimple->mCRC32 = mActiveWALEntrySimple->ComputeCRC32();
  mWalBuffered += sizeof(WALEntrySimple);
  publishWalFlushReq();
}

void Logging::WriteSimpleWal(WALEntry::TYPE type) {
  ReserveWALEntrySimple(type);
  SubmitWALEntrySimple();
}

/// @brief SubmitWALEntryComplex submits the wal record to group committer when
/// it is ready to flush to disk.
/// @param totalSize is the size of the wal record to be flush.
void Logging::SubmitWALEntryComplex(u64 totalSize) {
  if (!((mWalBuffered >= mTxWalBegin) ||
        (mWalBuffered + totalSize < mTxWalBegin))) {
    Worker::My().mActiveTx.mWalExceedBuffer = true;
  }
  mActiveWALEntryComplex->mCRC32 = mActiveWALEntryComplex->ComputeCRC32();
  mWalBuffered += totalSize;
  publishWalFlushReq();
  Worker::My().mActiveTx.MarkAsWrite();

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
    std::function<void(const WALEntry& entry)> callback) {
  u64 cursor = mTxWalBegin;
  while (cursor != mWalBuffered) {
    const WALEntry& entry = *reinterpret_cast<WALEntry*>(mWalBuffer + cursor);
    ENSURE(entry.size > 0);
    DEBUG_BLOCK() {
      if (entry.type != WALEntry::TYPE::CARRIAGE_RETURN)
        entry.CheckCRC();
    }
    if (entry.type == WALEntry::TYPE::CARRIAGE_RETURN) {
      cursor = 0;
    } else {
      callback(entry);
      cursor += entry.size;
    }
  }
}

} // namespace cr
} // namespace leanstore
