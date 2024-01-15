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

std::atomic<u64> Logging::sGlobalMinFlushedGSN = 0;
std::atomic<u64> Logging::sGlobalMaxFlushedGSN = 0;

/// @brief Calculate the free space left in the wal ring buffer.
/// @return Size of the free space
u32 Logging::walFreeSpace() {
  const auto flushed = mWalFlushed.load();
  if (flushed == mWalBuffered) {
    return FLAGS_wal_buffer_size;
  }
  if (flushed < mWalBuffered) {
    return flushed + (FLAGS_wal_buffer_size - mWalBuffered);
  }
  return flushed - mWalBuffered;
}

/// @brief Calculate the continuous free space left in the wal ring buffer.
/// @return Size of the contiguous free space
u32 Logging::walContiguousFreeSpace() {
  const auto flushed = mWalFlushed.load();
  if (flushed == mWalBuffered) {
    return FLAGS_wal_buffer_size;
  }
  if (flushed < mWalBuffered) {
    return FLAGS_wal_buffer_size - mWalBuffered;
  }
  return flushed - mWalBuffered;
}

void Logging::WalEnsureEnoughSpace(u32 requiredBytes) {
  if (!FLAGS_wal) {
    return;
  }

  u32 walEntryBytes = requiredBytes + Worker::kCrEntrySize;
  u32 totalRequiredBytes = walEntryBytes;
  if ((FLAGS_wal_buffer_size - mWalBuffered) < walEntryBytes) {
    totalRequiredBytes += FLAGS_wal_buffer_size - mWalBuffered;
  }

  // Spin until there is enough space. The wal ring buffer space is reclaimed
  // when the group commit thread commits the written wal entries.
  while (walFreeSpace() < totalRequiredBytes) {
  }

  // Carriage Return. Start a new round on the wal ring buffer.
  if (walContiguousFreeSpace() < walEntryBytes) {
    // Always keep place for CR entry
    auto entrySize = FLAGS_wal_buffer_size - mWalBuffered;
    auto entryType = WALEntry::TYPE::CARRIAGE_RETURN;
    auto* entryPtr = mWalBuffer + mWalBuffered;
    auto* entry = new (entryPtr) WALEntrySimple(0, entrySize, entryType);

    entry->mCRC32 = entry->ComputeCRC32();

    // start a new round
    mWalBuffered = 0;
    publishWalBufferedOffset();
  }

  DCHECK(walContiguousFreeSpace() >= requiredBytes);
  DCHECK(mWalBuffered + walEntryBytes <= FLAGS_wal_buffer_size);
}

/// Reserve space and initialize a WALEntrySimple when a transaction is started,
/// committed, or aborted.
/// TODO(jian.z): set previous LSN for the WALEntry.
WALEntrySimple& Logging::ReserveWALEntrySimple(WALEntry::TYPE type) {
  SCOPED_DEFER(mPrevLSN = mActiveWALEntrySimple->lsn;);

  WalEnsureEnoughSpace(sizeof(WALEntrySimple));
  auto* entryPtr = mWalBuffer + mWalBuffered;
  auto entrySize = sizeof(WALEntrySimple);
  std::memset(entryPtr, 0, entrySize);
  mActiveWALEntrySimple =
      new (entryPtr) WALEntrySimple(mLsnClock++, entrySize, type);

  // set previous LSN on demand.
  if (type != WALEntry::TYPE::TX_START) {
    mActiveWALEntrySimple->mPrevLSN = mPrevLSN;
  }
  auto& curWorker = leanstore::cr::Worker::my();
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
              << ", workerId=" << Worker::my().mWorkerId
              << ", startTs=" << Worker::my().mActiveTx.mStartTs
              << ", curGSN=" << GetCurrentGsn()
              << ", walJson=" << buffer.GetString();
  });

  if (!((mWalBuffered >= mTxWalBegin) ||
        (mWalBuffered + sizeof(WALEntrySimple) < mTxWalBegin))) {
    Worker::my().mActiveTx.mWalExceedBuffer = true;
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
    Worker::my().mActiveTx.mWalExceedBuffer = true;
  }
  mActiveWALEntryComplex->mCRC32 = mActiveWALEntryComplex->ComputeCRC32();
  mWalBuffered += totalSize;
  publishWalFlushReq();
  Worker::my().mActiveTx.MarkAsWrite();

  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().wal_write_bytes += totalSize;
  }
}

void Logging::publishWalBufferedOffset() {
  mWalFlushReq.UpdateAttribute(&WalFlushReq::mWalBuffered, mWalBuffered);
}

void Logging::publishWalFlushReq() {
  WalFlushReq current(mWalBuffered, GetCurrentGsn(),
                      Worker::my().mActiveTx.mStartTs);
  mWalFlushReq.SetSync(current);
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
