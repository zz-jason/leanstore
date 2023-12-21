#include "Worker.hpp"

#include "profiling/counters/CPUCounters.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "utils/Defer.hpp"
#include "utils/Misc.hpp"

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include <glog/logging.h>

namespace leanstore {
namespace cr {

atomic<u64> Logging::sMinFlushedGsn = 0;
atomic<u64> Logging::sMaxFlushedGsn = 0;
atomic<u64> Logging::sMinFlushedCommitTs = 0;

/// @brief Calculate the free space left in the wal ring buffer.
/// @return Size of the free space
u32 Logging::walFreeSpace() {
  const auto flushed = mWalFlushed.load();
  if (flushed == mWalBuffered) {
    return FLAGS_wal_buffer_size;
  } else if (flushed < mWalBuffered) {
    return flushed + (FLAGS_wal_buffer_size - mWalBuffered);
  } else {
    return flushed - mWalBuffered;
  }
}

/// @brief Calculate the continuous free space left in the wal ring buffer.
/// @return Size of the contiguous free space
u32 Logging::walContiguousFreeSpace() {
  const auto flushed = mWalFlushed.load();
  if (flushed == mWalBuffered) {
    return FLAGS_wal_buffer_size;
  } else if (flushed < mWalBuffered) {
    return FLAGS_wal_buffer_size - mWalBuffered;
  } else {
    return flushed - mWalBuffered;
  }
}

void Logging::walEnsureEnoughSpace(u32 requiredBytes) {
  if (!FLAGS_wal) {
    return;
  }

  u32 walEntryBytes = requiredBytes + Worker::CR_ENTRY_SIZE;
  u32 totalRequiredBytes = walEntryBytes;
  if ((FLAGS_wal_buffer_size - mWalBuffered) < walEntryBytes) {
    totalRequiredBytes += FLAGS_wal_buffer_size - mWalBuffered;
  }

  // Spin until there is enough space. The wal ring buffer space is reclaimed
  // when the group commit thread commits the written wal entries.
  {
    if (FLAGS_wal_variant == 2 && walFreeSpace() < totalRequiredBytes) {
      mWalFlushReq.mOptimisticLatch.notify_all();
    }
    while (walFreeSpace() < totalRequiredBytes) {
    }
  }

  // Carriage Return. Start a new round on the wal ring buffer.
  if (walContiguousFreeSpace() < walEntryBytes) {
    // Always keep place for CR entry
    auto entrySize = FLAGS_wal_buffer_size - mWalBuffered;
    auto entryType = WALEntry::TYPE::CARRIAGE_RETURN;
    auto entryPtr = mWalBuffer + mWalBuffered;
    auto entry = new (entryPtr) WALEntrySimple(0, entrySize, entryType);

    DEBUG_BLOCK() {
      entry->computeCRC();
    }

    // start a new round
    mWalBuffered = 0;
    publishOffset();
  }
  ENSURE(walContiguousFreeSpace() >= requiredBytes);
  ENSURE(mWalBuffered + walEntryBytes <= FLAGS_wal_buffer_size);
}

/// Reserve space and initialize a WALEntrySimple when a transaction is started,
/// committed, or aborted.
/// TODO(jian.z): set previous LSN for the WALEntry.
WALEntrySimple& Logging::ReserveWALEntrySimple(WALEntry::TYPE type) {
  SCOPED_DEFER(mPrevLSN = mActiveWALEntrySimple->lsn;);

  walEnsureEnoughSpace(sizeof(WALEntrySimple));
  auto entryPtr = mWalBuffer + mWalBuffered;
  auto entrySize = sizeof(WALEntrySimple);
  mActiveWALEntrySimple =
      new (entryPtr) WALEntrySimple(mLsnClock++, entrySize, type);

  // set prev LSN on demand.
  if (type != WALEntry::TYPE::TX_START) {
    mActiveWALEntrySimple->mPrevLSN = mPrevLSN;
  }
  auto& curWorker = leanstore::cr::Worker::my();
  mActiveWALEntrySimple->InitTxInfo(&curWorker.mActiveTx, curWorker.mWorkerId);
  return *mActiveWALEntrySimple;
}

/// Submits the WALEntrySimple to group committer when transaction is started,
/// committed, or aborted. It updates mPrevTxCommitTs to notify the group commit
/// thread to flush WAL records for finished (committed or aborted)
/// transactions.
///
/// NOTE: users should call ReserveWALEntrySimple() firstly to initialize the
/// WALEntrySimple to be flushed in the wal ring buffer.
void Logging::SubmitWALEntrySimple() {
  if (!((mWalBuffered >= mTxWalBegin) ||
        (mWalBuffered + sizeof(WALEntrySimple) < mTxWalBegin))) {
    Worker::my().mActiveTx.mWalExceedBuffer = true;
  }

  DEBUG_BLOCK() {
    mActiveWALEntrySimple->computeCRC();

    auto entryPtr = mWalBuffer + mWalBuffered;
    auto entry = reinterpret_cast<WALEntrySimple*>(entryPtr);
    auto doc = entry->ToJSON();

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc->Accept(writer);

    LOG(INFO) << "Submit WALEntrySimple: " << buffer.GetString();
  }

  mWalBuffered += sizeof(WALEntrySimple);
  auto flushReqCopy = mWalFlushReq.getNoSync();
  flushReqCopy.mWalBuffered = mWalBuffered;
  flushReqCopy.mPrevTxCommitTs = Worker::my().mActiveTx.startTS();
  mWalFlushReq.SetSync(flushReqCopy);
}

/// @brief SubmitWALEntryComplex submits the wal record to group committer when
/// it is ready to flush to disk.
/// @param totalSize is the size of the wal record to be flush.
void Logging::SubmitWALEntryComplex(u64 totalSize) {
  if (!((mWalBuffered >= mTxWalBegin) ||
        (mWalBuffered + totalSize < mTxWalBegin))) {
    Worker::my().mActiveTx.mWalExceedBuffer = true;
  }
  DEBUG_BLOCK() {
    mActiveWALEntryComplex->computeCRC();
  }
  COUNTERS_BLOCK() {
    WorkerCounters::myCounters().wal_write_bytes += totalSize;
  }
  DCHECK(totalSize % 8 == 0);
  mWalBuffered += totalSize;
  UpdateWalFlushReq();
}

// Called by worker, so concurrent writes on the buffer
void Logging::iterateOverCurrentTXEntries(
    std::function<void(const WALEntry& entry)> callback) {
  u64 cursor = mTxWalBegin;
  while (cursor != mWalBuffered) {
    const WALEntry& entry = *reinterpret_cast<WALEntry*>(mWalBuffer + cursor);
    ENSURE(entry.size > 0);
    DEBUG_BLOCK() {
      if (entry.type != WALEntry::TYPE::CARRIAGE_RETURN)
        entry.checkCRC();
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
