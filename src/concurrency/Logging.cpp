#include "leanstore/concurrency/Logging.hpp"

#include "leanstore/Exceptions.hpp"
#include "leanstore/concurrency/WalEntry.hpp"
#include "leanstore/concurrency/WorkerContext.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Log.hpp"
#include "utils/ToJson.hpp"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cstring>

namespace leanstore::cr {

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
  // Reserve space
  auto size = sizeof(WalTxAbort);
  ReserveContiguousBuffer(size);

  // Initialize a WalTxAbort
  auto* data = mWalBuffer + mWalBuffered;
  std::memset(data, 0, size);
  auto* entry [[maybe_unused]] = new (data) WalTxAbort(size);

  // Submit the WalTxAbort to group committer
  mWalBuffered += size;
  publishWalFlushReq();

  LS_DLOG("WriteWalTxAbort, workerId={}, startTs={}, curGSN={}, walJson={}",
          WorkerContext::My().mWorkerId, WorkerContext::My().mActiveTx.mStartTs, GetCurrentGsn(),
          utils::ToJsonString(entry));
}

void Logging::WriteWalTxFinish() {
  // Reserve space
  auto size = sizeof(WalTxFinish);
  ReserveContiguousBuffer(size);

  // Initialize a WalTxFinish
  auto* data = mWalBuffer + mWalBuffered;
  std::memset(data, 0, size);
  auto* entry [[maybe_unused]] = new (data) WalTxFinish(WorkerContext::My().mActiveTx.mStartTs);

  // Submit the WalTxAbort to group committer
  mWalBuffered += size;
  publishWalFlushReq();

  LS_DLOG("WriteWalTxFinish, workerId={}, startTs={}, curGSN={}, walJson={}",
          WorkerContext::My().mWorkerId, WorkerContext::My().mActiveTx.mStartTs, GetCurrentGsn(),
          utils::ToJsonString(entry));
}

void Logging::WriteWalCarriageReturn() {
  LS_DCHECK(mWalFlushed <= mWalBuffered,
            "CarriageReturn should only used for the last bytes in the wal buffer");
  auto entrySize = mWalBufferSize - mWalBuffered;
  auto* entryPtr = mWalBuffer + mWalBuffered;
  new (entryPtr) WalCarriageReturn(entrySize);
  mWalBuffered = 0;
  publishWalBufferedOffset();
}

void Logging::SubmitWALEntryComplex(uint64_t totalSize) {
  mActiveWALEntryComplex->mCrc32 = mActiveWALEntryComplex->ComputeCRC32();
  mWalBuffered += totalSize;
  publishWalFlushReq();

  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().wal_write_bytes += totalSize;
  }
  LS_DLOG("SubmitWal, workerId={}, startTs={}, curGSN={}, walJson={}",
          WorkerContext::My().mWorkerId, WorkerContext::My().mActiveTx.mStartTs, GetCurrentGsn(),
          utils::ToJsonString(mActiveWALEntryComplex));
}

void Logging::publishWalBufferedOffset() {
  mWalFlushReq.UpdateAttribute(&WalFlushReq::mWalBuffered, mWalBuffered);
}

void Logging::publishWalFlushReq() {
  WalFlushReq current(mWalBuffered, GetCurrentGsn(), WorkerContext::My().mActiveTx.mStartTs);
  mWalFlushReq.Set(current);
}

// Called by worker, so concurrent writes on the buffer
void Logging::IterateCurrentTxWALs(std::function<void(const WalEntry& entry)> callback) {
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
