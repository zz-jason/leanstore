#pragma once

#include "leanstore-c/perf_counters.h"
#include "leanstore/Units.hpp"
#include "leanstore/concurrency/Transaction.hpp"
#include "leanstore/sync/OptimisticGuarded.hpp"
#include "leanstore/utils/CounterUtil.hpp"

#include <algorithm>
#include <atomic>
#include <functional>
#include <mutex>
#include <vector>

namespace leanstore::cr {

//! forward declarations
class WalEntry;
class WalEntryComplex;

//! Used to sync wal flush request between group committer and worker.
struct WalFlushReq {
  //! Used for optimistic locking.
  uint64_t mVersion = 0;

  //! The offset in the wal ring buffer.
  uint64_t mWalBuffered = 0;

  //! The maximum system transasction ID written by the worker.
  //! NOTE: can only be updated when all the WAL entries belonging to the system transaction are
  //! written to the wal ring buffer.
  TXID mSysTxWrittern = 0;

  //! ID of the current transaction.
  //! NOTE: can only be updated when all the WAL entries belonging to the user transaction are
  //! written to the wal ring buffer.
  TXID mCurrTxId = 0;

  WalFlushReq(uint64_t walBuffered = 0, uint64_t sysTxWrittern = 0, TXID currTxId = 0)
      : mVersion(0),
        mWalBuffered(walBuffered),
        mSysTxWrittern(sysTxWrittern),
        mCurrTxId(currTxId) {
  }
};

template <typename T>
class WalPayloadHandler;

//! Helps to transaction concurrenct control and write-ahead logging.
class Logging {
public:
  LID mPrevLSN;

  //! The active complex WalEntry for the current transaction, usually used for insert, update,
  //! delete, or btree related operations.
  //!
  //! NOTE: Either mActiveWALEntrySimple or mActiveWALEntryComplex is effective during transaction
  //! processing.
  WalEntryComplex* mActiveWALEntryComplex;

  //! Protects mTxToCommit
  std::mutex mTxToCommitMutex;

  //! The queue for each worker thread to store pending-to-commit transactions which have remote
  //! dependencies.
  std::vector<Transaction> mTxToCommit;

  //! Protects mTxToCommit
  std::mutex mRfaTxToCommitMutex;

  //! The queue for each worker thread to store pending-to-commit transactions which doesn't have
  //! any remote dependencies.
  std::vector<Transaction> mRfaTxToCommit;

  //! Represents the maximum commit timestamp in the worker. Transactions in the worker are
  //! committed if their commit timestamps are smaller than it.
  //!
  //! Updated by group committer
  std::atomic<TXID> mSignaledCommitTs = 0;

  storage::OptimisticGuarded<WalFlushReq> mWalFlushReq;

  //! The ring buffer of the current worker thread. All the wal entries of the current worker are
  //! writtern to this ring buffer firstly, then flushed to disk by the group commit thread.
  alignas(512) uint8_t* mWalBuffer;

  //! The size of the wal ring buffer.
  uint64_t mWalBufferSize;

  //! Used to track the write order of wal entries.
  LID mLsnClock = 0;

  //! The maximum writtern system transaction ID in the worker.
  TXID mSysTxWrittern = 0;

  //! The written offset of the wal ring buffer.
  uint64_t mWalBuffered = 0;

  //! Represents the flushed offset in the wal ring buffer.  The wal ring buffer is firstly written
  //! by the worker thread then flushed to disk file by the group commit thread.
  std::atomic<uint64_t> mWalFlushed = 0;

  //! The first WAL record of the current active transaction.
  uint64_t mTxWalBegin;

public:
  void UpdateSignaledCommitTs(const LID signaledCommitTs) {
    mSignaledCommitTs.store(signaledCommitTs, std::memory_order_release);
  }

  void WaitToCommit(const TXID commitTs) {
    COUNTER_INC(&tlsPerfCounters.mTxCommitWait);
    while (!(commitTs <= mSignaledCommitTs.load())) {
    }
  }

  void ReserveContiguousBuffer(uint32_t requestedSize);

  //! Iterate over current TX entries
  void IterateCurrentTxWALs(std::function<void(const WalEntry& entry)> callback);

  void WriteWalTxAbort();
  void WriteWalTxFinish();
  void WriteWalCarriageReturn();

  template <typename T, typename... Args>
  WalPayloadHandler<T> ReserveWALEntryComplex(uint64_t payloadSize, PID pageId, LID psn,
                                              TREEID treeId, Args&&... args);

  //! Submits wal record to group committer when it is ready to flush to disk.
  //! @param totalSize size of the wal record to be flush.
  void SubmitWALEntryComplex(uint64_t totalSize);

  void UpdateSysTxWrittern(TXID sysTxId) {
    mSysTxWrittern = std::max(mSysTxWrittern, sysTxId);
  }

private:
  void publishWalBufferedOffset();

  void publishWalFlushReq();

  //! Calculate the continuous free space left in the wal ring buffer. Return
  //! size of the contiguous free space.
  uint32_t walContiguousFreeSpace();
};

} // namespace leanstore::cr