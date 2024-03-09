#pragma once

#include "concurrency/Transaction.hpp"
#include "concurrency/WalEntry.hpp"
#include "leanstore/Units.hpp"
#include "sync/OptimisticGuarded.hpp"

#include <glog/logging.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <atomic>
#include <functional>
#include <mutex>
#include <vector>

namespace leanstore::cr {

/// Used to sync wal flush request to group commit thread for each worker
/// thread.
struct WalFlushReq {
  /// Used for optimistic locking.
  uint64_t mVersion = 0;

  /// The offset in the wal ring buffer.
  uint64_t mWalBuffered = 0;

  /// GSN of the current WAL record.
  uint64_t mCurrGSN = 0;

  /// ID of the current transaction.
  TXID mCurrTxId = 0;

  WalFlushReq(uint64_t walBuffered = 0, uint64_t currGSN = 0, TXID currTxId = 0)
      : mVersion(0),
        mWalBuffered(walBuffered),
        mCurrGSN(currGSN),
        mCurrTxId(currTxId) {
  }
};

template <typename T> class WalPayloadHandler;

/// Helps to transaction concurrenct control and write-ahead logging.
class Logging {
public:
  LID mPrevLSN;

  /// The active simple WalEntry for the current transaction, usually used for
  /// transaction start, commit, or abort.
  ///
  /// NOTE: Either mActiveWALEntrySimple or mActiveWALEntryComplex is effective
  /// during transaction processing.
  WALEntrySimple* mActiveWALEntrySimple;

  /// The active complex WalEntry for the current transaction, usually used for
  /// insert, update, delete, or btree related operations.
  ///
  /// NOTE: Either mActiveWALEntrySimple or mActiveWALEntryComplex is effective
  /// during transaction processing.
  WALEntryComplex* mActiveWALEntryComplex;

  /// Protects mTxToCommit
  std::mutex mTxToCommitMutex;

  /// The queue for each worker thread to store pending-to-commit transactions
  /// which have remote dependencies.
  std::vector<Transaction> mTxToCommit;

  /// Protects mTxToCommit
  std::mutex mRfaTxToCommitMutex;

  /// The queue for each worker thread to store pending-to-commit transactions
  /// which doesn't have any remote dependencies.
  std::vector<Transaction> mRfaTxToCommit;

  /// Represents the maximum commit timestamp in the worker. Transactions in the
  /// worker are committed if their commit timestamps are smaller than it.
  ///
  /// Updated by group committer
  std::atomic<TXID> mSignaledCommitTs = 0;

  storage::OptimisticGuarded<WalFlushReq> mWalFlushReq;

  /// The ring buffer of the current worker thread. All the wal entries of the
  /// current worker are writtern to this ring buffer firstly, then flushed to
  /// disk by the group commit thread.
  alignas(512) uint8_t* mWalBuffer;

  /// The size of the wal ring buffer.
  uint64_t mWalBufferSize;

  /// Used to track the write order of wal entries.
  LID mLsnClock = 0;

  /// Used to track transaction dependencies.
  uint64_t mGSNClock = 0;

  /// The written offset of the wal ring buffer.
  uint64_t mWalBuffered = 0;

  /// Represents the flushed offset in the wal ring buffer.  The wal ring buffer
  /// is firstly written by the worker thread then flushed to disk file by the
  /// group commit thread.
  std::atomic<uint64_t> mWalFlushed = 0;

  // The global min flushed GSN when transaction started. Pages whose GSN larger
  // than this value might be modified by other transactions running at the same
  // time, which cause the remote transaction dependency.
  uint64_t mTxReadSnapshot;

  /// Whether the active transaction has accessed data written by other worker
  /// transactions, i.e. dependens on the transactions on other workers.
  bool mHasRemoteDependency = false;

  /// The first WAL record of the current active transaction.
  uint64_t mTxWalBegin;

public:
  inline void UpdateSignaledCommitTs(const LID signaledCommitTs) {
    mSignaledCommitTs.store(signaledCommitTs, std::memory_order_release);
  }

  inline bool TxUnCommitted(const TXID dependency) {
    return mSignaledCommitTs.load() < dependency;
  }

  void ReserveContiguousBuffer(uint32_t requestedSize);

  // Iterate over current TX entries
  void IterateCurrentTxWALs(
      std::function<void(const WalEntry& entry)> callback);

  WALEntrySimple& ReserveWALEntrySimple(WalEntry::Type type);

  void SubmitWALEntrySimple();

  void WriteSimpleWal(WalEntry::Type type);

  template <typename T, typename... Args>
  WalPayloadHandler<T> ReserveWALEntryComplex(uint64_t payloadSize, PID pageId,
                                              LID gsn, TREEID treeId,
                                              Args&&... args);

  void SubmitWALEntryComplex(uint64_t totalSize);

  inline uint64_t GetCurrentGsn() {
    return mGSNClock;
  }

  inline void SetCurrentGsn(uint64_t gsn) {
    mGSNClock = gsn;
  }

private:
  void publishWalBufferedOffset();

  void publishWalFlushReq();

  uint32_t walContiguousFreeSpace();
};

} // namespace leanstore::cr