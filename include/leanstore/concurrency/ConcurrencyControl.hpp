#pragma once

#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "leanstore/concurrency/HistoryStorage.hpp"
#include "leanstore/sync/HybridLatch.hpp"
#include "leanstore/utils/Log.hpp"

#include <atomic>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <utility>
#include <vector>

namespace leanstore {
class LeanStore;
} // namespace leanstore

namespace leanstore::cr {

//! The commit log of the current worker thread. It's used for MVCC visibility check. It's a vector
//! of (commitTs, startTs) pairs. Everytime when a transaction is committed, a (commitTs, startTs)
//! pair is appended to the commit log. It's compacted according to the current active transactions
//! in the system when full.
class CommitTree {
public:
  //! The hybrid latch to guard the commit log.
  storage::HybridLatch mLatch;

  //! The capacity of the commit log. Commit log is compacted when full.
  uint64_t mCapacity;

  //! The vector to store all the (commitTs, startTs) pairs.
  std::vector<std::pair<TXID, TXID>> mCommitLog;

public:
  //! Constructor.
  CommitTree(const uint64_t numWorkers) : mCapacity(numWorkers + 1) {
    mCommitLog.reserve(mCapacity);
  }

public:
  //! AppendCommitLog is called when a transaction is committed. It appends the (commitTs, startTs)
  //! pair to the commit log. Transactions are sequential in one worker, so the commitTs and startTs
  //! are also increasing in the commit log of one worker
  //! @param startTs: the start timestamp of the transaction.
  //! @param commitTs: the commit timestamp of the transaction.
  void AppendCommitLog(TXID startTs, TXID commitTs);

  //! CompactCommitLog is called when the commit log is full in the begging of a transaction. It
  //! keeps the latest (commitTs, startTs) in the commit log, and the commit log that is visible for
  //! other running transactions.  All the unused commit logs are removed in this function.
  void CompactCommitLog();

  //! Lcb is short for Last Committed Before. LCB(self, startTs) returns the largest transaction id
  //! that is committed before the given timestamp on the current. Given a start timestamp, it's
  //! read view can be determined by all the LCB calls on all the worker threads.
  //! @param txId: the transaction id to check.
  //! @return: the last committed transaction id before the given timestamp.
  TXID Lcb(TXID txId);

private:
  //! LcbNoLatch is the same as Lcb, but it doesn't acquire the latch on the commit log.
  std::optional<std::pair<TXID, TXID>> lcbNoLatch(TXID startTs);
};

//! The global watermark info. It's used to store the global watermarks of all the worker threads.
struct WaterMarkInfo {
  //! The write mutex to guard all the global watermark info, including the active transaction info
  //! and the watermark info.
  std::shared_mutex mGlobalMutex;

  //! The oldest active transaction id in the store.
  std::atomic<TXID> mOldestActiveTx;

  //! The oldest active short-running transaction id in the store.
  std::atomic<TXID> mOldestActiveShortTx;

  //! The newest active long-running transaction id in the store.
  std::atomic<TXID> mNewestActiveLongTx;

  //! The watermark of all the transactions, versions generated by transactions with id in the range
  //! [0, mWmkOfAllTx] can be completely removed.
  std::atomic<TXID> mWmkOfAllTx;

  //! The watermark of short-running transactions, versions generated by short-running transactions
  //! with id in the range (mWmkOfAllTx, mWmkOfShortTx] can be moved to the graveyard.
  std::atomic<TXID> mWmkOfShortTx;

  //! Update the active transaction info.
  //! Precondition: the caller should acquire the write lock of mGlobalMutex.
  void UpdateActiveTxInfo(TXID oldestTx, TXID oldestShortTx, TXID newestLongTx) {
    mOldestActiveTx.store(oldestTx, std::memory_order_release);
    mOldestActiveShortTx.store(oldestShortTx, std::memory_order_release);
    mNewestActiveLongTx.store(newestLongTx, std::memory_order_release);
    LS_DLOG("Global watermark updated, oldestActiveTx={}, "
            "oldestActiveShortTx={}, netestActiveLongTx={}",
            mOldestActiveTx.load(), mOldestActiveShortTx.load(), mNewestActiveLongTx.load());
  }

  //! Update the global watermarks.
  //! Precondition: the caller should acquire the write lock of mGlobalMutex.
  void UpdateWmks(TXID wmkOfAll, TXID wmkOfShort) {
    mWmkOfAllTx.store(wmkOfAll, std::memory_order_release);
    mWmkOfShortTx.store(wmkOfShort, std::memory_order_release);
    LS_DLOG("Global watermarks updated, wmkOfAllTx={}, wmkOfShortTx={}", mWmkOfAllTx.load(),
            mWmkOfShortTx.load());
  }

  //! Whether there is any active long-running transaction.
  bool HasActiveLongRunningTx() {
    return mOldestActiveTx != mOldestActiveShortTx;
  }
};

//! The version storage of the current worker thread. All the history versions of transaction
//! removes and updates, all the necessary commit log for MVCC visibility check are stored here.
class ConcurrencyControl {
public:
  ConcurrencyControl(leanstore::LeanStore* store, uint64_t numWorkers)
      : mStore(store),
        mCommitTree(numWorkers) {
  }

public:
  //! The LeanStore it belongs to.
  leanstore::LeanStore* mStore;

  //! The history storage of current worker thread. All history versions of transaction removes and
  //! updates executed by current worker are stored here. It's the version storage of the chained
  //! tuple. Used for MVCC.
  HistoryStorage mHistoryStorage;

  //! The commit log on the current worker. Used for MVCC visibility check.
  CommitTree mCommitTree;

  //! The start timestamp used to calculate the LCB of the target worker.
  std::unique_ptr<TXID[]> mLcbCacheKey;

  //! The LCB of the target worker on the LCB cache key.
  std::unique_ptr<TXID[]> mLcbCacheVal;

  //! The optismistic latch to guard mWmkOfShortTx and mWmkOfAllTx. There is at most one writer at a
  //! time, we can safely check whether the version is odd to verify whether the watermarks are
  //! being updated at the time to read.
  std::atomic<TXID> mWmkVersion = 0;

  //! Versions (tombstones, updates, etc.) generated by all the short-running and long-running
  //! transactions whose id in the range [0, mWmkOfAllTx] can be completely removed.
  std::atomic<TXID> mWmkOfAllTx;

  //! Tombstones generated by all the short-running transactions whose id in the range (mWmkOfAllTx,
  //! mWmkOfShortTx] can be moved to the graveyard. So that newer transactions can avoid seeing them
  //! when traversing the main btree, which saves the performance.
  std::atomic<TXID> mWmkOfShortTx;

  //! The latest commit timestamp of the current worker thread. It's updated everytime when a
  //! transaction is committed.
  std::atomic<TXID> mLatestCommitTs = 0;

  //! The latest commit timestamp of the current worker thread.
  std::atomic<TXID> mUpdatedLatestCommitTs = 0;

  //! Tombstones generated by all the short-running transactions whose id is in the range [0,
  //! mCleanedWmkOfShortTx) are all moved to tombstone. It is reset after each GC round.
  TXID mCleanedWmkOfShortTx = 0;

  //! A snapshot of mWmkOfAllTx, copied from mWmkOfAllTx at the beginning of each round of garbage
  //! collection.
  TXID mLocalWmkOfAllTx;

  //! A snapshot of mWmkOfShortTx, copied from mWmkOfShortTx at the beginning of each round of
  //! garbage collection.
  TXID mLocalWmkOfShortTx;

  //! A snapshot of global watermark, copied from global watermark info at the beginning of each
  //! transaction start. Used for visibility check of the current transaction.
  TXID mGlobalWmkOfAllTx = 0;

public:
  //! Get the older version in version storage according to the given (newerWorkerId, newerTxId,
  //! newerCommandId). The callback function is called with the version data when the version is
  //! found.
  ///
  //! NOTE: Version is retrieved from newest to oldest.
  ///
  //! @param newerWorkerId: the worker id of the newer version.
  //! @param newerTxId: the transaction id of the newer version.
  //! @param newerCommandId: the command id of the newer version.
  //! @param getCallback: the callback function to be called when the version is found.
  //! @return: true if the version is found, false otherwise.
  inline bool GetVersion(WORKERID newerWorkerId, TXID newerTxId, COMMANDID newerCommandId,
                         std::function<void(const uint8_t*, uint64_t versionSize)> getCallback) {
    auto isRemoveCommand = newerCommandId & kRemoveCommandMark;
    return Other(newerWorkerId)
        .mHistoryStorage.GetVersion(newerTxId, newerCommandId, isRemoveCommand, getCallback);
  }

  //! Put a version to the version storage. The callback function is called with the version data
  //! when the version is inserted, usually used to initialize the actual version payload.
  COMMANDID PutVersion(TREEID treeId, bool isRemoveCommand, uint64_t versionSize,
                       std::function<void(uint8_t*)> putCallback);

  //! Whether a tuple written by workerId in txId is visible for the current active transaction.
  bool VisibleForMe(WORKERID workerId, TXID txId);

  //! Whether a tuple written in txId is visible for all the current active transactions.
  bool VisibleForAll(TXID txId);

  //! Garbage collect the version storage. It's called at the end of each transaction. It updates
  //! the global and local watermarks, and removes the unused versions from the version storage.
  void GarbageCollection();

  //! Get the version storge in other worker thread.
  ConcurrencyControl& Other(WORKERID otherWorkerId);

private:
  //! Update global watermarks of all the worker threads before GC.
  void updateGlobalTxWatermarks();

  //! Update local watermarks of the current worker thread before GC.
  void updateLocalWatermarks();
};

} // namespace leanstore::cr