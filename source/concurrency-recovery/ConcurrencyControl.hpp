#pragma once

#include "HistoryTreeInterface.hpp"
#include "LeanStore.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "shared-headers/Units.hpp"
#include "utils/Misc.hpp"

#include <atomic>
#include <optional>
#include <shared_mutex>
#include <utility>
#include <vector>

namespace leanstore {
class LeanStore;
} // namespace leanstore

namespace leanstore::cr {

/// The commit log of the current worker thread. It's used for MVCC visibility
/// check. It's a vector of (commitTs, startTs) pairs. Everytime when a
/// transaction is committed, a (commitTs, startTs) pair is appended to the
/// commit log. It's compacted according to the current active transactions in
/// the system when full.
class CommitTree {
public:
  /// The mutex to guard the commit log.
  std::shared_mutex mMutex;

  /// The capacity of the commit log. Commit log is compacted when full.
  u64 mCapacity;

  /// The vector to store all the (commitTs, startTs) pairs.
  std::vector<std::pair<TXID, TXID>> mCommitLog;

public:
  /// Constructor.
  CommitTree(const u64 numWorkers) : mCapacity(numWorkers + 1) {
    mCommitLog.reserve(mCapacity);
  }

public:
  /// AppendCommitLog is called when a transaction is committed. It appends the
  /// (commitTs, startTs) pair to the commit log. Transactions are sequential in
  /// one worker, so the commitTs and startTs are also increasing in the commit
  /// log of one worker
  /// @param startTs: the start timestamp of the transaction.
  /// @param commitTs: the commit timestamp of the transaction.
  void AppendCommitLog(TXID startTs, TXID commitTs);

  /// CompactCommitLog is called when the commit log is full in the begging of a
  /// transaction. It keeps the latest (commitTs, startTs) in the commit log,
  /// and the commit log that is visible for other running transactions.  All
  /// the unused commit logs are removed in this function.
  void CompactCommitLog();

  /// Lcb is short for Last Committed Before. LCB(self, startTs) returns the
  /// largest transaction id that is committed before the given timestamp on the
  /// current. Given a start timestamp, it's read view can be determined by all
  /// the LCB calls on all the worker threads.
  /// @param txId: the transaction id to check.
  /// @return: the last committed transaction id before the given timestamp.
  TXID Lcb(TXID txId);

private:
  /// LcbNoLatch is the same as Lcb, but it doesn't acquire the latch on the
  /// commit log.
  std::optional<std::pair<TXID, TXID>> lcbNoLatch(TXID startTs);
};

// Concurrency Control
struct WaterMarkInfo {
  std::shared_mutex mGlobalMutex;
  std::atomic<TXID> mOldestActiveTx;
  std::atomic<TXID> mOldestActiveShortTx;
  std::atomic<TXID> mNewestActiveLongTx;

  std::atomic<TXID> mWmkOfAllTx;
  std::atomic<TXID> mWmkOfShortTx;

  void UpdateActiveTxInfo(TXID oldestTx, TXID oldestShortTx,
                          TXID newestLongTx) {
    mOldestActiveTx.store(oldestTx, std::memory_order_release);
    mOldestActiveShortTx.store(oldestShortTx, std::memory_order_release);
    mNewestActiveLongTx.store(newestLongTx, std::memory_order_release);
    DLOG(INFO) << "Global watermark updated"
               << ", oldestActiveTx=" << mOldestActiveTx
               << ", netestActiveLongTx=" << mNewestActiveLongTx
               << ", oldestActiveShortTx=" << mOldestActiveShortTx;
  }

  void UpdateWmks(TXID wmkOfAll, TXID wmkOfShort) {
    mWmkOfAllTx.store(wmkOfAll, std::memory_order_release);
    mWmkOfShortTx.store(wmkOfShort, std::memory_order_release);
    DLOG(INFO) << "Global watermarks updated"
               << ", wmkOfAllTx=" << mWmkOfAllTx
               << ", wmkOfShortTx=" << mWmkOfShortTx;
  }

  bool HasActiveLongRunningTx() {
    return mOldestActiveTx != mOldestActiveShortTx;
  }
};

/// The version storage of the current worker thread. All the history versions
/// of transaction removes and updates, all the necessary commit log for MVCC
/// visibility check are stored here.
class ConcurrencyControl {
public:
  ConcurrencyControl(leanstore::LeanStore* store, u64 numWorkers)
      : mStore(store),
        mHistoryTree(nullptr),
        mCommitTree(numWorkers) {
  }

public:
  leanstore::LeanStore* mStore;

  /// The history tree of the current worker thread. All the history versions of
  /// transaction removes and updates are stored here. It's the version storage
  /// of the chained tuple. Used for MVCC.
  HistoryTreeInterface* mHistoryTree;

  /// The commit log on the current worker. Used for MVCC visibility check.
  CommitTree mCommitTree;

  /// The start timestamp used to calculate the LCB of the target worker.
  unique_ptr<TXID[]> mLcbCacheKey;

  /// The LCB of the target worker on the LCB cache key.
  unique_ptr<TXID[]> mLcbCacheVal;

  /// The optismistic latch to guard mWmkOfShortTx and mWmkOfAllTx. There is at
  /// most one writer at a time, we can safely check whether the version is odd
  /// to verify whether the watermarks are being updated at the time to read.
  std::atomic<TXID> mWmkVersion = 0;

  /// Versions (tombstones, updates, etc.) generated by all the short-running
  /// and long-running transactions whose id in the range [0, mWmkOfAllTx] can
  /// be completely removed.
  std::atomic<TXID> mWmkOfAllTx;

  /// Tombstones generated by all the short-running transactions whose id in the
  /// range (mWmkOfAllTx, mWmkOfShortTx] can be moved to the graveyard. So that
  /// newer transactions can avoid seeing them when traversing the main btree,
  /// which saves the performance.
  std::atomic<TXID> mWmkOfShortTx;

  /// The latest commit timestamp of the current worker thread. It's updated
  /// everytime when a transaction is committed.
  std::atomic<TXID> mLatestCommitTs = 0;

  /// The latest commit timestamp of the current worker thread.
  std::atomic<TXID> mUpdatedLatestCommitTs = 0;

  /// Tombstones generated by all the short-running transactions whose id is in
  /// the range [0, mCleanedWmkOfShortTx) are all moved to tombstone. It is
  /// reset after each GC round.
  TXID mCleanedWmkOfShortTx = 0;

  /// A snapshot of mWmkOfAllTx, copied from mWmkOfAllTx at the beginning of
  /// each round of garbage collection.
  TXID mLocalWmkOfAllTx;

  /// A snapshot of mWmkOfShortTx, copied from mWmkOfShortTx at the beginning of
  /// each round of garbage collection.
  TXID mLocalWmkOfShortTx;

  /// A snapshot of global watermark, copied from global watermark info at the
  /// beginning of each transaction start. Used for visibility check of the
  /// current transaction.
  TXID mGlobalWmkOfAllTx = 0;

public:
  /// Get the next version in the version storage according to the given
  /// (prevWorkerId, prevTxId, prevCommandId). The callback function is called
  /// with the version data when the version is found.
  ///
  /// NOTE:Version is retrieved from newest to oldest. Previous version is the
  /// newer one.
  ///
  /// @param prevWorkerId: the worker id of the previous version.
  /// @param prevTxId: the transaction id of the previous version.
  /// @param prevCommandId: the command id of the previous version.
  /// @param getCallback: the callback function to be called when the
  /// version is found.
  /// @return: true if the version is found, false otherwise.
  inline bool GetVersion(
      WORKERID prevWorkerId, TXID prevTxId, COMMANDID prevCommandId,
      std::function<void(const u8*, u64 versionSize)> getCallback) {
    utils::Timer timer(CRCounters::MyCounters().cc_ms_history_tree_retrieve);
    auto isRemoveCommand = prevCommandId & TYPE_MSB(COMMANDID);
    return mHistoryTree->GetVersion(prevWorkerId, prevTxId, prevCommandId,
                                    isRemoveCommand, getCallback);
  }

  /// Put a version to the version storage. The callback function is called with
  /// the version data when the version is inserted, usually used to initialize
  /// the actual version payload.
  COMMANDID PutVersion(TREEID treeId, bool isRemoveCommand, u64 versionSize,
                       std::function<void(u8*)> putCallback);

  /// Whether a tuple written by workerId in txId is visible for the current
  /// active transaction.
  bool VisibleForMe(WORKERID workerId, TXID txId);

  /// Whether a tuple written in txId is visible for all the current active
  /// transactions.
  bool VisibleForAll(TXID txId);

  /// Garbage collect the version storage. It's called at the end of each
  /// transaction. It updates the global and local watermarks, and removes the
  /// unused versions from the version storage.
  void GarbageCollection();

  /// Get the version storge in other worker thread.
  ConcurrencyControl& Other(WORKERID otherWorkerId);

private:
  /// Update global watermarks of all the worker threads before GC.
  void updateGlobalTxWatermarks();

  /// Update local watermarks of the current worker thread before GC.
  void updateLocalWatermarks();
};

} // namespace leanstore::cr