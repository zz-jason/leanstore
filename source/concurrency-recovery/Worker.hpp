#pragma once

#include "HistoryTreeInterface.hpp"
#include "Transaction.hpp"
#include "WALEntry.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "shared-headers/Exceptions.hpp"
#include "utils/Defer.hpp"
#include "utils/OptimisticSpinStruct.hpp"

#include <glog/logging.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <vector>

namespace leanstore::cr {

static constexpr u16 kWorkerLimit = std::numeric_limits<WORKERID>::max();

/// Used to sync wal flush request to group commit thread for each worker
/// thread.
struct WalFlushReq {
  /// Used for optimistic locking.
  u64 mVersion = 0;

  /// The offset in the wal ring buffer.
  u64 mWalBuffered = 0;

  /// GSN of the current WAL record.
  u64 mCurrGSN = 0;

  /// ID of the current transaction.
  TXID mCurrTxId = 0;

  WalFlushReq(u64 walBuffered = 0, u64 currGSN = 0, TXID currTxId = 0)
      : mVersion(0),
        mWalBuffered(walBuffered),
        mCurrGSN(currGSN),
        mCurrTxId(currTxId) {
  }
};

/// Helps to transaction concurrenct control and write-ahead logging.
class Logging {
public:
  /// The minimum flushed GSN among all worker threads. Transactions whose max
  /// observed GSN not larger than sGlobalMinFlushedGSN can be committed safely.
  static std::atomic<u64> sGlobalMinFlushedGSN;

  /// The maximum flushed GSN among all worker threads in each group commit
  /// round. It is updated by the group commit thread and used to update the GCN
  /// counter of the current worker thread to prevent GSN from skewing and
  /// undermining RFA.
  static std::atomic<u64> sGlobalMaxFlushedGSN;

public:
  static void UpdateGlobalMinFlushedGSN(u64 toUpdate) {
    sGlobalMinFlushedGSN.store(toUpdate, std::memory_order_release);
  }

  static void UpdateGlobalMaxFlushedGSN(LID toUpdate) {
    sGlobalMaxFlushedGSN.store(toUpdate, std::memory_order_release);
  }

public:
  LID mPrevLSN;

  /// The active simple WALEntry for the current transaction, usually used for
  /// transaction start, commit, or abort.
  ///
  /// NOTE: Either mActiveWALEntrySimple or mActiveWALEntryComplex is effective
  /// during transaction processing.
  WALEntrySimple* mActiveWALEntrySimple;

  /// The active complex WALEntry for the current transaction, usually used for
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

  utils::OptimisticSpinStruct<WalFlushReq> mWalFlushReq;

  /// The ring buffer of the current worker thread. All the wal entries of the
  /// current worker are writtern to this ring buffer firstly, then flushed to
  /// disk by the group commit thread.
  alignas(512) u8* mWalBuffer;

  /// Used to track the write order of wal entries.
  LID mLsnClock = 0;

  /// Used to track transaction dependencies.
  u64 mGSNClock = 0;

  /// The written offset of the wal ring buffer.
  u64 mWalBuffered = 0;

  /// Represents the flushed offset in the wal ring buffer.  The wal ring buffer
  /// is firstly written by the worker thread then flushed to disk file by the
  /// group commit thread.
  std::atomic<u64> mWalFlushed = 0;

  // The global min flushed GSN when transaction started. Pages whose GSN larger
  // than this value might be modified by other transactions running at the same
  // time, which cause the remote transaction dependency.
  u64 mTxReadSnapshot;

  /// Whether the active transaction has accessed data written by other worker
  /// transactions, i.e. dependens on the transactions on other workers.
  bool mHasRemoteDependency = false;

  /// The first WAL record of the current active transaction.
  u64 mTxWalBegin;

public:
  inline void UpdateSignaledCommitTs(const LID signaledCommitTs) {
    mSignaledCommitTs.store(signaledCommitTs, std::memory_order_release);
  }

  inline bool TxUnCommitted(const TXID dependency) {
    return mSignaledCommitTs.load() < dependency;
  }

  void WalEnsureEnoughSpace(u32 requestedSize);

  // Iterate over current TX entries
  void IterateCurrentTxWALs(
      std::function<void(const WALEntry& entry)> callback);

  WALEntrySimple& ReserveWALEntrySimple(WALEntry::TYPE type);

  void SubmitWALEntrySimple();

  void WriteSimpleWal(WALEntry::TYPE type);

  template <typename T, typename... Args>
  WALPayloadHandler<T> ReserveWALEntryComplex(u64 payloadSize, PID pageId,
                                              LID gsn, TREEID treeId,
                                              Args&&... args);

  void SubmitWALEntryComplex(u64 totalSize);

  inline u64 GetCurrentGsn() {
    return mGSNClock;
  }

  inline void SetCurrentGsn(u64 gsn) {
    mGSNClock = gsn;
  }

private:
  void publishWalBufferedOffset();

  void publishWalFlushReq();

  u32 walFreeSpace();

  u32 walContiguousFreeSpace();
};

// LWM: start timestamp of the transaction that has its effect visible by all
// in its class
class ConcurrencyControl {
public:
  // Commmit Tree (single-writer multiple-reader)
  // TODO(jian.z): replace commit tree with a vector and mutex
  struct CommitTree {
  public:
    std::shared_mutex mutex;

    std::pair<TXID, TXID>* mCommitLog;

    u64 cursor = 0;

    u64 capacity;

  public:
    // CleanUpCommitLog is called when the commit log is full in the begging of
    // a transaction. It keeps the latest (commitTs, startTs) in the commit log,
    // and the commit log that is visible for other running transactions. It's
    // kind of like a garbage collection for the commit log.
    void CleanUpCommitLog();

    TXID AppendCommitLog(TXID startTs);

    TXID Lcb(TXID startTs);

    CommitTree(const u64 numWorkers) : capacity(numWorkers + 1) {
      mCommitLog = new std::pair<TXID, TXID>[capacity];
    }

  private:
    std::optional<std::pair<TXID, TXID>> lcbNoLatch(TXID startTs);
  };

  enum class Visibility : u8 { kVisible, kVisibleNextRound, kUndetermined };

public:
  ConcurrencyControl(const u64 numWorkers)
      : mHistoryTree(nullptr),
        mCommitTree(numWorkers) {
  }

public:
  /// The global timestamp oracle, used to generate start and commit timestamps
  /// for all transactions in the system.
  static std::atomic<u64> sTimeStampOracle;

public:
  /// The optismistic latch to guard mWmkOfShortTx and mWmkOfAllTx.
  std::atomic<TXID> mWmkVersion = 0;

  /// The oldest commit timestamp (lower watermark) for short-running
  /// transactions, under which all the tombstones generated by short-running
  /// transactions can be moved to the graveyard, so that newer transactions can
  /// avoid seeing them when traversing the main btree, which saves the
  /// performance.
  std::atomic<TXID> mWmkOfShortTx;

  /// The oldest commit timestamp (lower watermark) for all transactions, under
  /// which all the versions generated by both short-running and long-running
  /// transactions can be garbage collected.
  std::atomic<TXID> mWmkOfAllTx;

  /// The latest commit timestamp of the current worker thread. It's updated
  /// everytime when a transaction is committed.
  std::atomic<TXID> mLatestCommitTs = 0;

  /// The latest commit timestamp of the current worker thread.
  std::atomic<TXID> mUpdatedLatestCommitTs = 0;

  /// A snapshot of mWmkOfAllTx, copied from mWmkOfAllTx at the beginning of
  /// each round of garbage collection.
  TXID mLocalWmkOfAllTx;

  /// A snapshot of mWmkOfShortTx, copied from mWmkOfShortTx at the beginning of
  /// each round of garbage collection.
  TXID mLocalWmkOfShortTx;

  /// A snapshot of sGlobalWmkOfAllTx, copied from sGlobalWmkOfAllTx at the
  /// beginning of each transaction start. Used for visibility check of the
  /// current transaction.
  TXID mGlobalWmkOfAllTxSnapshot = 0;

  /// The start timestamp used to calculate the LCB of the target worker.
  unique_ptr<TXID[]> mLcbCacheKey;

  /// The LCB of the target worker on the LCB cache key.
  unique_ptr<TXID[]> mLcbCacheVal;

  unique_ptr<TXID[]> local_workers_start_ts;

  /// The history tree of the current worker thread. All the history versions of
  /// transaction removes and updates are stored here. It's the version storage
  /// of the chained tuple. Used for MVCC.
  HistoryTreeInterface* mHistoryTree;

  CommitTree mCommitTree;

  /// The oldest commit timestamp (lower watermark) under which all the vertions
  /// generated by short-running transactions are garbage collected.
  u64 mCleanedWmkOfShortTx = 0;

public:
  //-------------------------------------------------------------------------
  // Object utils
  //-------------------------------------------------------------------------
  void GarbageCollection();

  bool VisibleForAll(TXID txId);

  /// Visibility check. Whethe the current tuple is visible for the current
  /// worker transaction.
  bool VisibleForMe(WORKERID workerId, u64 txId);

  ConcurrencyControl& other(WORKERID otherWorkerId);

  u64 PutVersion(TREEID treeId, bool isRemoveCommand, u64 versionSize,
                 std::function<void(u8*)> putCallBack);

  inline bool GetVersion(WORKERID prevWorkerId, TXID prevTxId,
                         COMMANDID prevCommandId,
                         std::function<void(const u8*, u64 versionSize)> cb) {
    utils::Timer timer(CRCounters::MyCounters().cc_ms_history_tree_retrieve);
    const bool isRemoveCommand = prevCommandId & TYPE_MSB(COMMANDID);
    return mHistoryTree->GetVersion(prevWorkerId, prevTxId, prevCommandId,
                                    isRemoveCommand, cb);
  }

private:
  Visibility isVisibleForIt(WORKERID whomWorkerId, WORKERID whatWorkerId,
                            u64 tts);

  Visibility isVisibleForIt(WORKERID whomWorkerId, TXID commitTs);

  TXID getCommitTimestamp(WORKERID workerId, TXID startTs);

  /// Update global watermarks, should be called in each transaction commit.
  void updateGlobalTxWatermarks();

  void updateLocalWatermarks();
};

/**
 * Abbreviations:
 * 1. WT (Worker Thread),
 * 2. GCT (Group Commit Thread or whoever writes the WAL)
 *
 * Stages:
 * -> pre-committed (SI passed)
 * -> hardened (its own WALs are written and fsync)
 * -> committed/signaled (all dependencies are flushed and the user got OK)
 */
class Worker {
public:
  /// The write-ahead logging component.
  Logging mLogging;

  ConcurrencyControl cc;

  u64 mCommandId = 0;

  /// The current running transaction.
  Transaction mActiveTx;

  /// ID of the current worker itself.
  const u64 mWorkerId;

  /// All the workers.
  std::vector<Worker*>& mAllWorkers;

  /// Number of all the workers.
  const u64 mNumAllWorkers;

public:
  Worker(u64 workerId, std::vector<Worker*>& allWorkers, u64 numWorkers);

  ~Worker();

public:
  bool IsTxStarted() {
    return mActiveTx.mState == TxState::kStarted;
  }

  void StartTx(TxMode mode = TxMode::kShortRunning,
               IsolationLevel level = IsolationLevel::kSnapshotIsolation,
               bool isReadOnly = false);

  void CommitTx();

  void AbortTx();

  void shutdown();

public:
  static thread_local std::unique_ptr<Worker> sTlsWorker;

  // Concurrency Control
  static std::unique_ptr<std::atomic<u64>[]> sLatestStartTs;
  static std::atomic<TXID> sGlobalOldestShortTxId;
  static std::atomic<TXID> sGlobalWmkOfShortTx;
  static std::atomic<TXID> sGlobalOldestTxId;
  static std::atomic<TXID> sGlobalWmkOfAllTx;
  static std::atomic<TXID> sGlobalNewestLongTxId;
  static std::shared_mutex sGlobalMutex;

  static constexpr u64 kWorkersBits = 8;
  static constexpr u64 kRcBit = (1ull << 63);
  static constexpr u64 kLongRunningBit = (1ull << 62);
  static constexpr u64 kOltpOlapSameBit = kLongRunningBit;
  static constexpr u64 kCleanBitsMask = ~(kRcBit | kLongRunningBit);

  // TXID: [ kRcBit | kLongRunningBit  | id];
  // LWM:  [ kRcBit | kOltpOlapSameBit | id];
  static constexpr s64 kCrEntrySize = sizeof(WALEntrySimple);

public:
  inline static Worker& my() {
    return *Worker::sTlsWorker.get();
  }
};

// Shortcuts
inline Transaction& ActiveTx() {
  return cr::Worker::my().mActiveTx;
}

//------------------------------------------------------------------------------
// WALPayloadHandler
//------------------------------------------------------------------------------

template <typename T> inline void WALPayloadHandler<T>::SubmitWal() {
  SCOPED_DEFER(DEBUG_BLOCK() {
    auto walDoc = cr::Worker::my().mLogging.mActiveWALEntryComplex->ToJson();
    auto entry = reinterpret_cast<T*>(
        cr::Worker::my().mLogging.mActiveWALEntryComplex->payload);
    auto payloadDoc = entry->ToJson();
    walDoc->AddMember("payload", *payloadDoc, walDoc->GetAllocator());
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    walDoc->Accept(writer);
    LOG(INFO) << "SubmitWal"
              << ", workerId=" << Worker::my().mWorkerId
              << ", startTs=" << Worker::my().mActiveTx.mStartTs
              << ", curGSN=" << Worker::my().mLogging.GetCurrentGsn()
              << ", walJson=" << buffer.GetString();
  });

  cr::Worker::my().mLogging.SubmitWALEntryComplex(mTotalSize);
}

//------------------------------------------------------------------------------
// Logging
//------------------------------------------------------------------------------

template <typename T, typename... Args>
WALPayloadHandler<T> Logging::ReserveWALEntryComplex(u64 payloadSize,
                                                     PID pageId, LID psn,
                                                     TREEID treeId,
                                                     Args&&... args) {
  SCOPED_DEFER(mPrevLSN = mActiveWALEntryComplex->lsn);

  auto entryLSN = mLsnClock++;
  auto* entryPtr = mWalBuffer + mWalBuffered;
  auto entrySize = sizeof(WALEntryComplex) + payloadSize;
  DCHECK(walContiguousFreeSpace() >= entrySize);

  mActiveWALEntryComplex =
      new (entryPtr) WALEntryComplex(entryLSN, entrySize, psn, treeId, pageId);
  mActiveWALEntryComplex->mPrevLSN = mPrevLSN;
  auto& curWorker = leanstore::cr::Worker::my();
  mActiveWALEntryComplex->InitTxInfo(&curWorker.mActiveTx, curWorker.mWorkerId);

  auto* payloadPtr = mActiveWALEntryComplex->payload;
  auto walPayload = new (payloadPtr) T(std::forward<Args>(args)...);
  return {walPayload, entrySize, entryLSN};
}

//------------------------------------------------------------------------------
// ConcurrencyControl
//------------------------------------------------------------------------------

inline ConcurrencyControl& ConcurrencyControl::other(WORKERID otherWorkerId) {
  return Worker::my().mAllWorkers[otherWorkerId]->cc;
}

inline u64 ConcurrencyControl::PutVersion(
    TREEID treeId, bool isRemoveCommand, u64 versionSize,
    std::function<void(u8*)> putCallBack) {
  utils::Timer timer(CRCounters::MyCounters().cc_ms_history_tree_insert);
  auto& curWorker = Worker::my();
  const u64 commandId =
      (curWorker.mCommandId++) | ((isRemoveCommand) ? TYPE_MSB(COMMANDID) : 0);
  mHistoryTree->PutVersion(curWorker.mWorkerId, curWorker.mActiveTx.mStartTs,
                           commandId, treeId, isRemoveCommand, versionSize,
                           putCallBack);
  return commandId;
}

} // namespace leanstore::cr
