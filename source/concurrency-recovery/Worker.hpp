#pragma once

#include "HistoryTreeInterface.hpp"
#include "Transaction.hpp"
#include "WALEntry.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "profiling/counters/WorkerCounters.hpp"

#include "utils/OptimisticSpinStruct.hpp"

#include <glog/logging.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <atomic>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <vector>

namespace leanstore {
namespace cr {

static constexpr u16 STATIC_MAX_WORKERS = std::numeric_limits<WORKERID>::max();

/// @brief WalFlushReq is used to sync wal flush request to group commit thread
/// for each worker thread.
struct WalFlushReq {
  /// Used for optimistic locking.
  u64 mVersion = 0;

  /// Regarding the changes made by the current worker.
  LID mCurrGSN = 0;

  /// The offset in the wal ring buffer.
  u64 mWalBuffered = 0;

  /// The commit TS of the previous transaction.
  TXID mPrevTxCommitTs = 0;
};

class Logging {
public:
  /// The minimum flushed GSN among all worker threads. Transactions whose max
  /// observed GSN not larger than sMinFlushedGsn can be committed safely.
  static atomic<u64> sMinFlushedGsn;

  /// The maximum flushed GSN among all worker threads in each group commit
  /// round. It is updated by the group commit thread and used to update the GCN
  /// counter of the current worker thread to prevent GSN from skewing and
  /// undermining RFA.
  static atomic<u64> sMaxFlushedGsn;

  static atomic<u64> sMinFlushedCommitTs;

public:
  static void UpdateMinFlushedGsn(LID minGsn) {
    assert(sMinFlushedGsn.load() <= minGsn);
    sMinFlushedGsn.store(minGsn, std::memory_order_release);
  }

  static void UpdateMaxFlushedGsn(LID maxGsn) {
    sMaxFlushedGsn.store(maxGsn, std::memory_order_release);
  }

  static void UpdateMinFlushedCommitTs(TXID minCommitTs) {
    sMinFlushedCommitTs.store(minCommitTs, std::memory_order_release);
  }

  static bool NeedIncrementFlushesCounter(const Transaction& tx) {
    return tx.mMaxObservedGSN > sMinFlushedGsn ||
           tx.mStartTs > sMinFlushedCommitTs;
  }

public:
  LID mPrevLSN;

  WALEntrySimple* mActiveWALEntrySimple;

  WALEntryComplex* mActiveWALEntryComplex;

  // Shared between Group Committer and Worker
  std::mutex mPreCommittedQueueMutex;

  std::vector<Transaction> mPreCommittedQueue;

  std::vector<Transaction> mPreCommittedQueueRfa;

  /// Represents the commit TS of the last transaction in the current worker,
  /// whose wal records are all flushed by the group commit thread. Advanced by
  /// the group commit thread.
  ///
  /// Updated by group committer
  std::atomic<TXID> mFlushedCommitTs = 0;

  /// Tepresents the GSN of the transaction whose wal records are all flushed by
  /// the group commit thread. It is advanced by the group commit thread after a
  /// group commit round.
  ///
  /// Updated by group committer
  std::atomic<TXID> mFlushedGsn = 0;

  /// Represents the maximum commit timestamp in the worker. Transactions in the
  /// worker are committed if their commit timestamps are smaller than it.
  ///
  /// Updated by group committer
  std::atomic<TXID> mSignaledCommitTs = 0;

  utils::OptimisticSpinStruct<WalFlushReq> mWalFlushReq;

  // RFA: check for user tx dependency on tuple insert, update, lookup.
  // Deletes are treated as system transaction
  std::vector<std::tuple<WORKERID, TXID>> mRfaChecksAtPreCommit;

  /// The ring buffer of the current worker thread. All the wal entries of the
  /// current worker are writtern to this ring buffer firstly, then flushed to
  /// disk by the group commit thread.
  alignas(512) u8* mWalBuffer;

  /// Used to track the write order of wal entries.
  LID mLsnClock = 0;

  /// Used to track transaction dependencies.
  LID mGsnClock;

  /// The written offset of the wal ring buffer.
  u64 mWalBuffered = 0;

  /// Represents the flushed offset in the wal ring buffer.  The wal ring buffer
  /// is firstly written by the worker thread then flushed to disk file by the
  /// group commit thread.
  atomic<u64> mWalFlushed = 0;

  // for current transaction, reset on every transaction start
  LID mMinFlushedGsn;

  bool mHasRemoteDependency = false;

  u64 mTxWalBegin;

public:
  //---------------------------------------------------------------------------
  // Object utils
  //---------------------------------------------------------------------------

  void UpdateFlushedCommitTs(const TXID flushedCommitTs) {
    mFlushedCommitTs.store(flushedCommitTs, std::memory_order_release);
  }

  void UpdateFlushedGsn(const LID flushedGsn) {
    mFlushedGsn.store(flushedGsn, std::memory_order_release);
  }

  void UpdateSignaledCommitTs(const LID signaledCommitTs) {
    mSignaledCommitTs.store(signaledCommitTs, std::memory_order_release);
  }

  bool TxUnCommitted(const TXID dependency) {
    return mSignaledCommitTs.load() < dependency;
  }

  size_t PreCommittedQueueRfaSize() {
    std::unique_lock<std::mutex> guard(mPreCommittedQueueMutex);
    return mPreCommittedQueueRfa.size();
  }

  void checkLogDepdency(WORKERID otherWorkerId, TXID otherTxId);

  void publishOffset() {
    mWalFlushReq.updateAttribute(&WalFlushReq::mWalBuffered, mWalBuffered);
  }

  void UpdateWalFlushReq() {
    auto current = mWalFlushReq.getNoSync();
    current.mWalBuffered = mWalBuffered;
    current.mCurrGSN = GetCurrentGsn();
    mWalFlushReq.SetSync(current);
  }

  u32 walFreeSpace();
  u32 walContiguousFreeSpace();
  void walEnsureEnoughSpace(u32 requested_size);

  // Iterate over current TX entries
  void iterateOverCurrentTXEntries(
      std::function<void(const WALEntry& entry)> callback);

  WALEntrySimple& ReserveWALEntrySimple(WALEntry::TYPE type);

  void SubmitWALEntrySimple();

  template <typename T, typename... Args>
  WALPayloadHandler<T> ReserveWALEntryComplex(u64 payloadSize, PID pageId,
                                              LID gsn, TREEID treeId,
                                              Args&&... args);

  void SubmitWALEntryComplex(u64 totalSize);

  inline LID GetCurrentGsn() {
    return mGsnClock;
  }

  inline void SetCurrentGsn(LID gsn) {
    mGsnClock = gsn;
  }

  Logging& other(WORKERID otherWorkerId);
};

// LWM: start timestamp of the transaction that has its effect visible by all
// in its class
class ConcurrencyControl {
public:
  // Commmit Tree (single-writer multiple-reader)
  struct CommitTree {
    u64 capacity;
    std::pair<TXID, TXID>* array;
    std::shared_mutex mutex;
    u64 cursor = 0;
    void cleanIfNecessary();
    TXID commit(TXID start_ts);
    std::optional<std::pair<TXID, TXID>> LCBUnsafe(TXID start_ts);
    TXID LCB(TXID start_ts);
    CommitTree(const u64 numWorkers) : capacity(numWorkers + 1) {
      array = new std::pair<TXID, TXID>[capacity];
    }
  };

  enum class VISIBILITY : u8 {
    VISIBLE_ALREADY,
    VISIBLE_NEXT_ROUND,
    UNDETERMINED
  };

public:
  //-------------------------------------------------------------------------
  // Constructor and Destructors
  //-------------------------------------------------------------------------
  ConcurrencyControl(HistoryTreeInterface& ht, const u64 numWorkers)
      : mHistoryTree(ht), commit_tree(numWorkers) {
  }

public:
  //-------------------------------------------------------------------------
  // Static members
  //-------------------------------------------------------------------------
  static atomic<u64> sGlobalClock;

public:
  //-------------------------------------------------------------------------
  // Object members
  //-------------------------------------------------------------------------
  atomic<TXID> local_lwm_latch = 0;
  atomic<TXID> oltp_lwm_receiver;
  atomic<TXID> all_lwm_receiver;
  atomic<TXID> mLatestWriteTx = 0;
  atomic<TXID> mLatestLwm4Tx = 0;
  TXID local_all_lwm;
  TXID local_oltp_lwm;
  TXID local_global_all_lwm_cache = 0;
  unique_ptr<TXID[]> mLocalSnapshotCache; // = Readview
  unique_ptr<TXID[]> local_snapshot_cache_ts;
  unique_ptr<TXID[]> local_workers_start_ts;

  // LeanStore NoSteal, Nothing for now
  HistoryTreeInterface& mHistoryTree;

  CommitTree commit_tree;

  // Clean up state
  u64 cleaned_untill_oltp_lwm = 0;

public:
  //-------------------------------------------------------------------------
  // Object utils
  //-------------------------------------------------------------------------
  void garbageCollection();
  void refreshGlobalState();
  void switchToReadCommittedMode();
  void switchToSnapshotIsolationMode();

  bool isVisibleForAll(WORKERID workerId, TXID start_ts);

  bool isVisibleForMe(WORKERID workerId, u64 tts, bool to_write = true);

  VISIBILITY isVisibleForIt(WORKERID whomWorkerId, WORKERID whatWorkerId,
                            u64 tts);

  VISIBILITY isVisibleForIt(WORKERID whomWorkerId, TXID commitTs);

  TXID getCommitTimestamp(WORKERID workerId, TXID startTs);

  ConcurrencyControl& other(WORKERID otherWorkerId);

  u64 insertVersion(TREEID treeId, bool is_remove, u64 payload_length,
                    std::function<void(u8*)> cb);

  inline bool retrieveVersion(
      WORKERID its_worker_id, TXID its_tx_id, COMMANDID its_command_id,
      std::function<void(const u8*, u64 payload_length)> cb) {
    utils::Timer timer(CRCounters::myCounters().cc_ms_history_tree_retrieve);
    const bool is_remove = its_command_id & TYPE_MSB(COMMANDID);
    const bool found = mHistoryTree.retrieveVersion(
        its_worker_id, its_tx_id, its_command_id, is_remove, cb);
    return found;
  }
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
  Logging mLogging;
  ConcurrencyControl cc;
  u64 command_id = 0;
  Transaction mActiveTx;

  const u64 mWorkerId;
  std::vector<Worker*>& mAllWorkers;
  const u64 mNumAllWorkers;

public:
  Worker(u64 workerId, std::vector<Worker*>& allWorkers, u64 numWorkers,
         HistoryTreeInterface& historyTree);

  ~Worker();

public:
  bool IsTxStarted() {
    return mActiveTx.state == TX_STATE::STARTED;
  }

  void startTX(
      TX_MODE mode = TX_MODE::OLTP,
      TX_ISOLATION_LEVEL level = TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION,
      bool isReadOnly = false);

  void commitTX();

  void abortTX();

  void shutdown();

public:
  static thread_local std::unique_ptr<Worker> sTlsWorker;

  // Concurrency Control
  static std::unique_ptr<atomic<u64>[]> sWorkersCurrentSnapshot;
  static std::atomic<TXID> sOldestOltpStartTx;
  static std::atomic<TXID> sOltpLwm;
  static std::atomic<TXID> sOldestAllStartTs;
  static std::atomic<TXID> sAllLwm;
  static std::atomic<TXID> sNewestOlapStartTx;
  static std::shared_mutex sGlobalMutex;

  static constexpr u64 WORKERS_BITS = 8;
  static constexpr u64 WORKERS_INCREMENT = 1ull << WORKERS_BITS;
  static constexpr u64 WORKERS_MASK = (1ull << WORKERS_BITS) - 1;
  static constexpr u64 LATCH_BIT = (1ull << 63);
  static constexpr u64 RC_BIT = (1ull << 62);
  static constexpr u64 OLAP_BIT = (1ull << 61);
  static constexpr u64 OLTP_OLAP_SAME_BIT = OLAP_BIT;
  static constexpr u64 CLEAN_BITS_MASK = ~(LATCH_BIT | OLAP_BIT | RC_BIT);

  // TXID: [LATCH_BIT | RC_BIT | OLAP_BIT           | id];
  // LWM:  [LATCH_BIT | RC_BIT | OLTP_OLAP_SAME_BIT | id];
  static constexpr s64 CR_ENTRY_SIZE = sizeof(WALEntrySimple);

public:
  static inline Worker& my() {
    return *Worker::sTlsWorker.get();
  }
};

// Shortcuts
inline Transaction& activeTX() {
  return cr::Worker::my().mActiveTx;
}

//------------------------------------------------------------------------------
// WALPayloadHandler
//------------------------------------------------------------------------------

template <typename T> inline void WALPayloadHandler<T>::SubmitWal() {
  DEBUG_BLOCK() {
    auto dtEntryPtr = cr::Worker::my().mLogging.mWalBuffer +
                      cr::Worker::my().mLogging.mWalBuffered;
    auto dtEntry = reinterpret_cast<WALEntryComplex*>(dtEntryPtr);
    auto dtDoc = dtEntry->ToJSON();

    auto entryPtr = dtEntryPtr + sizeof(WALEntryComplex);
    auto entry = reinterpret_cast<T*>(entryPtr);
    auto doc = entry->ToJSON();

    dtDoc->AddMember("payload", *doc, dtDoc->GetAllocator());

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    dtDoc->Accept(writer);

    DLOG(INFO) << "SubmitWal: " << buffer.GetString();
  }

  cr::Worker::my().mLogging.SubmitWALEntryComplex(mTotalSize);
}

//------------------------------------------------------------------------------
// Logging
//------------------------------------------------------------------------------

inline void Logging::checkLogDepdency(WORKERID otherWorkerId, TXID otherTxId) {
  if (FLAGS_recover) {
    return;
  }

  if (!mHasRemoteDependency && Worker::my().mWorkerId != otherWorkerId) {
    if (other(otherWorkerId).TxUnCommitted(otherTxId)) {
      mRfaChecksAtPreCommit.push_back({otherWorkerId, otherTxId});
    }
  }
}

inline Logging& Logging::other(WORKERID otherWorkerId) {
  return Worker::my().mAllWorkers[otherWorkerId]->mLogging;
}

template <typename T, typename... Args>
inline WALPayloadHandler<T> Logging::ReserveWALEntryComplex(u64 payloadSize,
                                                            PID pageId, LID gsn,
                                                            TREEID treeId,
                                                            Args&&... args) {
  SCOPED_DEFER(mPrevLSN = mActiveWALEntryComplex->lsn);

  auto entryLSN = mLsnClock++;
  auto entryPtr = mWalBuffer + mWalBuffered;
  auto entrySize = sizeof(WALEntryComplex) + payloadSize;
  DCHECK(walContiguousFreeSpace() >= entrySize);

  mActiveWALEntryComplex =
      new (entryPtr) WALEntryComplex(entryLSN, entrySize, gsn, treeId, pageId);
  mActiveWALEntryComplex->mPrevLSN = mPrevLSN;
  mActiveWALEntryComplex->mTxId =
      leanstore::cr::Worker::my().mActiveTx.mStartTs;

  auto payloadPtr = mActiveWALEntryComplex->payload;
  auto walPayload = new (payloadPtr) T(std::forward<Args>(args)...);
  return {walPayload, entrySize, entryLSN};
}

//------------------------------------------------------------------------------
// ConcurrencyControl
//------------------------------------------------------------------------------

inline ConcurrencyControl& ConcurrencyControl::other(WORKERID otherWorkerId) {
  return Worker::my().mAllWorkers[otherWorkerId]->cc;
}

inline u64 ConcurrencyControl::insertVersion(TREEID treeId, bool is_remove,
                                             u64 payload_length,
                                             std::function<void(u8*)> cb) {
  utils::Timer timer(CRCounters::myCounters().cc_ms_history_tree_insert);
  auto& curWorker = Worker::my();
  const u64 new_command_id =
      (curWorker.command_id++) | ((is_remove) ? TYPE_MSB(COMMANDID) : 0);
  mHistoryTree.insertVersion(curWorker.mWorkerId, curWorker.mActiveTx.startTS(),
                             new_command_id, treeId, is_remove, payload_length,
                             cb);
  return new_command_id;
}

} // namespace cr
} // namespace leanstore
