#pragma once

#include "ConcurrencyControl.hpp"
#include "Transaction.hpp"
#include "WALEntry.hpp"
#include "shared-headers/Exceptions.hpp"
#include "shared-headers/Units.hpp"
#include "sync-primitives/OptimisticGuarded.hpp"
#include "utils/Defer.hpp"

#include <glog/logging.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

namespace leanstore {

class LeanStore;

} // namespace leanstore

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

  storage::OptimisticGuarded<WalFlushReq> mWalFlushReq;

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

  void ReserveContiguousBuffer(u32 requestedSize);

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

  u32 walContiguousFreeSpace();
};

class Worker {
public:
  /// The write-ahead logging component.
  Logging mLogging;

  ConcurrencyControl cc;

  COMMANDID mCommandId = 0;

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

public:
  static thread_local std::unique_ptr<Worker> sTlsWorker;

  // Concurrency Control
  static std::unique_ptr<std::atomic<u64>[]> sActiveTxId;

  static std::shared_mutex sGlobalMutex;
  static std::atomic<TXID> sOldestActiveTx;
  static std::atomic<TXID> sOldestActiveShortTx;
  static std::atomic<TXID> sNetestActiveLongTx;
  static std::atomic<TXID> sWmkOfAllTx;
  static std::atomic<TXID> sWmkOfShortTx;

  static constexpr u64 kWorkersBits = 8;
  static constexpr u64 kRcBit = (1ull << 63);
  static constexpr u64 kLongRunningBit = (1ull << 62);
  static constexpr u64 kOltpOlapSameBit = kLongRunningBit;
  static constexpr u64 kCleanBitsMask = ~(kRcBit | kLongRunningBit);

public:
  inline static Worker& My() {
    return *Worker::sTlsWorker.get();
  }
};

// Shortcuts
inline Transaction& ActiveTx() {
  return cr::Worker::My().mActiveTx;
}

//------------------------------------------------------------------------------
// WALPayloadHandler
//------------------------------------------------------------------------------

template <typename T> inline void WALPayloadHandler<T>::SubmitWal() {
  SCOPED_DEFER(DEBUG_BLOCK() {
    auto walDoc = cr::Worker::My().mLogging.mActiveWALEntryComplex->ToJson();
    auto entry = reinterpret_cast<T*>(
        cr::Worker::My().mLogging.mActiveWALEntryComplex->payload);
    auto payloadDoc = entry->ToJson();
    walDoc->AddMember("payload", *payloadDoc, walDoc->GetAllocator());
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    walDoc->Accept(writer);
    LOG(INFO) << "SubmitWal"
              << ", workerId=" << Worker::My().mWorkerId
              << ", startTs=" << Worker::My().mActiveTx.mStartTs
              << ", curGSN=" << Worker::My().mLogging.GetCurrentGsn()
              << ", walJson=" << buffer.GetString();
  });

  cr::Worker::My().mLogging.SubmitWALEntryComplex(mTotalSize);
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
  ReserveContiguousBuffer(entrySize);

  mActiveWALEntryComplex =
      new (entryPtr) WALEntryComplex(entryLSN, entrySize, psn, treeId, pageId);
  mActiveWALEntryComplex->mPrevLSN = mPrevLSN;
  auto& curWorker = leanstore::cr::Worker::My();
  mActiveWALEntryComplex->InitTxInfo(&curWorker.mActiveTx, curWorker.mWorkerId);

  auto* payloadPtr = mActiveWALEntryComplex->payload;
  auto walPayload = new (payloadPtr) T(std::forward<Args>(args)...);
  return {walPayload, entrySize, entryLSN};
}

} // namespace leanstore::cr
