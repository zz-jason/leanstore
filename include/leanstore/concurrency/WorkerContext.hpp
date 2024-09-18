#pragma once

#include "leanstore-c/PerfCounters.h"
#include "leanstore/Units.hpp"
#include "leanstore/concurrency/ConcurrencyControl.hpp"
#include "leanstore/concurrency/Logging.hpp"
#include "leanstore/concurrency/Transaction.hpp"

#include <atomic>
#include <memory>
#include <vector>

namespace leanstore {

class LeanStore;

} // namespace leanstore

namespace leanstore::cr {

class Logging;
class ConcurrencyControl;

class WorkerContext {
public:
  //! The store it belongs to.
  leanstore::LeanStore* mStore = nullptr;

  //! The write-ahead logging component.
  Logging mLogging;

  //! The concurrent control component.
  ConcurrencyControl mCc;

  //! The ID of the current command in the current transaction.
  COMMANDID mCommandId = 0;

  //! The current running transaction.
  Transaction mActiveTx;

  //! The ID of the current transaction. It's set by the current worker thread and read by the
  //! garbage collection process to determine the lower watermarks of the transactions.
  std::atomic<TXID> mActiveTxId = 0;

  //! ID of the current worker itself.
  const uint64_t mWorkerId;

  //! All the workers.
  std::vector<WorkerContext*>& mAllWorkers;

  //! Construct a WorkerContext.
  WorkerContext(uint64_t workerId, std::vector<WorkerContext*>& allWorkers,
                leanstore::LeanStore* store);

  //! Destruct a WorkerContext.
  ~WorkerContext() = default;

  //! Whether a user transaction is started.
  bool IsTxStarted() {
    return mActiveTx.mState == TxState::kStarted;
  }

  //! Starts a user transaction.
  void StartTx(TxMode mode = TxMode::kShortRunning,
               IsolationLevel level = IsolationLevel::kSnapshotIsolation, bool isReadOnly = false);

  //! Commits a user transaction.
  void CommitTx();

  //! Aborts a user transaction.
  void AbortTx();

  //! Starts a system transaction.
  TXID StartSysTx() {
    auto sysTx = mStore->AllocSysTxTs();
    mLogging.mActiveSysTx.store(sysTx);
    return sysTx;
  }

  //! Commits a system transaction.
  void CommitSysTx() {
    // 1. Write all the buffered write-ahead logs
    mLogging.mWalBuffer.Persist();

    // 2. Update the committed system transaction ID in the end
    mLogging.mActiveSysTx.store(0);
  }

  //! Get the PerfCounters of the current worker.
  PerfCounters* GetPerfCounters();

public:
  //! Thread-local storage for WorkerContext.
  static thread_local std::unique_ptr<WorkerContext> sTlsWorkerCtx;

  //! Raw pointer to sTlsWorkerCtx to avoid the overhead of std::unique_ptr.
  static thread_local WorkerContext* sTlsWorkerCtxRaw;

  static constexpr uint64_t kRcBit = (1ull << 63);
  static constexpr uint64_t kLongRunningBit = (1ull << 62);
  static constexpr uint64_t kCleanBitsMask = ~(kRcBit | kLongRunningBit);

  static WorkerContext& My() {
    return *WorkerContext::sTlsWorkerCtxRaw;
  }

  static bool InWorker() {
    return WorkerContext::sTlsWorkerCtxRaw != nullptr;
  }

private:
  void waitDependencyToCommit() {
    while (mActiveTx.mDependentSysTx > Logging::sGlobalMinCommittedSysTx.load()) {
      TXID curSysTso = mStore->GetSysTxTs();

      // collect min committed system transaction ID
      TXID minActiveSysTx = std::numeric_limits<TXID>::max();
      for (auto& workerCtx : mAllWorkers) {
        auto activeSysTx = workerCtx->mLogging.mActiveSysTx.load();
        if (activeSysTx < minActiveSysTx && activeSysTx != 0) {
          minActiveSysTx = activeSysTx;
        }
      }

      if (minActiveSysTx != std::numeric_limits<TXID>::max()) {
        Logging::sGlobalMinCommittedSysTx.store(minActiveSysTx - 1);
      } else {
        Logging::sGlobalMinCommittedSysTx.store(curSysTso);
      }
    }
  }
};

inline Transaction& ActiveTx() {
  return cr::WorkerContext::My().mActiveTx;
}

} // namespace leanstore::cr
