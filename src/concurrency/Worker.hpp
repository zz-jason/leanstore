#pragma once

#include "concurrency/ConcurrencyControl.hpp"
#include "concurrency/Logging.hpp"
#include "concurrency/Transaction.hpp"
#include "leanstore/Units.hpp"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <atomic>
#include <memory>
#include <vector>

namespace leanstore {

class LeanStore;

} // namespace leanstore

namespace leanstore::cr {

class Logging;
class ConcurrencyControl;

class Worker {
public:
  /// The store it belongs to.
  leanstore::LeanStore* mStore = nullptr;

  /// The write-ahead logging component.
  Logging mLogging;

  ConcurrencyControl mCc;

  /// The ID of the current command in the current transaction.
  COMMANDID mCommandId = 0;

  /// The current running transaction.
  Transaction mActiveTx;

  /// The ID of the current transaction. It's set by the current worker thread
  /// and read by the garbage collection process to determine the lower
  /// watermarks of the transactions.
  std::atomic<TXID> mActiveTxId = 0;

  /// ID of the current worker itself.
  const uint64_t mWorkerId;

  /// All the workers.
  std::vector<Worker*>& mAllWorkers;

public:
  Worker(uint64_t workerId, std::vector<Worker*>& allWorkers,
         leanstore::LeanStore* store);

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
  static thread_local Worker* sTlsWorkerRaw;

  static constexpr uint64_t kRcBit = (1ull << 63);
  static constexpr uint64_t kLongRunningBit = (1ull << 62);
  static constexpr uint64_t kCleanBitsMask = ~(kRcBit | kLongRunningBit);

public:
  static Worker& My() {
    return *Worker::sTlsWorkerRaw;
  }

  static bool InWorker() {
    return Worker::sTlsWorkerRaw != nullptr;
  }
};

// Shortcuts
inline Transaction& ActiveTx() {
  return cr::Worker::My().mActiveTx;
}

} // namespace leanstore::cr
