#pragma once

#include "leanstore-c/perf_counters.h"
#include "leanstore/concurrency/concurrency_control.hpp"
#include "leanstore/concurrency/logging.hpp"
#include "leanstore/concurrency/transaction.hpp"
#include "leanstore/units.hpp"

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
  /// The store it belongs to.
  leanstore::LeanStore* store_ = nullptr;

  /// The write-ahead logging component.
  Logging logging_;

  /// The concurrent control component.
  ConcurrencyControl cc_;

  /// The ID of the current command in the current transaction.
  COMMANDID command_id_ = 0;

  /// The current running transaction.
  Transaction active_tx_;

  /// The ID of the current transaction. It's set by the current worker thread and read by the
  /// garbage collection process to determine the lower watermarks of the transactions.
  std::atomic<TXID> active_tx_id_ = 0;

  /// ID of the current worker itself.
  const uint64_t worker_id_;

  /// All the workers.
  std::vector<WorkerContext*>& all_workers_;

  /// Construct a WorkerContext.
  WorkerContext(uint64_t worker_id, std::vector<WorkerContext*>& all_workers,
                leanstore::LeanStore* store);

  /// Destruct a WorkerContext.
  ~WorkerContext();

  /// Whether a user transaction is started.
  bool IsTxStarted() {
    return active_tx_.state_ == TxState::kStarted;
  }

  /// Starts a user transaction.
  void StartTx(TxMode mode = TxMode::kShortRunning,
               IsolationLevel level = IsolationLevel::kSnapshotIsolation,
               bool is_read_only = false);

  /// Commits a user transaction.
  void CommitTx();

  /// Aborts a user transaction.
  void AbortTx();

  /// Get the PerfCounters of the current worker.
  PerfCounters* GetPerfCounters();

  Logging& GetLogging() {
    return logging_;
  }

  /// thread-local storage for WorkerContext.
  static thread_local std::unique_ptr<WorkerContext> s_tls_worker_ctx;

  /// Raw pointer to s_tls_worker_ctx to avoid the overhead of std::unique_ptr.
  static thread_local WorkerContext* s_tls_worker_ctx_ptr;

  static constexpr uint64_t kRcBit = (1ull << 63);
  static constexpr uint64_t kLongRunningBit = (1ull << 62);
  static constexpr uint64_t kCleanBitsMask = ~(kRcBit | kLongRunningBit);

  static WorkerContext& My() {
    return *WorkerContext::s_tls_worker_ctx_ptr;
  }

  static bool InWorker() {
    return WorkerContext::s_tls_worker_ctx_ptr != nullptr;
  }
};

// Shortcuts
inline Transaction& ActiveTx() {
  return cr::WorkerContext::My().active_tx_;
}

} // namespace leanstore::cr
