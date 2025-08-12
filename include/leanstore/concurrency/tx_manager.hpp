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

class TxManager {
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

  /// Last committed system transaction ID in the worker.
  std::atomic<TXID> last_committed_sys_tx_ = 0;

  /// Last committed user transaction ID in the worker.
  std::atomic<TXID> last_committed_usr_tx_ = 0;

  /// The ID of the current transaction. It's set by the current worker thread and read by the
  /// garbage collection process to determine the lower watermarks of the transactions.
  std::atomic<TXID> active_tx_id_ = 0;

  /// ID of the current worker itself.
  const uint64_t worker_id_;

  /// All the workers.
  std::vector<std::unique_ptr<TxManager>>& tx_mgrs_;

  /// Construct a TxManager.
  TxManager(uint64_t worker_id, std::vector<std::unique_ptr<TxManager>>& tx_mgrs,
            leanstore::LeanStore* store);

  /// Destruct a TxManager.
  ~TxManager();

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

  TXID GetLastCommittedSysTx() const {
    return last_committed_sys_tx_.load(std::memory_order_acquire);
  }

  TXID GetLastCommittedUsrTx() const {
    return last_committed_usr_tx_.load(std::memory_order_acquire);
  }

  void UpdateLastCommittedSysTx(TXID sys_tx_id) {
    last_committed_sys_tx_.store(sys_tx_id, std::memory_order_release);
  }

  void UpdateLastCommittedUsrTx(TXID usr_tx_id) {
    last_committed_usr_tx_.store(usr_tx_id, std::memory_order_release);
  }

  /// Raw pointer to avoid the overhead of std::unique_ptr.
  static thread_local TxManager* s_tls_tx_manager;

  static constexpr uint64_t kRcBit = (1ull << 63);
  static constexpr uint64_t kLongRunningBit = (1ull << 62);
  static constexpr uint64_t kCleanBitsMask = ~(kRcBit | kLongRunningBit);

  static TxManager& My() {
    return *TxManager::s_tls_tx_manager;
  }

  static bool InWorker() {
    return TxManager::s_tls_tx_manager != nullptr;
  }
};

// Shortcuts
inline Transaction& ActiveTx() {
  return cr::TxManager::My().active_tx_;
}

} // namespace leanstore::cr
