#pragma once

#include "leanstore/common/perf_counters.h"
#include "leanstore/common/types.h"
#include "leanstore/concurrency/concurrency_control.hpp"
#include "leanstore/concurrency/transaction.hpp"
#include "utils/coroutine/coro_env.hpp"
#include "utils/coroutine/coroutine.hpp"

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

  /// The concurrent control component.
  ConcurrencyControl cc_;

  /// The ID of the current command in the current transaction.
  lean_cmdid_t cmd_id_ = 0;

  /// The current running transaction.
  Transaction active_tx_;

  /// Protects tx_to_commit_
  LeanMutex tx_to_commit_mutex_;

  /// The queue for each worker thread to store pending-to-commit transactions which have remote
  /// dependencies.
  std::vector<Transaction> tx_to_commit_;

  /// Protects rfa_tx_to_commit_
  LeanMutex rfa_tx_to_commit_mutex_;

  /// The queue for each worker thread to store pending-to-commit transactions which doesn't have
  /// any remote dependencies.
  std::vector<Transaction> rfa_tx_to_commit_;

  /// Last committed user transaction ID in the worker.
  std::atomic<lean_txid_t> last_committed_usr_tx_ = 0;

  /// The ID of the current transaction. It's set by the current worker thread and read by the
  /// garbage collection process to determine the lower watermarks of the transactions.
  std::atomic<lean_txid_t> active_tx_id_ = 0;

  /// ID of the current worker itself.
  const uint64_t worker_id_;

  /// All the workers.
  std::vector<std::unique_ptr<TxManager>>& tx_mgrs_;

  /// Construct a TxManager.
  TxManager(uint64_t worker_id, std::vector<std::unique_ptr<TxManager>>& tx_mgrs,
            leanstore::LeanStore* store);

  /// Destruct a TxManager.
  ~TxManager();

  Transaction& ActiveTx() {
    return active_tx_;
  }

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

  /// Get the lean_perf_counters of the current worker.
  lean_perf_counters* GetPerfCounters();

  lean_txid_t GetLastCommittedUsrTx() const {
    return last_committed_usr_tx_.load(std::memory_order_acquire);
  }

  void UpdateLastCommittedUsrTx(lean_txid_t usr_tx_id) {
    last_committed_usr_tx_.store(usr_tx_id, std::memory_order_release);
  }

  static constexpr uint64_t kRcBit = (1ull << 63);
  static constexpr uint64_t kLongRunningBit = (1ull << 62);
  static constexpr uint64_t kCleanBitsMask = ~(kRcBit | kLongRunningBit);

private:
  void WaitToCommit(const lean_txid_t commit_ts) {
    while (!(commit_ts <= GetLastCommittedUsrTx())) {
#ifdef ENABLE_COROUTINE
      CoroEnv::CurCoro()->Yield(CoroState::kRunning);
#endif
    }
  }
};

} // namespace leanstore::cr
