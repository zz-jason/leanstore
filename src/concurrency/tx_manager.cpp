#include "leanstore/concurrency/tx_manager.hpp"

#include "leanstore-c/perf_counters.h"
#include "leanstore/buffer-manager/tree_registry.hpp"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/concurrency/group_committer.hpp"
#include "leanstore/concurrency/logging.hpp"
#include "leanstore/concurrency/transaction.hpp"
#include "leanstore/concurrency/wal_entry.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/counter_util.hpp"
#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/log.hpp"
#include "utils/coroutine/coro_env.hpp"
#include "utils/coroutine/lean_mutex.hpp"
#include "utils/coroutine/mvcc_manager.hpp"

#include <algorithm>
#include <cstdlib>

namespace leanstore::cr {

thread_local PerfCounters tls_perf_counters;

TxManager::TxManager(uint64_t worker_id, std::vector<std::unique_ptr<TxManager>>& tx_mgrs,
                     leanstore::LeanStore* store)
    : store_(store),
      cc_(store, store->store_option_->worker_threads_),
      active_tx_id_(0),
      worker_id_(worker_id),
      tx_mgrs_(tx_mgrs) {
}

TxManager::~TxManager() = default;

void TxManager::StartTx(TxMode mode, IsolationLevel level, bool is_read_only) {
  Transaction prev_tx [[maybe_unused]] = active_tx_;
  LEAN_DCHECK(prev_tx.state_ != TxState::kStarted,
              "Previous transaction not ended, workerId={}, startTs={}, txState={}", worker_id_,
              prev_tx.start_ts_, TxStatUtil::ToString(prev_tx.state_));
  SCOPED_DEFER({
    LEAN_DLOG("Start transaction, workerId={}, startTs={}, min_global_committed_sys_tx={}",
              worker_id_, active_tx_.start_ts_, store_->MvccManager()->GetMinCommittedSysTx());
  });

  active_tx_.Start(mode, level);

  if (!active_tx_.is_durable_) {
    return;
  }

  /// Reset the max observed system transaction id
  active_tx_.max_observed_sys_tx_id_ = store_->MvccManager()->GetMinCommittedSysTx();

  // Init wal and group commit related transaction information
  CoroEnv::CurLogging().tx_wal_begin_ = CoroEnv::CurLogging().wal_buffered_;

  // For now, we only support SI and SSI
  if (level < IsolationLevel::kSnapshotIsolation) {
    Log::Fatal("Unsupported isolation level: {}", static_cast<uint64_t>(level));
  }

  // Draw TXID from global counter and publish it with the TX type (i.e.  long-running or
  // short-running) We have to acquire a transaction id and use it for locking in ANY isolation
  // level
  if (is_read_only) {
    active_tx_.start_ts_ = store_->MvccManager()->GetUsrTxTs();
  } else {
    active_tx_.start_ts_ = store_->MvccManager()->AllocUsrTxTs();
  }
  auto cur_tx_id = active_tx_.start_ts_;
  if (store_->store_option_->enable_long_running_tx_ && active_tx_.IsLongRunning()) {
    // Mark as long-running transaction
    cur_tx_id |= kLongRunningBit;
  }

  // Publish the transaction id
  active_tx_id_.store(cur_tx_id, std::memory_order_release);
  cc_.global_wmk_of_all_tx_ = store_->MvccManager()->GlobalWmkInfo().wmk_of_all_tx_.load();

  // Cleanup commit log if necessary
  cc_.commit_tree_.CompactCommitLog();
}

void TxManager::CommitTx() {
  SCOPED_DEFER({
    COUNTER_INC(&tls_perf_counters.tx_committed_);
    if (active_tx_.has_remote_dependency_) {
      COUNTER_INC(&tls_perf_counters.tx_with_remote_dependencies_);
    } else {
      COUNTER_INC(&tls_perf_counters.tx_without_remote_dependencies_);
    }
    active_tx_.state_ = TxState::kCommitted;
  });

  if (!active_tx_.is_durable_) {
    return;
  }

  // Reset command_id_ on commit
  command_id_ = 0;
  if (active_tx_.has_wrote_) {
    active_tx_.commit_ts_ = store_->MvccManager()->AllocUsrTxTs();
    cc_.commit_tree_.AppendCommitLog(active_tx_.start_ts_, active_tx_.commit_ts_);
    cc_.latest_commit_ts_.store(active_tx_.commit_ts_, std::memory_order_release);
  } else {
    LEAN_DLOG("Transaction has no writes, skip assigning commitTs, append log to "
              "commit tree, and group commit, workerId={}, actual startTs={}",
              worker_id_, active_tx_.start_ts_);
  }

  // Reset startTs so that other transactions can safely update the global
  // transaction watermarks and garbage collect the unused versions.
  active_tx_id_.store(0, std::memory_order_release);

  if (!active_tx_.has_wrote_) {
    return;
  }

  if (active_tx_.is_durable_) {
    CoroEnv::CurLogging().WriteWalTxFinish();
  }

  // for group commit
  if (active_tx_.has_remote_dependency_) {
    LEAN_UNIQUE_LOCK(CoroEnv::CurLogging().tx_to_commit_mutex_);
    CoroEnv::CurLogging().tx_to_commit_.push_back(active_tx_);
  } else {
    LEAN_UNIQUE_LOCK(CoroEnv::CurLogging().rfa_tx_to_commit_mutex_);
    CoroEnv::CurLogging().rfa_tx_to_commit_.push_back(active_tx_);
  }

  // Cleanup versions in history tree
  cc_.GarbageCollection();

  // Wait logs to be flushed
  LEAN_DLOG("Wait transaction to commit, workerId={}, startTs={}, commitTs={}, maxObseredSysTx={}, "
            "hasRemoteDep={}",
            worker_id_, active_tx_.start_ts_, active_tx_.commit_ts_,
            active_tx_.max_observed_sys_tx_id_, active_tx_.has_remote_dependency_);

  CoroEnv::CurLogging().WaitToCommit(active_tx_.commit_ts_);
}

/// TODO(jian.z): revert changes made in-place on the btree process of a transaction abort:
///
/// 1. Read previous wal entries
/// 2. Undo the changes via btree operations
/// 3. Write compensation wal entries during the undo process
/// 4. Purge versions in history tree, clean garbages made by the aborted transaction
///
/// It may share the same code with the recovery process?
void TxManager::AbortTx() {
  SCOPED_DEFER({
    active_tx_.state_ = TxState::kAborted;
    COUNTER_INC(&tls_perf_counters.tx_aborted_);
    if (active_tx_.has_remote_dependency_) {
      COUNTER_INC(&tls_perf_counters.tx_with_remote_dependencies_);
    } else {
      COUNTER_INC(&tls_perf_counters.tx_without_remote_dependencies_);
    }
    active_tx_id_.store(0, std::memory_order_release);
    Log::Info("Transaction aborted, workerId={}, startTs={}, commitTs={}, maxObservedSysTx={}",
              worker_id_, active_tx_.start_ts_, active_tx_.commit_ts_,
              active_tx_.max_observed_sys_tx_id_);
  });

  if (!(active_tx_.state_ == TxState::kStarted && active_tx_.is_durable_)) {
    return;
  }

  // TODO(jian.z): support reading from WAL file once
  LEAN_DCHECK(!active_tx_.wal_exceed_buffer_, "Aborting from WAL file is not supported yet");
  std::vector<const WalEntry*> entries;
  CoroEnv::CurLogging().IterateCurrentTxWALs([&](const WalEntry& entry) {
    if (entry.type_ == WalEntry::Type::kComplex) {
      entries.push_back(&entry);
    }
  });

  const uint64_t tx_id = active_tx_.start_ts_;
  std::for_each(entries.rbegin(), entries.rend(), [&](const WalEntry* entry) {
    const auto& complex_entry = *reinterpret_cast<const WalEntryComplex*>(entry);
    store_->tree_registry_->Undo(complex_entry.tree_id_, complex_entry.payload_, tx_id);
  });

  cc_.history_storage_.PurgeVersions(
      active_tx_.start_ts_, active_tx_.start_ts_,
      [&](const TXID, const TREEID, const uint8_t*, uint64_t, const bool) {}, 0);

  if (active_tx_.has_wrote_ && active_tx_.is_durable_) {
    // TODO: write compensation wal records between abort and finish
    CoroEnv::CurLogging().WriteWalTxAbort();
    CoroEnv::CurLogging().WriteWalTxFinish();
  }
}

PerfCounters* TxManager::GetPerfCounters() {
  return &tls_perf_counters;
}

} // namespace leanstore::cr
