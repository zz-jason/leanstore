#include "leanstore/concurrency/worker_context.hpp"

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

#include <algorithm>
#include <cstdlib>
#include <mutex>

namespace leanstore::cr {

thread_local std::unique_ptr<WorkerContext> WorkerContext::sTlsWorkerCtx = nullptr;
thread_local WorkerContext* WorkerContext::sTlsWorkerCtxRaw = nullptr;
thread_local PerfCounters tls_perf_counters;

WorkerContext::WorkerContext(uint64_t worker_id, std::vector<WorkerContext*>& all_workers,
                             leanstore::LeanStore* store)
    : store_(store),
      cc_(store, all_workers.size()),
      active_tx_id_(0),
      worker_id_(worker_id),
      all_workers_(all_workers) {

  // init wal buffer
  logging_.wal_buffer_size_ = store_->store_option_->wal_buffer_size_;
  logging_.wal_buffer_ = (uint8_t*)(std::aligned_alloc(512, logging_.wal_buffer_size_));
  std::memset(logging_.wal_buffer_, 0, logging_.wal_buffer_size_);

  cc_.lcb_cache_val_ = std::make_unique<uint64_t[]>(all_workers_.size());
  cc_.lcb_cache_key_ = std::make_unique<uint64_t[]>(all_workers_.size());
}

WorkerContext::~WorkerContext() {
  free(logging_.wal_buffer_);
  logging_.wal_buffer_ = nullptr;
}

void WorkerContext::StartTx(TxMode mode, IsolationLevel level, bool is_read_only) {
  Transaction prev_tx [[maybe_unused]] = active_tx_;
  LS_DCHECK(prev_tx.state_ != TxState::kStarted,
            "Previous transaction not ended, workerId={}, startTs={}, txState={}", worker_id_,
            prev_tx.start_ts_, TxStatUtil::ToString(prev_tx.state_));
  SCOPED_DEFER({
    LS_DLOG("Start transaction, workerId={}, startTs={}, globalMinFlushedSysTx={}", worker_id_,
            active_tx_.start_ts_,
            store_->crmanager_->group_committer_->global_min_flushed_sys_tx_.load());
  });

  active_tx_.Start(mode, level);

  if (!active_tx_.is_durable_) {
    return;
  }

  /// Reset the max observed system transaction id
  active_tx_.max_observed_sys_tx_id_ =
      store_->crmanager_->group_committer_->global_min_flushed_sys_tx_;

  // Init wal and group commit related transaction information
  logging_.tx_wal_begin_ = logging_.wal_buffered_;

  // For now, we only support SI and SSI
  if (level < IsolationLevel::kSnapshotIsolation) {
    Log::Fatal("Unsupported isolation level: {}", static_cast<uint64_t>(level));
  }

  // Draw TXID from global counter and publish it with the TX type (i.e.  long-running or
  // short-running) We have to acquire a transaction id and use it for locking in ANY isolation
  // level
  if (is_read_only) {
    active_tx_.start_ts_ = store_->GetUsrTxTs();
  } else {
    active_tx_.start_ts_ = store_->AllocUsrTxTs();
  }
  auto cur_tx_id = active_tx_.start_ts_;
  if (store_->store_option_->enable_long_running_tx_ && active_tx_.IsLongRunning()) {
    // Mark as long-running transaction
    cur_tx_id |= kLongRunningBit;
  }

  // Publish the transaction id
  active_tx_id_.store(cur_tx_id, std::memory_order_release);
  cc_.global_wmk_of_all_tx_ = store_->crmanager_->global_wmk_info_.wmk_of_all_tx_.load();

  // Cleanup commit log if necessary
  cc_.commit_tree_.CompactCommitLog();
}

void WorkerContext::CommitTx() {
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
    active_tx_.commit_ts_ = store_->AllocUsrTxTs();
    cc_.commit_tree_.AppendCommitLog(active_tx_.start_ts_, active_tx_.commit_ts_);
    cc_.latest_commit_ts_.store(active_tx_.commit_ts_, std::memory_order_release);
  } else {
    LS_DLOG("Transaction has no writes, skip assigning commitTs, append log to "
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
    logging_.WriteWalTxFinish();
  }

  // for group commit
  if (active_tx_.has_remote_dependency_) {
    std::unique_lock<std::mutex> g(logging_.tx_to_commit_mutex_);
    logging_.tx_to_commit_.push_back(active_tx_);
  } else {
    std::unique_lock<std::mutex> g(logging_.rfa_tx_to_commit_mutex_);
    logging_.rfa_tx_to_commit_.push_back(active_tx_);
  }

  // Cleanup versions in history tree
  cc_.GarbageCollection();

  // Wait logs to be flushed
  LS_DLOG("Wait transaction to commit, workerId={}, startTs={}, commitTs={}, maxObseredSysTx={}, "
          "hasRemoteDep={}",
          worker_id_, active_tx_.start_ts_, active_tx_.commit_ts_,
          active_tx_.max_observed_sys_tx_id_, active_tx_.has_remote_dependency_);

  logging_.WaitToCommit(active_tx_.commit_ts_);
}

/// TODO(jian.z): revert changes made in-place on the btree process of a transaction abort:
///
/// 1. Read previous wal entries
/// 2. Undo the changes via btree operations
/// 3. Write compensation wal entries during the undo process
/// 4. Purge versions in history tree, clean garbages made by the aborted transaction
///
/// It may share the same code with the recovery process?
void WorkerContext::AbortTx() {
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
  LS_DCHECK(!active_tx_.wal_exceed_buffer_, "Aborting from WAL file is not supported yet");
  std::vector<const WalEntry*> entries;
  logging_.IterateCurrentTxWALs([&](const WalEntry& entry) {
    if (entry.type_ == WalEntry::Type::kComplex) {
      entries.push_back(&entry);
    }
  });

  const uint64_t tx_id = active_tx_.start_ts_;
  std::for_each(entries.rbegin(), entries.rend(), [&](const WalEntry* entry) {
    const auto& complex_entry = *reinterpret_cast<const WalEntryComplex*>(entry);
    store_->tree_registry_->undo(complex_entry.tree_id_, complex_entry.payload_, tx_id);
  });

  cc_.history_storage_.PurgeVersions(
      active_tx_.start_ts_, active_tx_.start_ts_,
      [&](const TXID, const TREEID, const uint8_t*, uint64_t, const bool) {}, 0);

  if (active_tx_.has_wrote_ && active_tx_.is_durable_) {
    // TODO: write compensation wal records between abort and finish
    logging_.WriteWalTxAbort();
    logging_.WriteWalTxFinish();
  }
}

PerfCounters* WorkerContext::GetPerfCounters() {
  return &tls_perf_counters;
}

} // namespace leanstore::cr
