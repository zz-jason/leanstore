#include "leanstore/concurrency/tx_manager.hpp"

#include "leanstore/buffer-manager/tree_registry.hpp"
#include "leanstore/common/perf_counters.h"
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
#include "utils/to_json.hpp"

#include <algorithm>
#include <cstdlib>

namespace leanstore::cr {

TxManager::TxManager(uint64_t worker_id, std::vector<std::unique_ptr<TxManager>>& tx_mgrs,
                     leanstore::LeanStore* store)
    : store_(store),
      cc_(store, store->store_option_->worker_threads_ *
                     store->store_option_->max_concurrent_transaction_per_worker_),
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
    LEAN_DLOG("Start transaction, workerId={}, startTs={}, max_observed_sys_tx={}", worker_id_,
              active_tx_.start_ts_, active_tx_.max_observed_sys_tx_id_);
  });

  active_tx_.Start(mode, level);

  if (!active_tx_.is_durable_) {
    return;
  }

  /// Reset the max observed system transaction id
  active_tx_.max_observed_sys_tx_id_ = store_->MvccManager()->GetMinCommittedSysTx();

  // Init wal and group commit related transaction information
  active_tx_.first_wal_ = CoroEnv::CurLogging().wal_buffered_;

  // For now, we only support SI and SSI
  if (level < IsolationLevel::kSnapshotIsolation) {
    Log::Fatal("Unsupported isolation level: {}", static_cast<uint64_t>(level));
  }

  // Draw lean_txid_t from global counter and publish it with the TX type (i.e.  long-running or
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

#ifdef ENABLE_COROUTINE
  CoroEnv::CurCoroExec()->AutoCommitter()->RegisterTxMgr(this);
#endif
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
    WriteWalTxFinish();
  }

  // for group commit
  if (active_tx_.has_remote_dependency_) {
    LEAN_UNIQUE_LOCK(tx_to_commit_mutex_);
    tx_to_commit_.push_back(active_tx_);
  } else {
    LEAN_UNIQUE_LOCK(rfa_tx_to_commit_mutex_);
    rfa_tx_to_commit_.push_back(active_tx_);
  }

  // Cleanup versions in history tree
  cc_.GarbageCollection();

  // Wait logs to be flushed
  LEAN_DLOG("Wait transaction to commit, workerId={}, startTs={}, commitTs={}, maxObseredSysTx={}, "
            "hasRemoteDep={}",
            worker_id_, active_tx_.start_ts_, active_tx_.commit_ts_,
            active_tx_.max_observed_sys_tx_id_, active_tx_.has_remote_dependency_);

  WaitToCommit(active_tx_.commit_ts_);

#ifdef ENABLE_COROUTINE
  CoroEnv::CurCoroExec()->AutoCommitter()->UnregisterTxMgr(this);
#endif
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
  CoroEnv::CurLogging().IterateCurrentTxWALs(active_tx_.first_wal_, [&](const WalEntry& entry) {
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
      [&](const lean_txid_t, const lean_treeid_t, const uint8_t*, uint64_t, const bool) {}, 0);

  if (active_tx_.has_wrote_ && active_tx_.is_durable_) {
    // TODO: write compensation wal records between abort and finish
    WriteWalTxAbort();
    WriteWalTxFinish();
  }

#ifdef ENABLE_COROUTINE
  CoroEnv::CurCoroExec()->AutoCommitter()->UnregisterTxMgr(this);
#endif
}

void TxManager::WriteWalTxAbort() {
  auto& logging = CoroEnv::CurLogging();

  // Reserve space
  auto size = sizeof(WalTxAbort);
  auto* data = logging.ReserveWalBuffer(size);

  // Initialize a WalTxAbort
  std::memset(data, 0, size);
  auto* entry [[maybe_unused]] = new (data) WalTxAbort(size);

  // Submit the WalTxAbort to group committer
  logging.wal_buffered_ += size;
  logging.PublishWalFlushReq(active_tx_.start_ts_);

  LEAN_DLOG("WriteWalTxAbort, workerId={}, startTs={}, walJson={}", worker_id_,
            active_tx_.start_ts_, utils::ToJsonString(entry));
}

void TxManager::WriteWalTxFinish() {
  auto& logging = CoroEnv::CurLogging();

  // Reserve space
  auto size = sizeof(WalTxFinish);
  auto* data = logging.ReserveWalBuffer(size);

  // Initialize a WalTxFinish
  std::memset(data, 0, size);
  auto* entry [[maybe_unused]] = new (data) WalTxFinish(active_tx_.start_ts_);

  // Submit the WalTxAbort to group committer
  logging.wal_buffered_ += size;
  logging.PublishWalFlushReq(active_tx_.start_ts_);

  LEAN_DLOG("WriteWalTxFinish, workerId={}, startTs={}, walJson={}", worker_id_,
            active_tx_.start_ts_, utils::ToJsonString(entry));
}

void TxManager::SubmitWALEntryComplex(uint64_t total_size) {
  auto& logging = CoroEnv::CurLogging();

  active_walentry_complex_->crc32_ = active_walentry_complex_->ComputeCRC32();
  logging.wal_buffered_ += total_size;
  logging.PublishWalFlushReq(active_tx_.start_ts_);

  LEAN_DLOG("SubmitWal, workerId={}, startTs={}, walJson={}", CoroEnv::CurTxMgr().worker_id_,
            CoroEnv::CurTxMgr().ActiveTx().start_ts_,
            utils::ToJsonString(active_walentry_complex_));
}

lean_perf_counters* TxManager::GetPerfCounters() {
  return &tls_perf_counters;
}

} // namespace leanstore::cr
