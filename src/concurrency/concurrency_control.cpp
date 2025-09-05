#include "leanstore/concurrency/concurrency_control.hpp"

#include "leanstore/buffer-manager/tree_registry.hpp"
#include "leanstore/common/perf_counters.h"
#include "leanstore/common/types.h"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/exceptions.hpp"
#include "leanstore/sync/hybrid_mutex.hpp"
#include "leanstore/sync/scoped_hybrid_guard.hpp"
#include "leanstore/units.hpp"
#include "leanstore/utils/counter_util.hpp"
#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/jump_mu.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/random_generator.hpp"
#include "utils/coroutine/mvcc_manager.hpp"

#include <atomic>
#include <format>
#include <set>

namespace leanstore::cr {

//------------------------------------------------------------------------------
// CommitTree
//------------------------------------------------------------------------------

void CommitTree::AppendCommitLog(lean_txid_t start_ts, lean_txid_t commit_ts) {
  LEAN_DCHECK(commit_log_.size() < capacity_,
              std::format("Commit log is full, commit_log_.size()={}, capacity_={}",
                          commit_log_.size(), capacity_));
  storage::ScopedHybridGuard x_guard(latch_, storage::LatchMode::kExclusivePessimistic);
  commit_log_.push_back({commit_ts, start_ts});
  LEAN_DLOG("Commit log appended, workerId={}, startTs={}, commitTs={}",
            CoroEnv::CurTxMgr().worker_id_, start_ts, commit_ts);
}

void CommitTree::CompactCommitLog() {
  if (commit_log_.size() < capacity_) {
    return;
  }

  // Calculate the compacted commit log.
  std::set<std::pair<lean_txid_t, lean_txid_t>> set;

  // Keep the latest (commitTs, startTs) in the commit log, so that other
  // workers can see the latest commitTs of this worker.
  set.insert(commit_log_[commit_log_.size() - 1]);

  const lean_wid_t my_worker_id = CoroEnv::CurTxMgr().worker_id_;
  auto& tx_mgrs = CoroEnv::CurTxMgr().tx_mgrs_;
  for (lean_wid_t i = 0; i < CoroEnv::CurTxMgr().tx_mgrs_.size(); i++) {
    if (i == my_worker_id) {
      continue;
    }

    auto active_tx_id = tx_mgrs[i]->active_tx_id_.load();
    if (active_tx_id == 0) {
      // Don't need to keep the old commit log entry if the worker is not
      // running any transaction.
      continue;
    }

    active_tx_id &= TxManager::kCleanBitsMask;
    if (auto result = LcbUnlocked(active_tx_id); result) {
      set.insert(*result);
    }
  }

  // Refill the compacted commit log
  storage::ScopedHybridGuard x_guard(latch_, storage::LatchMode::kExclusivePessimistic);
  commit_log_.clear();
  for (auto& p : set) {
    commit_log_.push_back(p);
  }

  DEBUG_BLOCK() {
    LEAN_DLOG("Commit log cleaned up, workerId={}, commit_log_.size()={}",
              CoroEnv::CurTxMgr().worker_id_, commit_log_.size());
  }
}

lean_txid_t CommitTree::Lcb(lean_txid_t start_ts) {
  COUNTER_INC(&tls_perf_counters.lcb_executed_);
  COUNTER_TIMER_SCOPED(&tls_perf_counters.lcb_total_lat_ns_);

  while (true) {
    JUMPMU_TRY() {
      storage::ScopedHybridGuard o_guard(latch_, storage::LatchMode::kOptimisticOrJump);
      if (auto result = LcbUnlocked(start_ts); result) {
        o_guard.Unlock();
        JUMPMU_RETURN result->second;
      }
      o_guard.Unlock();
      JUMPMU_RETURN 0;
    }
    JUMPMU_CATCH() {
    }
  }
}

std::optional<std::pair<lean_txid_t, lean_txid_t>> CommitTree::LcbUnlocked(lean_txid_t start_ts) {
  auto comp = [&](const auto& pair, lean_txid_t start_ts) { return start_ts > pair.first; };
  auto it = std::lower_bound(commit_log_.begin(), commit_log_.end(), start_ts, comp);
  if (it == commit_log_.begin()) {
    return {};
  }
  it--;
  LEAN_DCHECK(it->second < start_ts);
  return *it;
}

//------------------------------------------------------------------------------
// ConcurrencyControl
//------------------------------------------------------------------------------

lean_cmdid_t ConcurrencyControl::PutVersion(lean_treeid_t tree_id, bool is_remove_command,
                                            uint64_t version_size,
                                            std::function<void(uint8_t*)> put_call_back) {
  auto& tx_mgr = CoroEnv::CurTxMgr();
  auto command_id = tx_mgr.cmd_id_++;
  if (is_remove_command) {
    command_id |= kCmdRemoveMark;
  }
  history_storage_.PutVersion(tx_mgr.ActiveTx().start_ts_, command_id, tree_id, is_remove_command,
                              version_size, put_call_back);
  return command_id;
}

bool ConcurrencyControl::VisibleForMe(lean_wid_t worker_id, lean_txid_t tx_id) {
  // visible if writtern by me
  if (CoroEnv::CurTxMgr().worker_id_ == worker_id) {
    return true;
  }

  switch (CoroEnv::CurTxMgr().ActiveTx().tx_isolation_level_) {
  case IsolationLevel::kSnapshotIsolation:
  case IsolationLevel::kSerializable: {
    // global_wmk_of_all_tx_ is copied from global watermark info at the beginning of each
    // transaction. Global watermarks are occassionally updated by
    // TxManager::updateGlobalTxWatermarks, it's possible that global_wmk_of_all_tx_ is not the
    // latest value, but it is always safe to use it as the lower bound of the visibility check.
    if (tx_id < global_wmk_of_all_tx_) {
      return true;
    }

    // If we have queried the LCB on the target worker and cached the value in lcb_cache_val_, we
    // can use it to check the visibility directly.
    if (lcb_cache_key_[worker_id] == CoroEnv::CurTxMgr().ActiveTx().start_ts_) {
      return lcb_cache_val_[worker_id] >= tx_id;
    }

    // If the tuple is visible for the last transaction, it is visible for the current transaction
    // as well. No need to query LCB on the target worker.
    if (lcb_cache_val_[worker_id] >= tx_id) {
      return true;
    }

    // Now we need to query LCB on the target worker and update the local cache.
    lean_txid_t largest_visible_tx_id =
        Other(worker_id).commit_tree_.Lcb(CoroEnv::CurTxMgr().ActiveTx().start_ts_);
    if (largest_visible_tx_id) {
      lcb_cache_key_[worker_id] = CoroEnv::CurTxMgr().ActiveTx().start_ts_;
      lcb_cache_val_[worker_id] = largest_visible_tx_id;
      return largest_visible_tx_id >= tx_id;
    }

    return false;
  }
  default: {
    Log::Fatal("Unsupported isolation level: {}",
               static_cast<uint64_t>(CoroEnv::CurTxMgr().ActiveTx().tx_isolation_level_));
  }
  }

  return false;
}

bool ConcurrencyControl::VisibleForAll(lean_txid_t tx_id) {
  return tx_id < store_->MvccManager()->GlobalWmkInfo().wmk_of_all_tx_.load();
}

// TODO: smooth purge, we should not let the system hang on this, as a quick
// fix, it should be enough if we purge in small batches
void ConcurrencyControl::GarbageCollection() {
  if (!store_->store_option_->enable_gc_) {
    return;
  }

  COUNTER_INC(&tls_perf_counters.gc_executed_);
  COUNTER_TIMER_SCOPED(&tls_perf_counters.gc_total_lat_ns_);

  UpdateGlobalWmks();
  UpdateLocalWmks();

  // remove versions that are nolonger needed by any transaction
  if (cleaned_wmk_of_short_tx_ <= local_wmk_of_all_tx_) {
    LEAN_DLOG(
        "Garbage collect history tree, workerId={}, fromTxId={}, toTxId(local_wmk_of_all_tx_)={}",
        CoroEnv::CurTxMgr().worker_id_, 0, local_wmk_of_all_tx_);
    history_storage_.PurgeVersions(
        0, local_wmk_of_all_tx_,
        [&](const lean_txid_t version_tx_id, const lean_treeid_t tree_id,
            const uint8_t* version_data, uint64_t version_size [[maybe_unused]],
            const bool called_before) {
          store_->tree_registry_->GarbageCollect(
              tree_id, version_data, CoroEnv::CurTxMgr().worker_id_, version_tx_id, called_before);
        },
        0);
    cleaned_wmk_of_short_tx_ = local_wmk_of_all_tx_ + 1;
  } else {
    LEAN_DLOG("Skip garbage collect history tree, workerId={}, "
              "cleaned_wmk_of_short_tx_={}, local_wmk_of_all_tx_={}",
              CoroEnv::CurTxMgr().worker_id_, cleaned_wmk_of_short_tx_, local_wmk_of_all_tx_);
  }

  // move tombstones to graveyard
  if (store_->store_option_->enable_long_running_tx_ &&
      local_wmk_of_all_tx_ < local_wmk_of_short_tx_ &&
      cleaned_wmk_of_short_tx_ <= local_wmk_of_short_tx_) {
    LEAN_DLOG("Garbage collect graveyard, workerId={}, fromTxId={}, "
              "toTxId(local_wmk_of_short_tx_)={}",
              CoroEnv::CurTxMgr().worker_id_, cleaned_wmk_of_short_tx_, local_wmk_of_short_tx_);
    history_storage_.VisitRemovedVersions(
        cleaned_wmk_of_short_tx_, local_wmk_of_short_tx_,
        [&](const lean_txid_t version_tx_id, const lean_treeid_t tree_id,
            const uint8_t* version_data, uint64_t, const bool called_before) {
          store_->tree_registry_->GarbageCollect(
              tree_id, version_data, CoroEnv::CurTxMgr().worker_id_, version_tx_id, called_before);
        });
    cleaned_wmk_of_short_tx_ = local_wmk_of_short_tx_ + 1;
  } else {
    LEAN_DLOG("Skip garbage collect graveyard, workerId={}, "
              "cleaned_wmk_of_short_tx_={}, local_wmk_of_short_tx_={}",
              CoroEnv::CurTxMgr().worker_id_, cleaned_wmk_of_short_tx_, local_wmk_of_short_tx_);
  }
}

ConcurrencyControl& ConcurrencyControl::Other(lean_wid_t other_worker_id) {
  LEAN_DCHECK(other_worker_id < CoroEnv::CurTxMgr().tx_mgrs_.size(),
              std::format("Invalid other_worker_id: {}", other_worker_id));
  return CoroEnv::CurTxMgr().tx_mgrs_[other_worker_id]->cc_;
}

// It calculates and updates the global oldest running transaction id and the
// oldest running short-running transaction id. Based on these two oldest
// running transaction ids, it calculates and updates the global watermarks of
// all transactions and short-running transactions, under which all transactions
// and short-running transactions are visible, and versions older than the
// watermarks can be garbage collected.
//
// Called by the worker thread that is committing a transaction before garbage
// collection.
void ConcurrencyControl::UpdateGlobalWmks() {
  if (!store_->store_option_->enable_gc_) {
    LEAN_DLOG("Skip updating global watermarks, GC is disabled");
    return;
  }

  auto meet_gc_probability =
      store_->store_option_->enable_eager_gc_ ||
      utils::RandomGenerator::RandU64(0, CoroEnv::CurTxMgr().tx_mgrs_.size()) == 0;
  auto perform_gc =
      meet_gc_probability && store_->MvccManager()->GlobalWmkInfo().global_mutex_.try_lock();
  if (!perform_gc) {
    LEAN_DLOG("Skip updating global watermarks, meetGcProbability={}, performGc={}",
              meet_gc_probability, perform_gc);
    return;
  }

  // release the lock on exit
  SCOPED_DEFER(store_->MvccManager()->GlobalWmkInfo().global_mutex_.unlock());

  // There is a chance that oldestTxId or oldestShortTxId is
  // std::numeric_limits<lean_txid_t>::max(). It is ok because LCB(+oo) returns the id
  // of latest committed transaction. Under this condition, all the tombstones
  // or update versions generated by the previous transactions can be garbage
  // collected, i.e. removed or moved to graveyard.
  lean_txid_t oldest_tx_id = std::numeric_limits<lean_txid_t>::max();
  lean_txid_t newest_long_tx_id = std::numeric_limits<lean_txid_t>::min();
  lean_txid_t oldest_short_tx_id = std::numeric_limits<lean_txid_t>::max();
  auto& tx_mgrs = CoroEnv::CurTxMgr().tx_mgrs_;
  for (lean_wid_t i = 0; i < CoroEnv::CurTxMgr().tx_mgrs_.size(); i++) {
    auto active_tx_id = tx_mgrs[i]->active_tx_id_.load();
    // Skip transactions not running.
    if (active_tx_id == 0) {
      continue;
    }
    // Skip transactions running in read-committed mode.
    if (active_tx_id & TxManager::kRcBit) {
      continue;
    }

    bool is_long_running_tx = active_tx_id & TxManager::kLongRunningBit;
    active_tx_id &= TxManager::kCleanBitsMask;
    oldest_tx_id = std::min(active_tx_id, oldest_tx_id);
    if (is_long_running_tx) {
      newest_long_tx_id = std::max(active_tx_id, newest_long_tx_id);
    } else {
      oldest_short_tx_id = std::min(active_tx_id, oldest_short_tx_id);
    }
  }

  // Update the three transaction ids
  store_->MvccManager()->GlobalWmkInfo().UpdateActiveTxInfo(oldest_tx_id, oldest_short_tx_id,
                                                            newest_long_tx_id);

  if (!store_->store_option_->enable_long_running_tx_ &&
      store_->MvccManager()->GlobalWmkInfo().oldest_active_tx_ !=
          store_->MvccManager()->GlobalWmkInfo().oldest_active_short_tx_) {
    Log::Fatal("Oldest transaction id should be equal to the oldest "
               "short-running transaction id when long-running transaction is "
               "disabled");
  }

  // Update global lower watermarks based on the three transaction ids
  lean_txid_t global_wmk_of_all_tx = std::numeric_limits<lean_txid_t>::max();
  lean_txid_t global_wmk_of_short_tx = std::numeric_limits<lean_txid_t>::max();
  for (lean_wid_t i = 0; i < CoroEnv::CurTxMgr().tx_mgrs_.size(); i++) {
    ConcurrencyControl& cc = Other(i);
    if (cc.updated_latest_commit_ts_ == cc.latest_commit_ts_) {
      LEAN_DLOG("Skip updating watermarks for worker {}, no transaction "
                "committed since last round, latest_commit_ts_={}",
                i, cc.latest_commit_ts_.load());
      lean_txid_t wmk_of_all_tx = cc.wmk_of_all_tx_;
      lean_txid_t wmk_of_short_tx = cc.wmk_of_short_tx_;
      if (wmk_of_all_tx > 0 || wmk_of_short_tx > 0) {
        global_wmk_of_all_tx = std::min(wmk_of_all_tx, global_wmk_of_all_tx);
        global_wmk_of_short_tx = std::min(wmk_of_short_tx, global_wmk_of_short_tx);
      }
      continue;
    }

    lean_txid_t wmk_of_all_tx =
        cc.commit_tree_.Lcb(store_->MvccManager()->GlobalWmkInfo().oldest_active_tx_);
    lean_txid_t wmk_of_short_tx =
        cc.commit_tree_.Lcb(store_->MvccManager()->GlobalWmkInfo().oldest_active_short_tx_);

    cc.wmk_version_.store(cc.wmk_version_.load() + 1, std::memory_order_release);
    cc.wmk_of_all_tx_.store(wmk_of_all_tx, std::memory_order_release);
    cc.wmk_of_short_tx_.store(wmk_of_short_tx, std::memory_order_release);
    cc.wmk_version_.store(cc.wmk_version_.load() + 1, std::memory_order_release);
    cc.updated_latest_commit_ts_.store(cc.latest_commit_ts_, std::memory_order_release);
    LEAN_DLOG("Watermarks updated for worker {}, wmk_of_all_tx_=LCB({})={}, "
              "wmk_of_short_tx_=LCB({})={}",
              i, wmk_of_all_tx, cc.wmk_of_all_tx_.load(), wmk_of_short_tx,
              cc.wmk_of_short_tx_.load());

    // The lower watermarks of current worker only matters when there are
    // transactions started before global oldestActiveTx
    if (wmk_of_all_tx > 0 || wmk_of_short_tx > 0) {
      global_wmk_of_all_tx = std::min(wmk_of_all_tx, global_wmk_of_all_tx);
      global_wmk_of_short_tx = std::min(wmk_of_short_tx, global_wmk_of_short_tx);
    }
  }

  // If a worker hasn't committed any new transaction since last round, the
  // commit log keeps the same, which causes the lower watermarks the same
  // as last round, which further causes the global lower watermarks the
  // same as last round. This is not a problem, but updating the global
  // lower watermarks is not necessary in this case.
  if (store_->MvccManager()->GlobalWmkInfo().wmk_of_all_tx_ == global_wmk_of_all_tx &&
      store_->MvccManager()->GlobalWmkInfo().wmk_of_short_tx_ == global_wmk_of_short_tx) {
    LEAN_DLOG("Skip updating global watermarks, global watermarks are the "
              "same as last round, globalWmkOfAllTx={}, globalWmkOfShortTx={}",
              global_wmk_of_all_tx, global_wmk_of_short_tx);
    return;
  }

  // lean_txid_t globalWmkOfAllTx = std::numeric_limits<lean_txid_t>::max();
  // lean_txid_t globalWmkOfShortTx = std::numeric_limits<lean_txid_t>::max();
  if (global_wmk_of_all_tx == std::numeric_limits<lean_txid_t>::max() ||
      global_wmk_of_short_tx == std::numeric_limits<lean_txid_t>::max()) {
    LEAN_DLOG("Skip updating global watermarks, can not find any valid lower "
              "watermarks, globalWmkOfAllTx={}, globalWmkOfShortTx={}",
              global_wmk_of_all_tx, global_wmk_of_short_tx);
    return;
  }

  store_->MvccManager()->GlobalWmkInfo().UpdateWmks(global_wmk_of_all_tx, global_wmk_of_short_tx);
}

void ConcurrencyControl::UpdateLocalWmks() {
  SCOPED_DEFER(LEAN_DLOG("Local watermarks updated, workerId={}, "
                         "local_wmk_of_all_tx_={}, local_wmk_of_short_tx_={}",
                         CoroEnv::CurTxMgr().worker_id_, local_wmk_of_all_tx_,
                         local_wmk_of_short_tx_));
  while (true) {
    uint64_t version = wmk_version_.load();

    // spin until the latch is free
    while ((version = wmk_version_.load()) & 1) {
    };

    // update the two local watermarks
    local_wmk_of_all_tx_ = wmk_of_all_tx_.load();
    local_wmk_of_short_tx_ = wmk_of_short_tx_.load();

    // restart if the latch was taken
    if (version == wmk_version_.load()) {
      return;
    }
  }

  LEAN_DCHECK(!store_->store_option_->enable_long_running_tx_ ||
                  local_wmk_of_all_tx_ <= local_wmk_of_short_tx_,
              "Lower watermark of all transactions should be no higher than the lower "
              "watermark of short-running transactions, workerId={}, "
              "local_wmk_of_all_tx_={}, local_wmk_of_short_tx_={}",
              CoroEnv::CurTxMgr().worker_id_, local_wmk_of_all_tx_, local_wmk_of_short_tx_);
}

} // namespace leanstore::cr
