#include "coroutine/auto_commit_protocol.hpp"

#include "coroutine/coro_env.hpp"
#include "coroutine/mvcc_manager.hpp"
#include "leanstore/concurrency/logging.hpp"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/utils/managed_thread.hpp"
#include "utils/scoped_timer.hpp"

#include <algorithm>
#include <cassert>
#include <limits>

namespace leanstore {

AutoCommitProtocol::AutoCommitProtocol(LeanStore* store, uint32_t group_id)
    : store_(store),
      group_id_(group_id) {
  auto num_workers = store->store_option_->worker_threads_;

  synced_last_committed_sys_tx_.resize(num_workers, 0);
  synced_last_committed_usr_tx_.resize(num_workers, 0);

  min_committed_usr_tx_ = 0;
  min_committed_sys_tx_ = 0;
};

void AutoCommitProtocol::LogFlush() {
  auto& logging = CoroEnv::CurLogging();

  logging.CoroFlush();

  // All system transactions are hardened after log flush
  auto last_hardened_sys_tx = logging.GetLastHardenedSysTx();
  last_hardened_sys_tx = std::max<lean_txid_t>(last_hardened_sys_tx, logging.GetBufferedSysTx());
  logging.SetLastHardenedSysTx(last_hardened_sys_tx);

  // All user transactions are hardened after log flush
  auto max_hardened_usr_tx = logging.GetLastHardenedUsrTx();
  for (auto* tx_mgr : active_tx_mgrs_) {
    for (auto& tx : tx_mgr->tx_to_commit_) {
      max_hardened_usr_tx = std::max<lean_txid_t>(max_hardened_usr_tx, tx.start_ts_);
    }
    for (auto& tx : tx_mgr->rfa_tx_to_commit_) {
      max_hardened_usr_tx = std::max<lean_txid_t>(max_hardened_usr_tx, tx.start_ts_);
    }
  }
  logging.SetLastHardenedUsrTx(max_hardened_usr_tx);
}

void AutoCommitProtocol::CommitAck() {
  TrySyncLastCommittedTx();

  for (auto* tx_mgr : active_tx_mgrs_) {
    auto& tx_queue = tx_mgr->tx_to_commit_;
    auto& tx_queue_rfa = tx_mgr->rfa_tx_to_commit_;

    // Determine the maximum commit timestamp for user transactions
    lean_txid_t max_commit_ts = DetermineCommitableUsrTx(tx_queue);
    lean_txid_t max_commit_ts_rfa = DetermineCommitableUsrTxRfA(tx_queue_rfa);

    // Update the last committed transaction IDs
    lean_txid_t signaled_up_to = 0;
    if (max_commit_ts == 0 && max_commit_ts_rfa != 0) {
      signaled_up_to = max_commit_ts_rfa;
    } else if (max_commit_ts != 0 && max_commit_ts_rfa == 0) {
      signaled_up_to = max_commit_ts;
    } else if (max_commit_ts != 0 && max_commit_ts_rfa != 0) {
      signaled_up_to = std::min(max_commit_ts, max_commit_ts_rfa);
    }

    if (signaled_up_to > 0) {
      tx_mgr->UpdateLastCommittedUsrTx(signaled_up_to);
    }
  }
}

void AutoCommitProtocol::TrySyncLastCommittedTx() {
  ScopedTimer timer([&]([[maybe_unused]] double elapsed_ms) {
    CoroEnv::CurStore().GetMvccManager().UpdateMinCommittedSysTx(min_committed_sys_tx_);
    LEAN_DLOG("SyncLastCommittedTx finished, elapsed_ms={}"
              ", min_committed_sys_tx={}, min_committed_usr_tx={}",
              elapsed_ms, min_committed_sys_tx_, min_committed_usr_tx_);
  });

  // sync last committed sys tx
  auto min_committed_sys_tx = std::numeric_limits<lean_txid_t>::max();
  auto& loggings = store_->GetMvccManager().Loggings();
  assert(loggings.size() == synced_last_committed_sys_tx_.size());
  for (auto i = 0u; i < loggings.size(); i++) {
    auto last_committed_sys_tx = loggings[i]->GetLastHardenedSysTx();
    if (last_committed_sys_tx != synced_last_committed_sys_tx_[i]) {
      synced_last_committed_sys_tx_[i] = last_committed_sys_tx;
      min_committed_sys_tx = std::min(min_committed_sys_tx, last_committed_sys_tx);
    }
  }
  if (min_committed_sys_tx != std::numeric_limits<lean_txid_t>::max()) {
    min_committed_sys_tx_ = std::max(min_committed_sys_tx_, min_committed_sys_tx);
    // min_committed_sys_tx_ = min_committed_sys_tx;
  }

  // sync last committed user tx
  auto min_committed_usr_tx = std::numeric_limits<lean_txid_t>::max();
  assert(loggings.size() == synced_last_committed_usr_tx_.size());
  for (auto i = 0u; i < loggings.size(); i++) {
    auto last_committed_usr_tx = loggings[i]->GetLastHardenedUsrTx();
    if (last_committed_usr_tx != synced_last_committed_usr_tx_[i]) {
      synced_last_committed_usr_tx_[i] = last_committed_usr_tx;
      min_committed_usr_tx = std::min(min_committed_usr_tx, last_committed_usr_tx);
    }
  }
  if (min_committed_usr_tx != std::numeric_limits<lean_txid_t>::max()) {
    min_committed_usr_tx_ = min_committed_usr_tx;
  }
}

lean_txid_t AutoCommitProtocol::DetermineCommitableUsrTx(std::vector<Transaction>& tx_queue) {
  lean_txid_t max_commit_ts = 0;
  auto i = 0u;
  for (; i < tx_queue.size(); ++i) {
    auto& tx = tx_queue[i];
    if (!tx.CanCommit(min_committed_sys_tx_, min_committed_usr_tx_)) {
      break;
    }
    max_commit_ts = std::max<lean_txid_t>(max_commit_ts, tx.commit_ts_);
    LEAN_DLOG("Transaction committed, startTs={}, commitTs={}", tx.start_ts_, tx.commit_ts_);
  }
  if (i > 0) {
    tx_queue.erase(tx_queue.begin(), tx_queue.begin() + i);
  }
  return max_commit_ts;
}

lean_txid_t AutoCommitProtocol::DetermineCommitableUsrTxRfA(
    std::vector<Transaction>& tx_queue_rfa) {
  lean_txid_t max_commit_ts = 0;
  for (auto& tx : tx_queue_rfa) {
    max_commit_ts = std::max<lean_txid_t>(max_commit_ts, tx.commit_ts_);
    LEAN_DLOG("Transaction-RFA committed, startTs={}, commitTs={}", tx.start_ts_, tx.commit_ts_);
  }
  tx_queue_rfa.clear();
  return max_commit_ts;
}

} // namespace leanstore