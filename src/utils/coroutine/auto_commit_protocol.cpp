#include "utils/coroutine/auto_commit_protocol.hpp"

#include "leanstore/concurrency/logging.hpp"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/utils/managed_thread.hpp"
#include "utils/coroutine/coro_env.hpp"
#include "utils/coroutine/mvcc_manager.hpp"
#include "utils/scoped_timer.hpp"

namespace leanstore {

bool AutoCommitProtocol::LogFlush() {
  return CoroEnv::CurLogging().CoroFlush();
}

void AutoCommitProtocol::CommitAck() {
  TrySyncLastCommittedTx();

  for (auto* tx_mgr : active_tx_mgrs_) {
    auto& tx_queue = tx_mgr->tx_to_commit_;
    auto& tx_queue_rfa = tx_mgr->rfa_tx_to_commit_;

    // Determine the maximum commit timestamp for user transactions
    TXID max_commit_ts = DetermineCommitableUsrTx(tx_queue);
    TXID max_commit_ts_rfa = DetermineCommitableUsrTxRfA(tx_queue_rfa);

    // Update the last committed transaction IDs
    TXID signaled_up_to = 0;
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
  ScopedTimer timer([&](double elapsed_ms) {
    CoroEnv::CurStore()->MvccManager()->UpdateMinCommittedSysTx(min_committed_sys_tx_);
    LEAN_DLOG("SyncLastCommittedTx finished, elapsed_ms={}"
              ", min_committed_sys_tx={}, min_committed_usr_tx={}",
              elapsed_ms, min_committed_sys_tx_, min_committed_usr_tx_);
  });

  // sync last committed sys tx
  auto& loggings = store_->MvccManager()->Loggings();
  for (auto i = 0u; i < loggings.size(); i++) {
    auto last_committed_sys_tx = loggings[i]->GetSysTxWrittern();
    if (last_committed_sys_tx != synced_last_committed_sys_tx_[i]) {
      synced_last_committed_sys_tx_[i] = last_committed_sys_tx;
      min_committed_sys_tx_ = std::min(min_committed_sys_tx_, last_committed_sys_tx);
    }
  }

  // sync last committed user tx
  auto& tx_mgrs = store_->MvccManager()->TxMgrs();
  for (auto i = 0u; i < tx_mgrs.size(); i++) {
    auto last_committed_usr_tx = tx_mgrs[i]->GetLastCommittedUsrTx();
    if (last_committed_usr_tx != synced_last_committed_usr_tx_[i]) {
      synced_last_committed_usr_tx_[i] = last_committed_usr_tx;
      min_committed_usr_tx_ = std::min(min_committed_usr_tx_, last_committed_usr_tx);
    }
  }
}

TXID AutoCommitProtocol::DetermineCommitableUsrTx(std::vector<cr::Transaction>& tx_queue) {
  TXID max_commit_ts = 0;
  auto i = 0u;
  for (; i < tx_queue.size(); ++i) {
    auto& tx = tx_queue[i];
    if (!tx.CanCommit(min_committed_sys_tx_, min_committed_usr_tx_)) {
      break;
    }
    max_commit_ts = std::max<TXID>(max_commit_ts, tx.commit_ts_);
    LEAN_DLOG("Transaction committed, startTs={}, commitTs={}", tx.start_ts_, tx.commit_ts_);
  }
  if (i > 0) {
    tx_queue.erase(tx_queue.begin(), tx_queue.begin() + i);
  }
  return max_commit_ts;
}

TXID AutoCommitProtocol::DetermineCommitableUsrTxRfA(std::vector<cr::Transaction>& tx_queue_rfa) {
  TXID max_commit_ts = 0;
  for (auto& tx : tx_queue_rfa) {
    max_commit_ts = std::max<TXID>(max_commit_ts, tx.commit_ts_);
    LEAN_DLOG("Transaction-RFA committed, startTs={}, commitTs={}", tx.start_ts_, tx.commit_ts_);
  }
  tx_queue_rfa.clear();
  return max_commit_ts;
}

} // namespace leanstore