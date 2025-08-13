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

void AutoCommitProtocol::CommitSysTx() {
  auto& cur_worker_ctx = CoroEnv::CurTxMgr();
  auto sys_tx_written = CoroEnv::CurLogging().GetSysTxWrittern();
  cur_worker_ctx.UpdateLastCommittedSysTx(sys_tx_written);
}

void AutoCommitProtocol::CommitUsrTx() {
  TrySyncLastCommittedTx();

  TXID max_commit_ts = DetermineCommitableUsrTx();
  TXID max_commit_ts_rfa = DetermineCommitableUsrTxRfA();
  TXID signaled_up_to = 0;
  if (max_commit_ts == 0 && max_commit_ts_rfa != 0) {
    signaled_up_to = max_commit_ts_rfa;
  } else if (max_commit_ts != 0 && max_commit_ts_rfa == 0) {
    signaled_up_to = max_commit_ts;
  } else if (max_commit_ts != 0 && max_commit_ts_rfa != 0) {
    signaled_up_to = std::min<TXID>(max_commit_ts, max_commit_ts_rfa);
  }
  if (signaled_up_to > 0) {
    CoroEnv::CurTxMgr().UpdateLastCommittedUsrTx(signaled_up_to);
  }
}

void AutoCommitProtocol::TrySyncLastCommittedTx() {
  ScopedTimer timer([&](double elapsed_ms) {
    CoroEnv::CurStore()->MvccManager()->UpdateMinCommittedSysTx(min_committed_sys_tx_);
    LEAN_DLOG("SyncLastCommittedTx finished, workerId={}, elapsed_ms={}"
              ", min_committed_sys_tx={}, min_committed_usr_tx={}",
              CoroEnv::CurTxMgr().worker_id_, elapsed_ms, min_committed_sys_tx_,
              min_committed_usr_tx_);
  });

  auto& tx_mgrs = store_->MvccManager()->TxMgrs();
  for (auto i = 0u; i < tx_mgrs.size(); i++) {
    auto& tx_mgr = tx_mgrs[i];

    // sync last committed sys tx
    auto last_committed_sys_tx = tx_mgr->GetLastCommittedSysTx();
    if (last_committed_sys_tx != last_committed_sys_tx_[i]) {
      last_committed_sys_tx_[i] = last_committed_sys_tx;
      min_committed_sys_tx_ = std::min(min_committed_sys_tx_, last_committed_sys_tx);
    }

    // sync last committed user tx
    auto last_committed_usr_tx = tx_mgr->GetLastCommittedUsrTx();
    if (last_committed_usr_tx != last_committed_usr_tx_[i]) {
      last_committed_usr_tx_[i] = last_committed_usr_tx;
      min_committed_usr_tx_ = std::min(min_committed_usr_tx_, last_committed_usr_tx);
    }
  }
}

TXID AutoCommitProtocol::DetermineCommitableUsrTx() {
  TXID max_commit_ts = 0;
  auto& logging = CoroEnv::CurLogging();
  auto& tx_queue = logging.tx_to_commit_;

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

TXID AutoCommitProtocol::DetermineCommitableUsrTxRfA() {
  TXID max_commit_ts = 0;
  for (auto& tx : CoroEnv::CurLogging().rfa_tx_to_commit_) {
    max_commit_ts = std::max<TXID>(max_commit_ts, tx.commit_ts_);
    LEAN_DLOG("RFA Transaction committed, startTs={}, commitTs={}", tx.start_ts_, tx.commit_ts_);
  }
  CoroEnv::CurLogging().rfa_tx_to_commit_.clear();
  return max_commit_ts;
}

} // namespace leanstore