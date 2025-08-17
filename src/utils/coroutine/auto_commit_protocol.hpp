#pragma once

#include "leanstore/units.hpp"

#include <unordered_set>
#include <vector>

namespace leanstore::cr {
class TxManager;
class Transaction;
} // namespace leanstore::cr

namespace leanstore {

class LeanStore;

class AutoCommitProtocol {
public:
  AutoCommitProtocol(LeanStore* store, uint32_t commit_group, uint64_t num_workers)
      : store_(store),
        group_id_(commit_group) {
    synced_last_committed_sys_tx_.resize(num_workers, 0);
    synced_last_committed_usr_tx_.resize(num_workers, 0);
  };

  ~AutoCommitProtocol() = default;

  // No copy and assignment
  AutoCommitProtocol(const AutoCommitProtocol&) = delete;
  AutoCommitProtocol& operator=(const AutoCommitProtocol&) = delete;

  /// Commit Phase 1: auto log flush
  /// Commit Phase 2: auto commit ack
  void Run() {
    LogFlush();

    if (active_tx_mgrs_.size() > 0) {
      CommitAck();
    }
  }

  void RegisterTxMgr(cr::TxManager* tx_mgr) {
    active_tx_mgrs_.insert(tx_mgr);
  }

  void UnregisterTxMgr(cr::TxManager* tx_mgr) {
    active_tx_mgrs_.erase(tx_mgr);
  }

private:
  /// Performs the autonomous log flush phase for decentralized logging. All
  /// logging state is recorded in the Logging component of each TxManager.
  ///
  /// Return true if any log flush is performed successfully, false otherwise.
  void LogFlush();

  /// Performs the commit acknowledgment phase. Syncs the last committed
  /// transaction ID for all workers in the system, only when all the dependent
  /// transactions are committed the pending ack transaction can be committed.
  ///
  /// The synced last committed transaction ID is shared for all workers in the
  /// same commit group.
  void CommitAck();

  void TrySyncLastCommittedTx();

  TXID DetermineCommitableUsrTx(std::vector<cr::Transaction>& tx_queue);

  TXID DetermineCommitableUsrTxRfA(std::vector<cr::Transaction>& tx_queue_rfa);

private:
  LeanStore* store_;

  /// Commit group id, identifies the group of workers that are committing
  /// together.  All workers in the same commit group shares the same
  /// AutoCommitProtocol instance and the same commit acknowledgment.
  const uint32_t group_id_;

  std::unordered_set<cr::TxManager*> active_tx_mgrs_;

  std::vector<TXID> synced_last_committed_usr_tx_;
  std::vector<TXID> synced_last_committed_sys_tx_;

  TXID min_committed_usr_tx_ = 0;
  TXID min_committed_sys_tx_ = 0;
};

} // namespace leanstore