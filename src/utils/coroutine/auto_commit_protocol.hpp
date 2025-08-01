#pragma once

#include "leanstore/units.hpp"

#include <vector>

namespace leanstore {

class AutoCommitProtocol {
public:
  AutoCommitProtocol(uint32_t commit_group) : commit_group_(commit_group) {};
  ~AutoCommitProtocol() = default;

  // No copy and assignment
  AutoCommitProtocol(const AutoCommitProtocol&) = delete;
  AutoCommitProtocol& operator=(const AutoCommitProtocol&) = delete;

  void Run() {
    if (LogFlush()) {
      CommitAck();
    }
  }

private:
  /// Commit Phase 1.
  ///
  /// Performs the autonomous log flush phase for decentralized logging. All
  /// logging state is recorded in the Logging component of each WorkerContext.
  ///
  /// Return true if any log flush is performed successfully, false otherwise.
  static bool LogFlush();

  /// Commit Phase 2.
  ///
  /// Performs the commit acknowledgment phase. Syncs the last committed
  /// transaction ID for all workers in the system, only when all the dependent
  /// transactions are committed the pending ack transaction can be committed.
  ///
  /// The synced last committed transaction ID is shared for all workers in the
  /// same commit group.
  void CommitAck() {
  }

  /// Sync last committed transaction ID of all workers.
  void TrySyncLastCommittedTx() {
  }

  /// Commit group id, identifies the group of workers that are committing
  /// together.  All workers in the same commit group shares the same
  /// AutoCommitProtocol instance and the same commit acknowledgment.
  const uint32_t commit_group_;

  /// TXID of the last committed transaction for each worker
  std::vector<TXID> last_committed_tx_;
};

} // namespace leanstore