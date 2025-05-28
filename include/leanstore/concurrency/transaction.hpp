#pragma once

#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/units.hpp"
#include "leanstore/utils/user_thread.hpp"

namespace leanstore {
namespace cr {

enum class TxState { kIdle, kStarted, kCommitted, kAborted };

struct TxStatUtil {
  inline static std::string ToString(TxState state) {
    switch (state) {
    case TxState::kIdle: {
      return "Idle";
    }
    case TxState::kStarted: {
      return "Started";
    }
    case TxState::kCommitted: {
      return "Committed";
    }
    case TxState::kAborted: {
      return "Aborted";
    }
    default: {
      return "Unknown TxState";
    }
    }
  }
};

class Transaction {
public:
  /// The state of the current transaction.
  TxState state_ = TxState::kIdle;

  /// start_ts_ is the start timestamp of the transaction. Also used as
  /// teansaction ID.
  TXID start_ts_ = 0;

  /// commit_ts_ is the commit timestamp of the transaction.
  TXID commit_ts_ = 0;

  /// Maximum observed system transaction id during transaction processing. Used to track
  /// transaction dependencies.
  TXID max_observed_sys_tx_id_ = 0;

  /// Whether the transaction has any remote dependencies. Currently, we only support SI isolation
  /// level, a user transaction can only depend on a system transaction executed in a remote worker
  /// thread.
  bool has_remote_dependency_ = false;

  /// tx_mode_ is the mode of the current transaction.
  TxMode tx_mode_ = TxMode::kShortRunning;

  /// tx_isolation_level_ is the isolation level for the current transaction.
  IsolationLevel tx_isolation_level_ = IsolationLevel::kSnapshotIsolation;

  /// Whether the transaction has any data writes. Transaction writes can be
  /// detected once it generates a WAL entry.
  bool has_wrote_ = false;

  /// Whether the transaction is durable. A durable transaction can be committed
  /// or aborted only after all the WAL entries are flushed to disk.
  bool is_durable_ = true;

  bool wal_exceed_buffer_ = false;

public:
  bool IsLongRunning() {
    return tx_mode_ == TxMode::kLongRunning;
  }

  bool AtLeastSI() {
    return tx_isolation_level_ >= IsolationLevel::kSnapshotIsolation;
  }

  // Start a new transaction, initialize all fields
  void Start(TxMode mode, IsolationLevel level) {
    state_ = TxState::kStarted;
    start_ts_ = 0;
    commit_ts_ = 0;
    max_observed_sys_tx_id_ = 0;
    has_remote_dependency_ = false;
    tx_mode_ = mode;
    tx_isolation_level_ = level;
    has_wrote_ = false;
    is_durable_ = utils::tls_store->store_option_->enable_wal_;
    wal_exceed_buffer_ = false;
  }

  /// Check whether a user transaction with remote dependencies can be committed.
  bool CanCommit(TXID min_flushed_sys_tx, TXID min_flushed_usr_tx) {
    return max_observed_sys_tx_id_ <= min_flushed_sys_tx && start_ts_ <= min_flushed_usr_tx;
  }
};

} // namespace cr
} // namespace leanstore
