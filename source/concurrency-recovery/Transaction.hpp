#pragma once

#include "shared-headers/Exceptions.hpp"
#include "shared-headers/Units.hpp"

#include <glog/logging.h>

#include <chrono>

namespace leanstore {

enum class TxMode : u8 {
  kOLAP = 0,
  kOLTP = 1,
  kDeterministic = 2,
  kInstantlyVisibleBulkInsert = 3,
};

inline std::string ToString(TxMode txMode) {
  switch (txMode) {
  case TxMode::kOLAP: {
    return "OLAP";
  }
  case TxMode::kOLTP: {
    return "OLTP";
  }
  case TxMode::kDeterministic: {
    return "Deterministic";
  }
  case TxMode::kInstantlyVisibleBulkInsert: {
    return "InstantlyVisibleBulkInsert";
  }
  }
  return "Unknown TxMode";
}

enum class IsolationLevel : u8 {
  // kReadUnCommitted = 0,
  // kReadCommitted = 1,
  kSnapshotIsolation = 2,
  kSerializable = 3,
};

inline IsolationLevel ParseIsolationLevel(std::string str) {
  if (str == "ser") {
    return leanstore::IsolationLevel::kSerializable;
  }
  if (str == "si") {
    return leanstore::IsolationLevel::kSnapshotIsolation;
  }
  return leanstore::IsolationLevel::kSnapshotIsolation;
}

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

struct TxStats {
  std::chrono::high_resolution_clock::time_point start;
  std::chrono::high_resolution_clock::time_point precommit;
  std::chrono::high_resolution_clock::time_point commit;
};

class Transaction {
public:
  /// The state of the current transaction.
  TxState state = TxState::kIdle;

  /// mStartTs is the start timestamp of the transaction. Also used as
  /// teansaction ID.
  TXID mStartTs = 0;

  /// mCommitTs is the commit timestamp of the transaction.
  TXID mCommitTs = 0;

  /// mMaxObservedGSN is the maximum observed global sequence number during
  /// transaction processing. It's used to determine whether a transaction can
  /// be committed.
  LID mMaxObservedGSN = 0;

  /// mTxMode is the mode of the current transaction.
  TxMode mTxMode = TxMode::kOLTP;

  /// mTxIsolationLevel is the isolation level for the current transaction.
  IsolationLevel mTxIsolationLevel = IsolationLevel::kSnapshotIsolation;

  /// Whether the transaction is assumed to be read-only. Read-only transactions
  /// should not have any data writes during the transaction processing.
  bool mIsReadOnly = false;

  /// Whether the transaction has any data writes. Transaction writes can be
  /// detected once it generates a WAL entry.
  bool mHasWrote = false;

  bool mWalExceedBuffer = false;

  TxStats stats;

public:
  bool IsOLAP() {
    return mTxMode == TxMode::kOLAP;
  }

  bool IsOLTP() {
    return mTxMode == TxMode::kOLTP;
  }

  bool AtLeastSI() {
    return mTxIsolationLevel >= IsolationLevel::kSnapshotIsolation;
  }

  void MarkAsWrite() {
    DCHECK(mIsReadOnly == false);
    mHasWrote = true;
  }

  // Start a new transaction, reset all fields used by previous transaction
  void Start(TxMode mode, IsolationLevel level, bool isReadOnly) {
    state = TxState::kStarted;
    mStartTs = 0;
    mCommitTs = 0;
    mMaxObservedGSN = 0;
    mTxMode = mode;
    mTxIsolationLevel = level;
    mIsReadOnly = isReadOnly;
    mHasWrote = false;
    mWalExceedBuffer = false;

    COUNTERS_BLOCK() {
      stats.start = std::chrono::high_resolution_clock::now();
      stats.precommit = std::chrono::high_resolution_clock::time_point();
      stats.commit = std::chrono::high_resolution_clock::time_point();
    }
  }

  bool CanCommit(u64 minFlushedGSN, TXID minFlushedTxId) {
    return mMaxObservedGSN <= minFlushedGSN && mStartTs <= minFlushedTxId;
  }
};

} // namespace cr
} // namespace leanstore
