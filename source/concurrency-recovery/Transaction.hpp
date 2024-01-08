#pragma once

#include "Exceptions.hpp"
#include "Units.hpp"

#include <glog/logging.h>

#include <chrono>

namespace leanstore {

enum class TX_MODE : u8 {
  OLAP = 0,
  OLTP = 1,
  DETERMINISTIC = 2,
  INSTANTLY_VISIBLE_BULK_INSERT = 3,
};

inline std::string ToString(TX_MODE txMode) {
  switch (txMode) {
  case TX_MODE::OLAP: {
    return "OLAP";
  }
  case TX_MODE::OLTP: {
    return "OLTP";
  }
  case TX_MODE::DETERMINISTIC: {
    return "DETERMINISTIC";
  }
  case TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT: {
    return "INSTANTLY_VISIBLE_BULK_INSERT";
  }
  }
  return "Unknown TX_MODE";
}

enum class IsolationLevel : u8 {
  SERIALIZABLE = 3,
  kSnapshotIsolation = 2,
  READ_COMMITTED = 1,
  READ_UNCOMMITTED = 0
};

inline IsolationLevel parseIsolationLevel(std::string str) {
  if (str == "ser") {
    return leanstore::IsolationLevel::SERIALIZABLE;
  } else if (str == "si") {
    return leanstore::IsolationLevel::kSnapshotIsolation;
  } else if (str == "rc") {
    return leanstore::IsolationLevel::READ_COMMITTED;
  } else if (str == "ru") {
    return leanstore::IsolationLevel::READ_UNCOMMITTED;
  } else {
    UNREACHABLE();
    return leanstore::IsolationLevel::READ_UNCOMMITTED;
  }
}

namespace cr {

enum class TX_STATE { IDLE, STARTED, READY_TO_COMMIT, COMMITTED, ABORTED };

struct TxStats {
  std::chrono::high_resolution_clock::time_point start;
  std::chrono::high_resolution_clock::time_point precommit;
  std::chrono::high_resolution_clock::time_point commit;
  u64 flushes_counter = 0;
};

struct Transaction {
  TX_STATE state = TX_STATE::IDLE;

  /// mStartTs is the start timestamp of the transaction. Also used as
  /// teansaction ID
  TXID mStartTs = 0;

  /// mCommitTs is the commit timestamp of the transaction.
  TXID mCommitTs = 0;

  /// mMaxObservedGSN is the maximum observed global sequence number during
  /// transaction processing. It's used to determine whether a transaction can
  /// be committed.
  LID mMaxObservedGSN = 0;

  /// mTxMode is the mode of the current transaction.
  TX_MODE mTxMode = TX_MODE::OLTP;

  /// mTxIsolationLevel is the isolation level for the current transaction.
  IsolationLevel mTxIsolationLevel = IsolationLevel::kSnapshotIsolation;

  bool mIsDurable = false;

  bool mIsReadOnly = false;

  bool mHasWrote = false;

  bool mWalExceedBuffer = false;

  TxStats stats;

  //---------------------------------------------------------------------------
  // Object Utils
  //---------------------------------------------------------------------------
  bool isOLAP() {
    return mTxMode == TX_MODE::OLAP;
  }

  bool isOLTP() {
    return mTxMode == TX_MODE::OLTP;
  }

  bool isReadOnly() {
    return mIsReadOnly;
  }

  bool hasWrote() {
    return mHasWrote;
  }
  bool isDurable() {
    return mIsDurable;
  }
  bool atLeastSI() {
    return mTxIsolationLevel >= IsolationLevel::kSnapshotIsolation;
  }
  bool isSI() {
    return mTxIsolationLevel == IsolationLevel::kSnapshotIsolation;
  }
  bool isReadCommitted() {
    return mTxIsolationLevel == IsolationLevel::READ_COMMITTED;
  }
  bool isReadUncommitted() {
    return mTxIsolationLevel == IsolationLevel::READ_UNCOMMITTED;
  }

  inline u64 startTS() {
    return mStartTs;
  }

  inline u64 commitTS() {
    return mCommitTs;
  }

  void markAsWrite() {
    DCHECK(isReadOnly() == false);
    mHasWrote = true;
  }

  // Start a new transaction, reset all fields used by previous transaction
  void Start(TX_MODE mode, IsolationLevel level, bool isReadOnly) {
    state = TX_STATE::STARTED;
    mStartTs = 0;
    mCommitTs = 0;
    mMaxObservedGSN = 0;
    mTxMode = mode;
    mTxIsolationLevel = level;
    mIsDurable = FLAGS_wal;
    mIsReadOnly = isReadOnly;
    mHasWrote = false;
    mWalExceedBuffer = false;

    stats.start = std::chrono::high_resolution_clock::now();
    stats.precommit = std::chrono::high_resolution_clock::time_point();
    stats.commit = std::chrono::high_resolution_clock::time_point();
    stats.flushes_counter = 0;
  }

  bool CanCommit(u64 minFlushedGSN, TXID minFlushedTxId) {
    return mMaxObservedGSN <= minFlushedGSN && mStartTs <= minFlushedTxId;
  }
};

} // namespace cr
} // namespace leanstore
