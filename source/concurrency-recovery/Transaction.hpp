#pragma once

#include "Config.hpp"
#include "Exceptions.hpp"
#include "Units.hpp"

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

inline IsolationLevel parseIsolationLevel(std::string str) {
  if (str == "ser") {
    return leanstore::IsolationLevel::kSerializable;
  }
  if (str == "si") {
    return leanstore::IsolationLevel::kSnapshotIsolation;
  }
  return leanstore::IsolationLevel::kSnapshotIsolation;
}

namespace cr {

enum class TxState { kIdle, kStarted, kReadyToCommit, kCommitted, kAborted };

struct TxStats {
  std::chrono::high_resolution_clock::time_point start;
  std::chrono::high_resolution_clock::time_point precommit;
  std::chrono::high_resolution_clock::time_point commit;
  u64 flushes_counter = 0;
};

struct Transaction {
  TxState state = TxState::kIdle;

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
  TxMode mTxMode = TxMode::kOLTP;

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
    return mTxMode == TxMode::kOLAP;
  }

  bool isOLTP() {
    return mTxMode == TxMode::kOLTP;
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
  void Start(TxMode mode, IsolationLevel level, bool isReadOnly) {
    state = TxState::kStarted;
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
