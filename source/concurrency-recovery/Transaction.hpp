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

enum class TX_ISOLATION_LEVEL : u8 {
  SERIALIZABLE = 3,
  SNAPSHOT_ISOLATION = 2,
  READ_COMMITTED = 1,
  READ_UNCOMMITTED = 0
};

inline TX_ISOLATION_LEVEL parseIsolationLevel(std::string str) {
  if (str == "ser") {
    return leanstore::TX_ISOLATION_LEVEL::SERIALIZABLE;
  } else if (str == "si") {
    return leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION;
  } else if (str == "rc") {
    return leanstore::TX_ISOLATION_LEVEL::READ_COMMITTED;
  } else if (str == "ru") {
    return leanstore::TX_ISOLATION_LEVEL::READ_UNCOMMITTED;
  } else {
    UNREACHABLE();
    return leanstore::TX_ISOLATION_LEVEL::READ_UNCOMMITTED;
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
  LID mMaxObservedGSN;

  /// mTxMode is the mode of the current transaction.
  TX_MODE mTxMode = TX_MODE::OLTP;

  /// mTxIsolationLevel is the isolation level for the current transaction.
  TX_ISOLATION_LEVEL mTxIsolationLevel = TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION;

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
    return mTxIsolationLevel >= TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION;
  }
  bool isSI() {
    return mTxIsolationLevel == TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION;
  }
  bool isReadCommitted() {
    return mTxIsolationLevel == TX_ISOLATION_LEVEL::READ_COMMITTED;
  }
  bool isReadUncommitted() {
    return mTxIsolationLevel == TX_ISOLATION_LEVEL::READ_UNCOMMITTED;
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

  void Start(TX_MODE mode, TX_ISOLATION_LEVEL level, bool isReadOnly) {
    stats.start = std::chrono::high_resolution_clock::now();
    if (!FLAGS_wal) {
      return;
    }

    mWalExceedBuffer = false;
    mHasWrote = false;
    mIsReadOnly = isReadOnly;
    mIsDurable = FLAGS_wal;
    mTxIsolationLevel = level;
    mTxMode = mode;
    state = TX_STATE::STARTED;
  }

  bool CanCommit(u64 minFlushedGSN, TXID minFlushedTxId) {
    return mMaxObservedGSN <= minFlushedGSN && mStartTs <= minFlushedTxId;
  }
};

} // namespace cr
} // namespace leanstore
