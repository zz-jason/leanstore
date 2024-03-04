#pragma once

#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/Units.hpp"

#include <glog/logging.h>

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
  TxState mState = TxState::kIdle;

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
  TxMode mTxMode = TxMode::kShortRunning;

  /// mTxIsolationLevel is the isolation level for the current transaction.
  IsolationLevel mTxIsolationLevel = IsolationLevel::kSnapshotIsolation;

  /// Whether the transaction is assumed to be read-only. Read-only transactions
  /// should not have any data writes during the transaction processing.
  bool mIsReadOnly = false;

  /// Whether the transaction has any data writes. Transaction writes can be
  /// detected once it generates a WAL entry.
  bool mHasWrote = false;

  /// Whether the transaction is durable. A durable transaction can be committed
  /// or aborted only after all the WAL entries are flushed to disk.
  bool mIsDurable = true;

  bool mWalExceedBuffer = false;

public:
  inline bool IsLongRunning() {
    return mTxMode == TxMode::kLongRunning;
  }

  inline bool AtLeastSI() {
    return mTxIsolationLevel >= IsolationLevel::kSnapshotIsolation;
  }

  inline void MarkAsWrite() {
    DCHECK(mIsReadOnly == false);
    mHasWrote = true;
  }

  // Start a new transaction, initialize all fields
  inline void Start(TxMode mode, IsolationLevel level, bool isReadOnly) {
    mState = TxState::kStarted;
    mStartTs = 0;
    mCommitTs = 0;
    mMaxObservedGSN = 0;
    mTxMode = mode;
    mTxIsolationLevel = level;
    mIsReadOnly = isReadOnly;
    mHasWrote = false;
    mIsDurable = FLAGS_wal;
    mWalExceedBuffer = false;
  }

  inline bool CanCommit(uint64_t minFlushedGSN, TXID minFlushedTxId) {
    return mMaxObservedGSN <= minFlushedGSN && mStartTs <= minFlushedTxId;
  }
};

} // namespace cr
} // namespace leanstore
