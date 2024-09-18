#pragma once

#include "leanstore/KVInterface.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "leanstore/utils/UserThread.hpp"

namespace leanstore::cr {

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
  //! The state of the current transaction.
  TxState mState = TxState::kIdle;

  //! mStartTs is the start timestamp of the transaction. Also used as
  //! teansaction ID.
  TXID mStartTs = 0;

  //! mCommitTs is the commit timestamp of the transaction.
  TXID mCommitTs = 0;

  //! Maximum observed system transaction id during transaction processing. Used to track
  //! transaction dependencies.
  TXID mDependentSysTx = 0;

  //! Whether the transaction has any remote dependencies. Currently, we only support SI isolation
  //! level, a user transaction can only depend on a system transaction executed in a remote worker
  //! thread.
  bool mHasRemoteDependency = false;

  //! mTxMode is the mode of the current transaction.
  TxMode mTxMode = TxMode::kShortRunning;

  //! mTxIsolationLevel is the isolation level for the current transaction.
  IsolationLevel mTxIsolationLevel = IsolationLevel::kSnapshotIsolation;

  //! Whether the transaction has any data writes. Transaction writes can be
  //! detected once it generates a WAL entry.
  bool mHasWrote = false;

  //! Whether the transaction is durable. A durable transaction can be committed
  //! or aborted only after all the WAL entries are flushed to disk.
  bool mIsDurable = true;

  bool mWalExceedBuffer = false;

public:
  bool IsLongRunning() {
    return mTxMode == TxMode::kLongRunning;
  }

  bool AtLeastSI() {
    return mTxIsolationLevel >= IsolationLevel::kSnapshotIsolation;
  }

  // Start a new transaction, initialize all fields
  void Start(TxMode mode, IsolationLevel level) {
    mState = TxState::kStarted;
    mStartTs = 0;
    mCommitTs = 0;
    mDependentSysTx = 0;
    mHasRemoteDependency = false;
    mTxMode = mode;
    mTxIsolationLevel = level;
    mHasWrote = false;
    mIsDurable = utils::tlsStore->mStoreOption->mEnableWal;
    mWalExceedBuffer = false;
  }
};

} // namespace leanstore::cr
