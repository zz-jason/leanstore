#pragma once

#include <cassert>
#include <cstdint>

namespace leanstore {

/// Forward declarations
class TxManager;

/// Coroutine session encapsulating the execution context for a coroutine,
/// including its assigned executor and transaction manager.
///
/// Any application task running within a coroutine should have an associated
/// CoroSession.
class CoroSession {
public:
  /// Constructor initializing the coroutine session with the specified executor
  /// and transaction manager.
  CoroSession(uint64_t runs_on, TxManager* tx_mgr) : runs_on_(runs_on), tx_mgr_(tx_mgr) {
  }
  /// Destructor
  ~CoroSession() = default;

  /// Get the executor ID this coroutine runs on.
  uint64_t GetRunsOn() const {
    return runs_on_;
  }

  /// Get the transaction manager associated with this coroutine, which is
  /// responsible for managing transactions within the coroutine's context.
  TxManager* GetTxMgr() const {
    return tx_mgr_;
  }

private:
  /// Which coroutine executor this coroutine runs on.
  const uint64_t runs_on_ = 0;

  /// Pointer to the transaction manager associated with this coroutine.
  TxManager* const tx_mgr_ = nullptr;
};

} // namespace leanstore