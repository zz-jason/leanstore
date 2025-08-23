#pragma once

#include <cassert>
#include <cstdint>

namespace leanstore::cr {
class TxManager;
} // namespace leanstore::cr

namespace leanstore {

class CoroSession {
public:
  CoroSession(uint64_t runs_on, cr::TxManager* tx_mgr) : runs_on_(runs_on), tx_mgr_(tx_mgr) {
  }
  ~CoroSession() = default;

  uint64_t GetRunsOn() const {
    return runs_on_;
  }

  cr::TxManager* GetTxMgr() const {
    return tx_mgr_;
  }

private:
  /// Which coroutine executor this coroutine runs on.
  const uint64_t runs_on_ = 0;

  /// Pointer to the transaction manager associated with this coroutine.
  cr::TxManager* const tx_mgr_ = nullptr;
};

} // namespace leanstore