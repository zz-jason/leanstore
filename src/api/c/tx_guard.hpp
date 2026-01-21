#pragma once

#include "leanstore/c/status.h"
#include "leanstore/coro/coro_env.hpp"
#include "leanstore/tx/tx_manager.hpp"

namespace leanstore {

class TxGuard {
public:
  explicit TxGuard(lean_status& status) : status_(status) {
    implicit_tx_ = !CoroEnv::CurTxMgr().IsTxStarted();
    if (implicit_tx_) {
      CoroEnv::CurTxMgr().StartTx();
    }
  }

  ~TxGuard() {
    if (implicit_tx_) {
      if (status_ == lean_status::LEAN_STATUS_OK) {
        CoroEnv::CurTxMgr().CommitTx();
      } else {
        CoroEnv::CurTxMgr().AbortTx();
      }
    }
  }

private:
  bool implicit_tx_;
  lean_status& status_;
};

} // namespace leanstore