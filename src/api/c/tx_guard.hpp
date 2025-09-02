#pragma once

#include "leanstore/c/leanstore.h"
#include "leanstore/concurrency/tx_manager.hpp"
#include "utils/coroutine/coro_env.hpp"

namespace leanstore {

class TxGuard {
public:
  TxGuard(lean_status& status) : status_(status) {
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