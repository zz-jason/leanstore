#include "leanstore/coro/coro_env.hpp"

#include "leanstore/base/log.hpp"
#include "leanstore/coro/coro_executor.hpp"
#include "leanstore/coro/coro_scheduler.hpp"
#include "leanstore/tx/tx_manager.hpp"
#include "leanstore/utils/managed_thread.hpp"
#include "leanstore/wal/logging.hpp"

namespace leanstore {

namespace {
thread_local LeanStore* tls_store = nullptr;
thread_local Logging* tls_logging = nullptr;
} // namespace

void CoroEnv::SetCurStore(LeanStore* store) {
  tls_store = store;
}

LeanStore& CoroEnv::CurStore() {
  LEAN_DCHECK(tls_store != nullptr, "Current store is not set");
  return *tls_store;
}

CoroExecutor* CoroEnv::CurCoroExec() {
  return CoroExecutor::CurrentThread();
}

Coroutine* CoroEnv::CurCoro() {
  if (CoroExecutor::CurrentThread() == nullptr) {
    return nullptr;
  }
  return CoroExecutor::CurrentCoro();
}

void CoroEnv::SetCurLogging(Logging* logging) {
  tls_logging = logging;
}

Logging& CoroEnv::CurLogging() {
  LEAN_DCHECK(tls_logging != nullptr, "Current logging is not set");
  return *tls_logging;
}

void CoroEnv::SetCurTxMgr(TxManager* tx_mgr) {
  CoroEnv::CurCoro()->SetTxMgr(tx_mgr);
}

TxManager& CoroEnv::CurTxMgr() {
  return *CurCoro()->GetTxMgr();
}

bool CoroEnv::HasTxMgr() {
  return CurCoroExec() != nullptr && CurCoro() != nullptr && CurCoro()->GetTxMgr() != nullptr;
}

} // namespace leanstore
