#include "coroutine/coro_env.hpp"

#include "coroutine/coro_executor.hpp"
#include "coroutine/coro_scheduler.hpp"
#include "leanstore/concurrency/logging.hpp"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/managed_thread.hpp"

namespace leanstore {

namespace {
thread_local LeanStore* tls_store = nullptr;
thread_local Logging* tls_logging = nullptr;

#ifndef ENABLE_COROUTINE
thread_local TxManager* tls_tx_mgr = nullptr;
#endif
} // namespace

void CoroEnv::SetCurStore(LeanStore* store) {
  tls_store = store;
}

LeanStore* CoroEnv::CurStore() {
  return tls_store;
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
#if ENABLE_COROUTINE
  CoroEnv::CurCoro()->SetTxMgr(tx_mgr);
#else
  tls_tx_mgr = tx_mgr;
#endif
}

TxManager& CoroEnv::CurTxMgr() {
#if ENABLE_COROUTINE
  return *CurCoro()->GetTxMgr();
#else
  return *tls_tx_mgr;
#endif
}

bool CoroEnv::HasTxMgr() {
#if ENABLE_COROUTINE
  return CurCoroExec() != nullptr && CurCoro() != nullptr && CurCoro()->GetTxMgr() != nullptr;
#else
  return tls_tx_mgr != nullptr;
#endif
}

} // namespace leanstore