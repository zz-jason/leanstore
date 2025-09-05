#include "utils/coroutine/coro_env.hpp"

#include "leanstore/concurrency/logging.hpp"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/managed_thread.hpp"
#include "utils/coroutine/coro_executor.hpp"
#include "utils/coroutine/coro_scheduler.hpp"

namespace leanstore {

namespace {
thread_local LeanStore* tls_store = nullptr;
thread_local cr::Logging* tls_logging = nullptr;

#ifndef ENABLE_COROUTINE
thread_local cr::TxManager* tls_tx_mgr = nullptr;
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

void CoroEnv::SetCurLogging(cr::Logging* logging) {
  tls_logging = logging;
}

cr::Logging& CoroEnv::CurLogging() {
  LEAN_DCHECK(tls_logging != nullptr, "Current logging is not set");
  return *tls_logging;
}

void CoroEnv::SetCurTxMgr(cr::TxManager* tx_mgr) {
#if ENABLE_COROUTINE
  CoroEnv::CurCoro()->SetTxMgr(tx_mgr);
#else
  tls_tx_mgr = tx_mgr;
#endif
}

cr::TxManager& CoroEnv::CurTxMgr() {
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