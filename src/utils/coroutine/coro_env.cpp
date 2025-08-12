#include "utils/coroutine/coro_env.hpp"

#include "leanstore/concurrency/tx_manager.hpp"
#include "utils/coroutine/coro_executor.hpp"
#include "utils/coroutine/coro_scheduler.hpp"

namespace leanstore {

Coroutine* CoroEnv::CurCoro() {
  return CoroExecutor::CurrentCoro();
}

CoroExecutor* CoroEnv::CurCoroExec() {
  return CoroExecutor::CurrentThread();
}

std::vector<std::unique_ptr<cr::TxManager>>& CoroEnv::AllWorkerCtxs() {
  return cr::TxManager::My().tx_mgrs_;
}

} // namespace leanstore