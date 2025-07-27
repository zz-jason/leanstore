#include "utils/coroutine/coro_env.hpp"

#include "utils/coroutine/coro_executor.hpp"

namespace leanstore {

Coroutine* CoroEnv::CurCoro() {
  return CoroExecutor::CurrentCoro();
}

CoroExecutor* CoroEnv::CurCoroExec() {
  return CoroExecutor::CurrentThread();
}

// std::unique_ptr<cr::Logging>& CoroEnv::CurWalWriter() {
//   return CoroExecutor::CurrentThread()->WalWriter();
// }

} // namespace leanstore