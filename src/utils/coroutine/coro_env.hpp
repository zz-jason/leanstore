#pragma once

#include <cstdint>
#include <memory>
#include <vector>

namespace leanstore::cr {
class Logging;
class TxManager;
} // namespace leanstore::cr

namespace leanstore {

class Coroutine;
class CoroExecutor;

class CoroEnv {
public:
  static constexpr int64_t kMaxCoroutinesPerThread = 256;
  static constexpr int64_t kStackSize = 8 << 20; // 8 MB

  static Coroutine* CurCoro();
  static CoroExecutor* CurCoroExec();
  static std::vector<std::unique_ptr<cr::TxManager>>& AllWorkerCtxs();
};

} // namespace leanstore