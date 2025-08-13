#pragma once

#include <cstdint>

namespace leanstore::cr {
class Logging;
class TxManager;
class Transaction;
} // namespace leanstore::cr

namespace leanstore {

class LeanStore;
class Coroutine;
class CoroExecutor;

class CoroEnv {
public:
  static constexpr int64_t kMaxCoroutinesPerThread = 256;
  static constexpr int64_t kStackSize = 8 << 20; // 8 MB

  static void SetCurStore(LeanStore* store);
  static LeanStore* CurStore();

  static CoroExecutor* CurCoroExec();
  static Coroutine* CurCoro();

  static void SetCurLogging(cr::Logging* logging);
  static cr::Logging& CurLogging();

  static void SetCurTxMgr(cr::TxManager* tx_mgr);
  static cr::TxManager& CurTxMgr();
  static bool HasTxMgr();
};

} // namespace leanstore