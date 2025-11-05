#pragma once

#include <cstdint>

namespace leanstore {

/// Forward declarations
class LeanStore;
class Logging;
class TxManager;
class Coroutine;
class CoroExecutor;

/// Coroutine environment utilities.
class CoroEnv {
public:
  /// Maximum number of coroutines per thread.
  static constexpr int64_t kMaxCoroutinesPerThread = 256;
  /// Stack size for each coroutine in bytes, 8 MB by default.
  static constexpr int64_t kStackSize = 8 << 20;

  /// Sets the current LeanStore instance for the calling thread.
  static void SetCurStore(LeanStore* store);
  /// Gets the current LeanStore instance for the calling thread.
  static LeanStore* CurStore();

  /// Sets the current coroutine executor for the calling thread.
  static CoroExecutor* CurCoroExec();
  /// Gets the current coroutine for the calling thread.
  static Coroutine* CurCoro();

  /// Sets the current logging module for the calling thread.
  static void SetCurLogging(Logging* logging);
  /// Gets the current logging module for the calling thread.
  static Logging& CurLogging();

  /// Sets the current transaction manager for the calling thread.
  static void SetCurTxMgr(TxManager* tx_mgr);
  /// Gets the current transaction manager for the calling thread.
  static TxManager& CurTxMgr();
  /// Checks if the current thread has a transaction manager set.
  static bool HasTxMgr();
};

} // namespace leanstore