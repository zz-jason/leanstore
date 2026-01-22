#pragma once

#include "leanstore/base/jump_mu.hpp"
#include "leanstore/coro/coro_env.hpp"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>

namespace leanstore {

/// Enum representing the state of a coroutine during its lifecycle.
enum class CoroState : uint8_t {
  kReady = 0,    // Ready to run, not started yet.
  kRunning,      // Running, not yielded yet.
  kWaitingMutex, // Waiting for mutex.
  kWaitingIo,    // Waiting for IO operation, read/write file, etc.
  kDone,         // Finished execution.
};

/// Coroutine class representing a task/job executed within a coroutine
/// executor. Each coroutine has its own execution context and can be yield and
/// resume during its lifecycle.
class Coroutine {
public:
  /// The task/job function type executed by the coroutine.
  using CoroFunc = std::function<void()>;

  /// Constructor initializing the coroutine with the given function.
  explicit Coroutine(CoroFunc&& func);

  /// Destructor - must manually destruct in-place Impl
  ~Coroutine();

  /// Disable copy and move semantics
  /// Move is complex with in-place storage, so we disable it for safety
  Coroutine(const Coroutine&) = delete;
  Coroutine& operator=(const Coroutine&) = delete;
  Coroutine(Coroutine&&) = delete;
  Coroutine& operator=(Coroutine&&) = delete;

  /// Runs the coroutine. If not started, it starts the coroutine; otherwise,
  /// it resumes the coroutine from its last yielded state.
  void Run() {
    if (!IsStarted()) {
      Start();
    } else {
      Resume();
    }
  }

  /// Starts the coroutine execution. Initializes the coroutine context and
  /// begins execution of the coroutine function.
  void Start();

  /// Resumes the coroutine from its current state.
  void Resume();

  /// Yields the coroutine, allowing it to be resumed later.
  /// Resume the sink process context to yield control back to the scheduler.
  void Yield(CoroState state);

  CoroState GetState() const {
    return state_;
  }

  void SetTxMgr(TxManager* tx_mgr) {
    tx_mgr_ = tx_mgr;
  }

  TxManager* GetTxMgr() const {
    return tx_mgr_;
  }

  void SetTryLockFunc(std::function<bool()> try_lock_func) {
    try_lock_func_ = std::move(try_lock_func);
  }

  void ClearTryLockFunc() {
    try_lock_func_ = nullptr;
  }

  bool TryLock() {
    assert(try_lock_func_ != nullptr);
    return try_lock_func_();
  }

  void IncWaitingIoReqs(int64_t inc = 1) {
    waiting_io_reqs_ += inc;
  }

  void DecWaitingIoReqs(int64_t dec = 1) {
    assert(waiting_io_reqs_ >= dec);
    waiting_io_reqs_ -= dec;
  }

  bool IsIoCompleted() {
    return waiting_io_reqs_ == 0;
  }

  bool IsStarted() const {
    return state_ != CoroState::kReady;
  }

  bool IsWaiting() const {
    return state_ > CoroState::kRunning && state_ <= CoroState::kWaitingIo;
  }

  bool IsDone() const {
    return state_ == CoroState::kDone;
  }

  JumpContext* GetJumpContext() {
    return &jump_context_;
  }

  /// In-place storage size constants (public for static_assert in .cpp)
  static constexpr size_t kImplSize = 160;
  static constexpr size_t kImplAlign = 16;

private:
  /// Forward declaration of implementation details (Pimpl idiom)
  /// Contains boost::context types to hide the dependency from public headers
  struct Impl;

  alignas(kImplAlign) std::byte impl_storage_[kImplSize];

  /// Accessor for type-safe access to impl storage
  Impl* GetImpl() {
    return reinterpret_cast<Impl*>(impl_storage_);
  }

  const Impl* GetImpl() const {
    return reinterpret_cast<const Impl*>(impl_storage_);
  }

  /// Function to be executed by the coroutine.
  /// This is a callable object that contains the logic of the coroutine.
  CoroFunc func_ = nullptr;

  /// Pointer to the transaction manager associated with this coroutine.
  TxManager* tx_mgr_ = nullptr;

  /// Try lock function for the coroutine.
  std::function<bool()> try_lock_func_ = nullptr;

  /// Number of IO requests that are currently waiting to be processed.
  /// Used to track the number of IO operations that are pending for this
  /// coroutine, allowing it to yield until the IO operations are complete.
  int64_t waiting_io_reqs_ = 0;

  /// Current state of the coroutine.
  CoroState state_ = CoroState::kReady;

  /// Jump context for the coroutine.
  JumpContext jump_context_;
};

} // namespace leanstore