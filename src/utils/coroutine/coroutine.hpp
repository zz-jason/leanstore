#pragma once

#include <cassert>
#include <cstdint>
#include <functional>

#define BOOST_NAMESPACE leanstore::boost
#include <boost/context/continuation.hpp>
#include <boost/context/continuation_fcontext.hpp>
#undef BOOST_NAMESPACE

#include "leanstore/utils/jump_mu.hpp"

namespace leanstore {

enum class CoroState : uint8_t {
  kReady = 0,          // Ready to run, not started yet.
  kRunning,            // Running, not yielded yet.
  kWaitingBufferFrame, // Waiting for buffer frame.
  kWaitingMutex,       // Waiting for mutex.
  kWaitingJumpLock,    // Waiting for jump lock.
  kWaitingIo,          // Waiting for IO operation, read/write file, etc.
  kDone,               // Finished execution.
};

class Coroutine {
public:
  constexpr static int64_t kStackSize = 8 << 20; // 8 MB stack size
  using CoroFunc = std::function<void()>;

  Coroutine(CoroFunc func) : func_(std::move(func)) {
  }
  ~Coroutine() = default;

  Coroutine(const Coroutine&) = delete;
  Coroutine& operator=(const Coroutine&) = delete;
  Coroutine(Coroutine&&) = default;
  Coroutine& operator=(Coroutine&&) = default;

  void Run() {
    if (!IsStarted()) {
      Start();
    } else {
      Resume();
    }
  }

  /// Executes the coroutine function.
  void Start() {
    assert(!IsStarted());
    context_ =
        boost::context::callcc(std::allocator_arg, boost::context::fixedsize_stack(kStackSize),
                               [this](boost::context::continuation&& sink) {
                                 sink_context_ = std::move(sink);
                                 state_ = CoroState::kRunning;
                                 func_();
                                 state_ = CoroState::kDone;
                                 return std::move(sink_context_);
                               });
  }

  /// Resumes the coroutine from its current state.
  void Resume() {
    assert(IsStarted());
    state_ = CoroState::kRunning;

    // Resume the coroutine context.
    context_ = context_.resume();
  }

  /// Yields the coroutine, allowing it to be resumed later.
  /// Resume the sink process context to yield control back to the scheduler.
  void Yield(CoroState state) {
    assert(IsStarted());
    state_ = state;
    sink_context_ = sink_context_.resume();
  }

  CoroState GetState() const {
    return state_;
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
    return state_ >= CoroState::kWaitingBufferFrame && state_ <= CoroState::kWaitingIo;
  }

  bool IsDone() const {
    return state_ == CoroState::kDone;
  }

  JumpContext* GetJumpContext() {
    return &jump_context_;
  }

private:
  /// Continuation for the coroutine's execution context.
  boost::context::continuation context_;

  /// Continuation for the coroutine's sink(caller) context.
  /// This is a pointer to the sink continuation that will be resumed when it's
  /// ready to continue. Used to manage the coroutine's execution flow.
  boost::context::continuation sink_context_;

  /// Function to be executed by the coroutine.
  /// This is a callable object that contains the logic of the coroutine.
  CoroFunc func_ = nullptr;

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