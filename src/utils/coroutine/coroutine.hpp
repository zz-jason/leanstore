#pragma once

#include <cassert>
#include <cstdint>
#include <functional>

#define BOOST_NAMESPACE leanstore::boost
#include <boost/context/continuation.hpp>
#include <boost/context/continuation_fcontext.hpp>
#undef BOOST_NAMESPACE

#include "utils/coroutine/coro_mutex.hpp"

namespace leanstore {

enum class CoroState : uint8_t {
  kReady = 0,          // Ready to run, not started yet.
  kRunning,            // Running, not yielded yet.
  kWaitingBufferFrame, // Waiting for buffer frame.
  kWaitingMutex,       // Waiting for lock.
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
    assert(IsWaiting());

    sink_context_ = sink_context_.resume();
  }

  CoroState GetState() const {
    return state_;
  }

  void SetWaitingMutex(CoroMutex* mutex) {
    waiting_mutex_ = mutex;
  }

  CoroMutex* GetWaitingMutex() const {
    return waiting_mutex_;
  }

  void SetWorkerId(int64_t worker_id) {
    worker_id_ = worker_id;
  }

  int64_t GetWorkerId() const {
    return worker_id_;
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

  /// Worker ID, or thread id, this coroutine is runnning on.
  int64_t worker_id_ = -1;

  /// Pointer to the mutex this coroutine is waiting on, if any. It's used to
  /// avoid deadlocks and manage coroutine synchronization.
  CoroMutex* waiting_mutex_ = nullptr;

  /// Current state of the coroutine.
  CoroState state_ = CoroState::kReady;
};

} // namespace leanstore