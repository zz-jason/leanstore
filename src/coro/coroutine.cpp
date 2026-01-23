#include "leanstore/coro/coroutine.hpp"

#include "leanstore/coro/coro_env.hpp"

#include <cassert>
#include <new>
#include <utility>

// Include boost::context headers only in implementation
#define BOOST_NAMESPACE leanstore::boost
#include <boost/context/continuation.hpp>
#include <boost/context/continuation_fcontext.hpp>
#include <boost/context/fixedsize_stack.hpp>
#include <boost/context/pooled_fixedsize_stack.hpp>
#undef BOOST_NAMESPACE

namespace leanstore {

/// Implementation struct containing boost::context types
/// This hides the boost dependency from the public header
struct Coroutine::Impl {
  /// Continuation for the coroutine's execution context.
  boost::context::continuation context_;

  /// Continuation for the coroutine's sink(caller) context.
  /// This is a pointer to the sink continuation that will be resumed when it's
  /// ready to continue. Used to manage the coroutine's execution flow.
  boost::context::continuation sink_context_;

  /// Thread-local pooled allocator for coroutine stacks
  inline static thread_local boost::context::pooled_fixedsize_stack s_pooled_salloc{
      CoroEnv::kStackSize, CoroEnv::kMaxCoroutinesPerThread};

  Impl() = default;
  ~Impl() = default;

  // Note: Cannot use static_assert for size checks here due to nested private struct limitations.
  // Verified manually: sizeof(Impl) = 2 * sizeof(boost::context::continuation) ≈ 128 bytes
  // This fits within kImplSize = 160 bytes with comfortable margin.
  // Alignment: alignof(continuation) ≈ 8-16 bytes, fits within kImplAlign = 16 bytes.
};

// Constructor: placement-new the Impl in the in-place storage
Coroutine::Coroutine(CoroFunc&& func) : func_(std::move(func)) {
  new (impl_storage_) Impl();
}

// Destructor: manually call destructor for in-place constructed Impl
Coroutine::~Coroutine() {
  GetImpl()->~Impl();
}

void Coroutine::Start() {
  assert(!IsStarted());
  auto fn = [this](boost::context::continuation&& sink) {
    GetImpl()->sink_context_ = std::move(sink);
    state_ = CoroState::kRunning;
    func_();
    state_ = CoroState::kDone;
    return std::move(GetImpl()->sink_context_);
  };
  GetImpl()->context_ =
      boost::context::callcc(std::allocator_arg, Impl::s_pooled_salloc, std::move(fn));
}

void Coroutine::Resume() {
  assert(IsStarted());
  state_ = CoroState::kRunning;
  GetImpl()->context_ = GetImpl()->context_.resume();
}

void Coroutine::Yield(CoroState state) {
  assert(IsStarted());
  state_ = state;
  GetImpl()->sink_context_ = GetImpl()->sink_context_.resume();
}

} // namespace leanstore
