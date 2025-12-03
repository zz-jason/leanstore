#pragma once

namespace leanstore {

/// Scoped deferrer, runs the given function upon destruction when the object
/// goes out of scope.
template <class F>
struct ScopedDeferrer {
  ScopedDeferrer(F f) : func_(f) {
  }

  ~ScopedDeferrer() {
    func_();
  }

  ScopedDeferrer(const ScopedDeferrer&) = delete;
  ScopedDeferrer(ScopedDeferrer&&) = default;
  ScopedDeferrer& operator=(const ScopedDeferrer&) = delete;
  ScopedDeferrer& operator=(ScopedDeferrer&&) = delete;

  /// The function to be executed upon destruction.
  F func_;
};

/// Helper function to create a ScopedDeferrer.
template <typename F>
ScopedDeferrer<F> MakeScopedDeferrer(F f) {
  return ScopedDeferrer<F>(f);
}

#define LEAN_DEFER_INTERNAL_INTERNAL(LINE) defer_at_line##LINE
#define LEAN_DEFER_INTERNAL(LINE) LEAN_DEFER_INTERNAL_INTERNAL(LINE)
#define LEAN_DEFER(f)                                                                              \
  auto LEAN_DEFER_INTERNAL(__LINE__) = leanstore::MakeScopedDeferrer([&]() { f; });

} // namespace leanstore