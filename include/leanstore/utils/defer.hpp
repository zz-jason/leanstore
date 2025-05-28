#pragma once

namespace leanstore {
namespace utils {

#ifndef SCOPED_DEFER

template <class F>
struct ScopedDeferrer {
  F func_;

  ScopedDeferrer(F f) : func_(f) {
  }

  ~ScopedDeferrer() {
    func_();
  }

  ScopedDeferrer(const ScopedDeferrer&) = delete;
  ScopedDeferrer(ScopedDeferrer&&) = default;
  ScopedDeferrer& operator=(const ScopedDeferrer&) = delete;
  ScopedDeferrer& operator=(ScopedDeferrer&&) = delete;
};

template <typename F>
ScopedDeferrer<F> MakeScopedDeferrer(F f) {
  return ScopedDeferrer<F>(f);
}

#define SCOPED_DEFER_INTERNAL_INTERNAL(LINE) defer_at_line##LINE
#define SCOPED_DEFER_INTERNAL(LINE) SCOPED_DEFER_INTERNAL_INTERNAL(LINE)
#define SCOPED_DEFER(f)                                                                            \
  auto SCOPED_DEFER_INTERNAL(__LINE__) = leanstore::utils::MakeScopedDeferrer([&]() { f; });

#endif // SCOPED_DEFER

} // namespace utils
} // namespace leanstore