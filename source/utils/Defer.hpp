#pragma once

#include <memory>

namespace leanstore {
namespace utils {

#ifndef SCOPED_DEFER

template <class F> struct ScopedDeferrer {
  F f;

  ScopedDeferrer(F f) : f(f) {
  }

  ~ScopedDeferrer() {
    f();
  }
};

template <typename F>
std::unique_ptr<ScopedDeferrer<F>> MakeScopedDeferrer(F f) {
  return std::make_unique<ScopedDeferrer<F>>(f);
}

#define SCOPED_DEFER_INTERNAL_INTERNAL(LINE) deferAtLine##LINE
#define SCOPED_DEFER_INTERNAL(LINE) SCOPED_DEFER_INTERNAL_INTERNAL(LINE)
#define SCOPED_DEFER(f)                                                        \
  auto SCOPED_DEFER_INTERNAL(__LINE__) =                                       \
      leanstore::utils::MakeScopedDeferrer([&]() { f; });

#endif // SCOPED_DEFER

} // namespace utils
} // namespace leanstore