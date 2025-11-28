#pragma once

#include <cassert>
#include <optional>

namespace leanstore {

#ifdef DEBUG
#define LEAN_DCHECK_HAS_VALUE assert(has_value() && "No value present in Optional");
#else
#define LEAN_DCHECK_HAS_VALUE
#endif

/// A simple Optional type that encapsulates an optional value of type T.
/// Optional<void> is not supported.
template <typename T>
  requires(!std::is_void_v<T>)
class [[nodiscard]] Optional {
public:
  using optional_t = std::optional<T>;

  /// Construct from std::optional
  Optional(optional_t&& opt) : opt_(std::move(opt)) {
  }

  /// Construct an empty Optional
  Optional() : opt_(std::nullopt) {
  }

  /// Construct an empty Optional from std::nullopt
  Optional(std::nullopt_t) : opt_(std::nullopt) {
  }

  /// Construct a value Optional
  Optional(T&& v) : opt_(std::move(v)) {
  }

  /// Construct a value Optional from const reference
  Optional(const T& v) : opt_(v) {
  }

  /// Checks if the Optional contains a value
  constexpr bool has_value() const noexcept { // NOLINT: mimicking std::optional
    return opt_.has_value();
  }

  /// Operator bool to check if it has value
  explicit operator bool() const noexcept {
    return has_value();
  }

  constexpr const T& operator*() const& { // NOLINT: mimicking std::optional
    LEAN_DCHECK_HAS_VALUE;
    return *opt_;
  }

  constexpr const T* operator->() const& { // NOLINT: mimicking std::optional
    LEAN_DCHECK_HAS_VALUE;
    return opt_.operator->();
  }

private:
  optional_t opt_;
};

#undef LEAN_DCHECK_HAS_VALUE

} // namespace leanstore