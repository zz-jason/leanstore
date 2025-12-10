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

  /// No copy and assign
  Optional(const Optional&) = delete;
  Optional& operator=(const Optional&) = delete;

  /// Move constructor
  Optional(Optional&& other) noexcept {
    // call move assignment
    *this = std::move(other);
  }

  /// Move assignment
  Optional& operator=(Optional&& other) noexcept {
    if (this != &other) {
      opt_ = std::move(other.opt_);
      other.opt_ = std::nullopt;
    }
    return *this;
  }

  /// Construct from std::optional
  Optional(optional_t&& opt) : opt_(std::move(opt)) { // NOLINT (google-explicit-constructor)
  }

  /// Construct an empty Optional
  Optional() : opt_(std::nullopt) {
  }

  /// Construct an empty Optional from std::nullopt
  Optional(std::nullopt_t) : opt_(std::nullopt) { // NOLINT (google-explicit-constructor)
  }

  /// Construct a value Optional
  Optional(T&& v) : opt_(std::move(v)) { // NOLINT (google-explicit-constructor)
  }

  /// Construct a value Optional from const reference
  Optional(const T& v) : opt_(v) { // NOLINT (google-explicit-constructor)
  }

  /// Checks if the Optional contains a value
  constexpr bool has_value() const noexcept { // NOLINT: mimicking std::optional
    return opt_.has_value();
  }

  /// Operator bool to check if it has value
  explicit operator bool() const noexcept {
    return has_value();
  }

  constexpr T& value() & { // NOLINT: mimicking std::optional
    LEAN_DCHECK_HAS_VALUE;
    return opt_.value();
  }

  constexpr const T& value() const& { // NOLINT: mimicking std::optional
    LEAN_DCHECK_HAS_VALUE;
    return opt_.value();
  }

  constexpr const T& operator*() const& {
    LEAN_DCHECK_HAS_VALUE;
    return opt_.value();
  }

  constexpr const T* operator->() const& {
    LEAN_DCHECK_HAS_VALUE;
    return &opt_.value();
  }

private:
  optional_t opt_;
};

#undef LEAN_DCHECK_HAS_VALUE

} // namespace leanstore