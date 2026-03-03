#pragma once

#include "leanstore/base/error.hpp"

#include <cassert>
#include <optional>

namespace leanstore {

#ifdef DEBUG
#define LEAN_DCHECK_HAS_VALUE assert(has_value() && "No value present in Optional");
#else
#define LEAN_DCHECK_HAS_VALUE
#endif

/// A concept constraining the value type of Optional. Rationale:
/// - Optional<void> has no well-defined value semantics.
/// - Optional<Error> is an anti-pattern: Error represents failure in the
///   control flow, not the absence of a value.
template <typename T>
concept OptionalValue = !std::is_void_v<T> && !std::same_as<std::remove_cvref_t<T>, Error>;

/// A simple Optional type that encapsulates an optional value of type T.
template <OptionalValue T>
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

  Optional& operator=(std::nullopt_t) {
    opt_ = std::nullopt;
    return *this;
  }

  Optional& operator=(T&& v) {
    opt_ = std::move(v);
    return *this;
  }

  Optional& operator=(const T& v) {
    opt_ = v;
    return *this;
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

  constexpr T& operator*() & {
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

  constexpr T* operator->() & {
    LEAN_DCHECK_HAS_VALUE;
    return &opt_.value();
  }

  constexpr bool operator==(const Optional& rhs) const {
    return opt_ == rhs.opt_;
  }

  constexpr bool operator!=(const Optional& rhs) const {
    return !(*this == rhs);
  }

private:
  optional_t opt_;
};

#undef LEAN_DCHECK_HAS_VALUE

} // namespace leanstore
