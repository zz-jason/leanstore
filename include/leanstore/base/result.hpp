#pragma once

#include "leanstore/base/error.hpp"

#include <cassert>
#include <expected>

namespace leanstore {

#ifdef DEBUG
#define LEAN_DCHECK_HAS_VALUE assert(has_value() && "No value present in Result");
#define LEAN_DCHECK_HAS_ERROR assert(!has_value() && "No error present in Result");
#else
#define LEAN_DCHECK_HAS_VALUE
#define LEAN_DCHECK_HAS_ERROR
#endif

template <typename T>
struct ResultStorageType {
  using type = T;
};

template <>
struct ResultStorageType<void> {
  using type = std::monostate;
};

/// A Result type that encapsulates either a value of type T or an Error.
/// All APIs are mimicked after std::expected.
template <typename T, typename E = Error>
class [[nodiscard]] Result {
public:
  /// Actual underlying std::expected type.
  using storage_t = typename ResultStorageType<T>::type;
  using result_t = std::expected<storage_t, E>;

  /// Construct from std::expected
  /// No explicit in order to allow implicit conversions.
  Result(result_t&& result) : result_(std::move(result)) { // NOLINT (google-explicit-constructor)
  }

  /// Construct an empty Result for void type.
  Result()
    requires std::is_void_v<T>
      : result_(std::expected<storage_t, E>{}) {
  }

  /// Construct a value Result. Moves the value.
  /// No explicit in order to allow implicit conversions.
  Result(storage_t&& v) : result_(std::move(v)) { // NOLINT (google-explicit-constructor)
  }

  /// Construct a value Result (const version). Copies the value.
  /// No explicit in order to allow implicit conversions.
  Result(const storage_t& v) : result_(v) { // NOLINT (google-explicit-constructor)
  }

  /// Construct an error Result.
  /// No explicit in order to allow implicit conversions.
  Result(E&& e) : result_(std::unexpected(std::move(e))) { // NOLINT (google-explicit-constructor)
  }

  /// Checks if the Result contains a value
  constexpr bool has_value() const noexcept { // NOLINT: mimicking std::expected
    return result_.has_value();
  }

  /// Operator bool to check if it has value
  explicit operator bool() const noexcept {
    return has_value();
  }

  /// Checks whether two Results are equal
  friend bool operator==(const Result& lhs, const Result& rhs) noexcept {
    return lhs.result_ == rhs.result_;
  }

  /// Checks whether two Results are not equal
  friend bool operator!=(const Result& lhs, const Result& rhs) noexcept {
    return !(lhs == rhs);
  }

  /// Gets the value.
  constexpr storage_t& value() & { // NOLINT: mimicking std::expected
    LEAN_DCHECK_HAS_VALUE;
    return result_.value();
  }

  /// Gets the value (const version).
  constexpr const storage_t& value() const& { // NOLINT: mimicking std::expected
    LEAN_DCHECK_HAS_VALUE;
    return result_.value();
  }

  /// Gets the error.
  constexpr E& error() & { // NOLINT: mimicking std::expected
    LEAN_DCHECK_HAS_ERROR;
    return result_.error();
  }

  /// Gets the error (const version).
  constexpr const E& error() const& { // NOLINT: mimicking std::expected
    LEAN_DCHECK_HAS_ERROR
    return result_.error();
  }

private:
  /// The underlying std::expected instance.
  result_t result_;
};

#undef LEAN_DCHECK_HAS_VALUE
#undef LEAN_DCHECK_HAS_ERROR

} // namespace leanstore