#pragma once

#include "leanstore/cpp/base/error.hpp"

#include <cassert>
#include <expected>

namespace leanstore {

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
class Result {
public:
  /// Actual underlying std::expected type.
  using storage_t = typename ResultStorageType<T>::type;
  using result_t = std::expected<storage_t, E>;

  /// Construct from std::expected
  /// No explicit in order to allow implicit conversions.
  Result(result_t&& result) : result_(std::move(result)) {
  }

  /// Construct an empty Result for void type.
  Result()
    requires std::is_void_v<T>
      : result_(std::expected<storage_t, E>{}) {
  }

  /// Construct a value Result.
  /// No explicit in order to allow implicit conversions.
  Result(storage_t&& v) : result_(std::move(v)) {
  }

  /// Construct an error Result.
  /// No explicit in order to allow implicit conversions.
  Result(E&& e) : result_(std::unexpected(std::move(e))) {
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
  /// Asserts if there is no value present in debug mode.
  constexpr storage_t& value() & { // NOLINT: mimicking std::expected
#ifdef DEBUG
    if (!has_value()) {
      assert(false && "No value present in Result");
    }
#endif
    return result_.value();
  }

  /// Gets the value (const version).
  /// Asserts if there is no value present in debug mode.
  const constexpr storage_t& value() const& { // NOLINT: mimicking std::expected
#ifdef DEBUG
    if (!has_value()) {
      assert(false && "No value present in Result");
    }
#endif

    return result_.value();
  }

  /// Gets the error.
  /// Asserts if there is a value present in debug mode.
  constexpr E& error() & { // NOLINT: mimicking std::expected
#ifdef DEBUG
    if (has_value()) {
      assert(false && "No error present in Result");
    }
#endif

    return result_.error();
  }

  /// Gets the error (const version).
  const constexpr E& error() const& { // NOLINT: mimicking std::expected
#ifdef DEBUG
    if (has_value()) {
      assert(false && "No error present in Result");
    }
#endif

    return result_.error();
  }

private:
  /// The underlying std::expected instance.
  result_t result_;
};

} // namespace leanstore