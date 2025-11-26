#pragma once

#include <optional>
#include <string_view>

namespace leanstore {

/// Trait struct for enum types to provide string conversion and integer
/// mapping. Each enum type must specialize this struct.
template <typename E>
struct EnumTraits {
  static std::string_view ToString(E) {
    static_assert(sizeof(E) == 0, "EnumTraits not specialized for this enum type");
    return {};
  }

  static std::optional<E> FromString(std::string_view) {
    static_assert(sizeof(E) == 0, "EnumTraits not specialized for this enum type");
    return {};
  }
};

/// Concept to ensure that EnumTraits specialization provides required methods
/// for a given enum type E.
template <typename E>
concept EnumTraitsRequired = requires(E e) {
  { EnumTraits<E>::ToString(e) } -> std::same_as<std::string_view>;
  { EnumTraits<E>::FromString(std::string_view{}) } -> std::same_as<std::optional<E>>;
};

} // namespace leanstore