#pragma once

#include "leanstore/Units.hpp"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>

using UInteger = uint32_t;
using Integer = int32_t;
using Timestamp = int64_t;
using Numeric = double;
static constexpr Integer minUInteger = std::numeric_limits<UInteger>::min();
static constexpr Integer minInteger = std::numeric_limits<Integer>::min();
// -------------------------------------------------------------------------------------
template <int maxLength>
struct Varchar {
  int16_t length;
  char data[maxLength] = {0}; // not '\0' terminated

  Varchar() : length(0) {
  }
  Varchar(const char* str) {
    int l = strlen(str);
    assert(l <= maxLength);
    length = l;
    memcpy(data, str, l);
  }
  template <int otherMaxLength>
  Varchar(const Varchar<otherMaxLength>& other) {
    assert(other.length <= maxLength);
    length = other.length;
    memcpy(data, other.data, length);
  }

  void Append(char x) {
    assert(length < maxLength);
    data[length++] = x;
  };

  std::string toString() const {
    return std::string(data, length);
  };

  template <int otherMaxLength>
  Varchar<maxLength> operator||(const Varchar<otherMaxLength>& other) const {
    Varchar<maxLength> tmp;
    assert((static_cast<int32_t>(length) + other.length) <= maxLength);
    tmp.length = length + other.length;
    memcpy(tmp.data, data, length);
    memcpy(tmp.data + length, other.data, other.length);
    return tmp;
  }

  bool operator==(const Varchar<maxLength>& other) const {
    return (length == other.length) && (memcmp(data, other.data, length) == 0);
  }

  bool operator!=(const Varchar<maxLength>& other) const {
    return !operator==(other);
  }

  bool operator<(const Varchar<maxLength>& other) const {
    int cmp = memcmp(data, other.data, (length < other.length) ? length : other.length);
    if (cmp)
      return cmp < 0;
    else
      return length < other.length;
  }
};
// -------------------------------------------------------------------------------------
// Fold functions convert integers to a lexicographical comparable format
unsigned Fold(uint8_t* writer, const Integer& x) {
  *reinterpret_cast<uint32_t*>(writer) = __builtin_bswap32(x ^ (1ul << 31));
  return sizeof(x);
}
// -------------------------------------------------------------------------------------
unsigned Fold(uint8_t* writer, const Timestamp& x) {
  *reinterpret_cast<uint64_t*>(writer) = __builtin_bswap64(x ^ (1ull << 63));
  return sizeof(x);
}
// -------------------------------------------------------------------------------------
unsigned Fold(uint8_t* writer, const uint32_t& x) {
  *reinterpret_cast<uint32_t*>(writer) = __builtin_bswap32(x);
  return sizeof(x);
}
// -------------------------------------------------------------------------------------
unsigned Fold(uint8_t* writer, const uint64_t& x) {
  *reinterpret_cast<uint64_t*>(writer) = __builtin_bswap64(x);
  return sizeof(x);
}
// -------------------------------------------------------------------------------------
template <int len>
unsigned Fold(uint8_t* writer, const Varchar<len>& x) {
  memcpy(writer, x.data, x.length);
  writer[x.length] = 0;
  return x.length + 1;
}
// -------------------------------------------------------------------------------------
unsigned Unfold(const uint8_t* input, Integer& x) {
  x = __builtin_bswap32(*reinterpret_cast<const uint32_t*>(input)) ^ (1ul << 31);
  return sizeof(x);
}
// -------------------------------------------------------------------------------------
unsigned Unfold(const uint8_t* input, Timestamp& x) {
  x = __builtin_bswap64(*reinterpret_cast<const uint64_t*>(input)) ^ (1ul << 63);
  return sizeof(x);
}
// -------------------------------------------------------------------------------------
unsigned Unfold(const uint8_t* input, uint32_t& x) {
  x = __builtin_bswap32(*reinterpret_cast<const uint32_t*>(input));
  return sizeof(x);
}
// -------------------------------------------------------------------------------------
unsigned Unfold(const uint8_t* input, uint64_t& x) {
  x = __builtin_bswap64(*reinterpret_cast<const uint64_t*>(input));
  return sizeof(x);
}
// -------------------------------------------------------------------------------------
template <int len>
unsigned Unfold(const uint8_t* input, Varchar<len>& x) {
  int l = strlen(reinterpret_cast<const char*>(input));
  assert(l <= len);
  memcpy(x.data, input, l);
  x.length = l;
  return l + 1;
}
