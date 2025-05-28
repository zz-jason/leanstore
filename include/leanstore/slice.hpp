#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace leanstore {

class Slice : public std::basic_string_view<uint8_t> {
public:
  Slice() : std::basic_string_view<uint8_t>() {
  }

  Slice(const std::string& str)
      : std::basic_string_view<uint8_t>(reinterpret_cast<const uint8_t*>(str.data()), str.size()) {
  }

  Slice(const std::string_view& str)
      : std::basic_string_view<uint8_t>(reinterpret_cast<const uint8_t*>(str.data()), str.size()) {
  }

  Slice(const std::basic_string<uint8_t>& str)
      : std::basic_string_view<uint8_t>(str.data(), str.size()) {
  }

  Slice(const uint8_t* data, size_t size) : std::basic_string_view<uint8_t>(data, size) {
  }

  Slice(const char* data)
      : std::basic_string_view<uint8_t>(reinterpret_cast<const uint8_t*>(data), std::strlen(data)) {
  }

  Slice(const char* data, size_t size)
      : std::basic_string_view<uint8_t>(reinterpret_cast<const uint8_t*>(data), size) {
  }

  std::string ToString() const {
    return std::string(reinterpret_cast<const char*>(data()), size());
  }

  void CopyTo(std::string& dest) const {
    dest.resize(size());
    std::memcpy(dest.data(), data(), size());
  }
};

class MutableSlice {
private:
  uint8_t* data_;
  uint64_t size_;

public:
  MutableSlice(uint8_t* ptr, uint64_t len) : data_(ptr), size_(len) {
  }

  uint8_t* Data() {
    return data_;
  }

  uint64_t Size() {
    return size_;
  }

  Slice Immutable() {
    return Slice(data_, size_);
  }
};

inline std::string ToString(Slice slice) {
  return std::string(reinterpret_cast<const char*>(slice.data()), slice.size());
}

inline Slice ToSlice(const std::string& str) {
  return Slice(reinterpret_cast<const uint8_t*>(str.data()), str.size());
}

} // namespace leanstore
