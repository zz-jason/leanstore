#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace leanstore {

class Slice : public std::basic_string_view<uint8_t> {
public:
  Slice() : std::basic_string_view<uint8_t>() {
  }

  Slice(const std::string& str)
      : std::basic_string_view<uint8_t>(
            reinterpret_cast<const uint8_t*>(str.data()), str.size()) {
  }

  Slice(const std::string_view& str)
      : std::basic_string_view<uint8_t>(
            reinterpret_cast<const uint8_t*>(str.data()), str.size()) {
  }

  Slice(const std::basic_string<uint8_t>& str)
      : std::basic_string_view<uint8_t>(str.data(), str.size()) {
  }

  Slice(const uint8_t* data, size_t size)
      : std::basic_string_view<uint8_t>(data, size) {
  }

  std::string ToString() const {
    return std::string(reinterpret_cast<const char*>(data()), size());
  }
};

class MutableSlice {
private:
  uint8_t* mData;
  uint64_t mSize;

public:
  MutableSlice(uint8_t* ptr, uint64_t len) : mData(ptr), mSize(len) {
  }

  uint8_t* Data() {
    return mData;
  }

  uint64_t Size() {
    return mSize;
  }

  Slice Immutable() {
    return Slice(mData, mSize);
  }
};

inline std::string ToString(Slice slice) {
  return std::string(reinterpret_cast<const char*>(slice.data()), slice.size());
}

inline Slice ToSlice(const std::string& str) {
  return Slice(reinterpret_cast<const uint8_t*>(str.data()), str.size());
}

} // namespace leanstore
