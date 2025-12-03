#pragma once

#include <cstdint>
#include <cstring>
#include <span>
#include <string>

namespace leanstore {

/// Non-owning byte slice, read-only
class Slice {
public:
  using const_slice_t = std::span<const uint8_t>;

  Slice() : slice_() {
  }

  Slice(const std::string& str) : slice_(reinterpret_cast<const uint8_t*>(str.data()), str.size()) {
  }

  Slice(const std::basic_string<uint8_t>& str) : slice_(str.data(), str.size()) {
  }

  Slice(std::string_view str) : slice_(reinterpret_cast<const uint8_t*>(str.data()), str.size()) {
  }

  Slice(const uint8_t* data, size_t size) : slice_(data, size) {
  }

  Slice(const char* data) : slice_(reinterpret_cast<const uint8_t*>(data), std::strlen(data)) {
  }

  Slice(const char* data, size_t size) : slice_(reinterpret_cast<const uint8_t*>(data), size) {
  }

  const uint8_t* data() const { // NOLINT: mimic std::string_view interface
    return slice_.data();
  }

  uint64_t size() const { // NOLINT: mimic std::string_view interface
    return slice_.size();
  }

  void remove_prefix(size_t n) { // NOLINT: mimic std::string_view interface
    slice_ = slice_.subspan(n);
  }

  friend bool operator==(const Slice& lhs, const Slice& rhs) {
    return lhs.slice_.size() == rhs.slice_.size() &&
           std::memcmp(lhs.slice_.data(), rhs.slice_.data(), lhs.slice_.size()) == 0;
  }

  friend bool operator<(const Slice& lhs, const Slice& rhs) {
    const size_t min_size = std::min(lhs.slice_.size(), rhs.slice_.size());
    int cmp = std::memcmp(lhs.slice_.data(), rhs.slice_.data(), min_size);
    if (cmp == 0) {
      return lhs.slice_.size() < rhs.slice_.size();
    }
    return cmp < 0;
  }

  friend bool operator<=(const Slice& lhs, const Slice& rhs) {
    return (lhs < rhs) || (lhs == rhs);
  }

  friend bool operator>(const Slice& lhs, const Slice& rhs) {
    return !(lhs <= rhs);
  }

  friend bool operator>=(const Slice& lhs, const Slice& rhs) {
    return !(lhs < rhs);
  }

  friend bool operator!=(const Slice& lhs, const Slice& rhs) {
    return !(lhs == rhs);
  }

  uint8_t operator[](size_t index) {
    return slice_[index];
  }

  std::string ToString() const {
    return std::string(reinterpret_cast<const char*>(slice_.data()), slice_.size());
  }

  void CopyTo(std::string& dest) const {
    dest.resize(slice_.size());
    std::memcpy(dest.data(), slice_.data(), slice_.size());
  }

private:
  const_slice_t slice_;
};

/// Non-owning mutable byte slice
class MutableSlice {
public:
  using mutable_slice_t = std::span<uint8_t>;

  MutableSlice(uint8_t* data, uint64_t size) : slice_(data, size) {
  }

  uint8_t* data() { // NOLINT: mimic std::span interface
    return slice_.data();
  }

  uint64_t size() const { // NOLINT: mimic std::span interface
    return slice_.size();
  }

private:
  mutable_slice_t slice_;
};

} // namespace leanstore