#pragma once

#include "leanstore/base/slice.hpp"

#include <cassert>
#include <cstddef>
#include <cstring>
#include <memory>

namespace leanstore {

class ByteBuffer {
public:
  /// Constructor to create an empty ByteBuffer
  ByteBuffer() : buffer_(nullptr), size_(0) {
  }

  /// Constructor to create a ByteBuffer of given size, initialized to zero
  explicit ByteBuffer(size_t size) : buffer_(new std::byte[size]()), size_(size) {
  }

  /// Constructor to create a ByteBuffer from existing data
  ByteBuffer(const std::byte* data, size_t size) : buffer_(new std::byte[size]), size_(size) {
    assert(data != nullptr && "Data pointer cannot be null");
    std::memcpy(buffer_.get(), data, size);
  }

  // No copy and assign
  ByteBuffer(const ByteBuffer&) = delete;
  ByteBuffer& operator=(const ByteBuffer&) = delete;

  /// Move constructor
  ByteBuffer(ByteBuffer&& other) noexcept : buffer_(std::move(other.buffer_)), size_(other.size_) {
    other.buffer_ = nullptr;
    other.size_ = 0;
  }

  /// Move assignment
  ByteBuffer& operator=(ByteBuffer&& other) noexcept {
    if (this != &other) {
      buffer_ = std::move(other.buffer_);
      size_ = other.size_;
      other.buffer_ = nullptr;
      other.size_ = 0;
    }
    return *this;
  }

  /// Returns the size of the buffer
  size_t size() const { // NOLINT: mimic std::vector interface
    return size_;
  }

  /// Returns the raw data pointer
  std::byte* data() { // NOLINT: mimic std::vector interface
    assert(buffer_ != nullptr && "Buffer is null");
    return buffer_.get();
  }

  /// Returns the raw data pointer as type T*
  template <typename T>
  T* as() { // NOLINT: mimic std::vector interface
    assert(buffer_ != nullptr && "Buffer is null");
    return reinterpret_cast<T*>(buffer_.get());
  }

  Slice slice(size_t offset = 0, size_t length = SIZE_MAX) const { // NOLINT
    if (length == SIZE_MAX) {
      length = size_ - offset;
    }
    assert(offset + length <= size_ && "Slice out of bounds");
    return Slice(reinterpret_cast<const uint8_t*>(buffer_.get() + offset), length);
  }

  MutableSlice mutable_slice(size_t offset = 0, size_t length = SIZE_MAX) { // NOLINT
    if (length == SIZE_MAX) {
      length = size_ - offset;
    }
    assert(offset + length <= size_ && "Slice out of bounds");
    return MutableSlice(reinterpret_cast<uint8_t*>(buffer_.get() + offset), length);
  }

private:
  std::unique_ptr<std::byte[]> buffer_;
  size_t size_;
};

} // namespace leanstore