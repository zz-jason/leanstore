#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>

namespace leanstore {

template <typename T, size_t stack_limit>
class SmallVector {
public:
  SmallVector(size_t capacity) {
    if (capacity > stack_limit) {
      data_ = new T[capacity];
    } else {
      data_ = stack_array_;
    }
  }

  ~SmallVector() {
    if (data_ != stack_array_) {
      delete[] data_;
      data_ = nullptr;
    }
  }

  // no copy or move semantics
  SmallVector(const SmallVector&) = delete;
  SmallVector& operator=(const SmallVector&) = delete;
  SmallVector(SmallVector&&) = delete;
  SmallVector& operator=(SmallVector&&) = delete;

  T& operator[](size_t index) {
    return data_[index];
  }

  const T& operator[](size_t index) const {
    return data_[index];
  }

  T* Data() {
    return data_;
  }

private:
  T stack_array_[stack_limit];
  T* data_ = nullptr;
};

template <typename T, size_t stack_limit, size_t alignment>
class SmallVectorAligned {
public:
  SmallVectorAligned(size_t capacity) {
    if (capacity > stack_limit) {
      // Allocate aligned memory for the heap array
      data_ = reinterpret_cast<T*>(std::aligned_alloc(alignment, capacity * sizeof(T)));
      if (!data_) {
        throw std::bad_alloc();
      }
    } else {
      data_ = stack_array_;
    }
  }

  ~SmallVectorAligned() {
    if (data_ != stack_array_) {
      std::free(data_);
      data_ = nullptr;
    }
  }

  // no copy or move semantics
  SmallVectorAligned(const SmallVectorAligned&) = delete;
  SmallVectorAligned& operator=(const SmallVectorAligned&) = delete;
  SmallVectorAligned(SmallVectorAligned&&) = delete;
  SmallVectorAligned& operator=(SmallVectorAligned&&) = delete;

  T& operator[](size_t index) {
    return data_[index];
  }

  const T& operator[](size_t index) const {
    return data_[index];
  }

  T* Data() {
    return data_;
  }

private:
  alignas(alignment) T stack_array_[stack_limit];
  T* data_ = nullptr;
};

template <size_t stack_limit>
using SmallBuffer = SmallVector<uint8_t, stack_limit>;

template <size_t stack_limit>
using SmallBuffer512Aligned = SmallVectorAligned<uint8_t, stack_limit, 512>;

} // namespace leanstore