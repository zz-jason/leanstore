#pragma once

#include "leanstore/utils/jump_mu.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/portable.hpp"

#include <cmath>
#include <memory>

namespace leanstore {

template <typename T1, typename T2>
T1 DownCast(T2 ptr) {
  LS_DCHECK(dynamic_cast<T1>(ptr) != nullptr);
  return static_cast<T1>(ptr);
}

} // namespace leanstore

namespace leanstore::utils {

inline uint32_t GetBitsNeeded(uint64_t input) {
  return std::max(std::floor(std::log2(input)) + 1, 1.0);
}

inline void PinThisThread(const uint64_t worker_id) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(worker_id, &cpuset);
  pthread_t current_thread = pthread_self();
  if (pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset) != 0) {
    Log::Warn("Could not pin a thread, maybe because of over subscription?");
  }
}

inline uint64_t AlignDown(uint64_t x, uint64_t alignment) {
  return x & ~(alignment - 1);
}

inline uint64_t AlignUp(uint64_t x, uint64_t alignment) {
  return AlignDown(x + alignment - 1, alignment);
}

uint32_t CRC(const uint8_t* src, uint64_t size);

// Fold functions convert integers to a lexicographical comparable format
inline uint64_t Fold(uint8_t* writer, const uint64_t& x) {
  *reinterpret_cast<uint64_t*>(writer) = __builtin_bswap64(x);
  return sizeof(x);
}

inline uint64_t Fold(uint8_t* writer, const uint32_t& x) {
  *reinterpret_cast<uint32_t*>(writer) = __builtin_bswap32(x);
  return sizeof(x);
}

inline uint64_t Fold(uint8_t* writer, const uint16_t& x) {
  *reinterpret_cast<uint16_t*>(writer) = __builtin_bswap16(x);
  return sizeof(x);
}

inline uint64_t Fold(uint8_t* writer, const uint8_t& x) {
  *reinterpret_cast<uint8_t*>(writer) = x;
  return sizeof(x);
}

inline uint64_t Unfold(const uint8_t* input, uint64_t& x) {
  x = __builtin_bswap64(*reinterpret_cast<const uint64_t*>(input));
  return sizeof(x);
}

inline uint64_t Unfold(const uint8_t* input, uint32_t& x) {
  x = __builtin_bswap32(*reinterpret_cast<const uint32_t*>(input));
  return sizeof(x);
}

inline uint64_t Unfold(const uint8_t* input, uint16_t& x) {
  x = __builtin_bswap16(*reinterpret_cast<const uint16_t*>(input));
  return sizeof(x);
}

inline uint64_t Unfold(const uint8_t* input, uint8_t& x) {
  x = *reinterpret_cast<const uint8_t*>(input);
  return sizeof(x);
}

inline uint64_t Fold(uint8_t* writer, const int32_t& x) {
  *reinterpret_cast<uint32_t*>(writer) = __builtin_bswap32(x ^ (1ul << 31));
  return sizeof(x);
}

inline uint64_t Unfold(const uint8_t* input, int32_t& x) {
  x = __builtin_bswap32(*reinterpret_cast<const uint32_t*>(input)) ^ (1ul << 31);
  return sizeof(x);
}

inline uint64_t Fold(uint8_t* writer, const int64_t& x) {
  *reinterpret_cast<uint64_t*>(writer) = __builtin_bswap64(x ^ (1ull << 63));
  return sizeof(x);
}

inline uint64_t Unfold(const uint8_t* input, int64_t& x) {
  x = __builtin_bswap64(*reinterpret_cast<const uint64_t*>(input)) ^ (1ul << 63);
  return sizeof(x);
}

inline std::string ToHex(const uint8_t* buf, size_t size) {
  static const char kSHexDigits[] = "0123456789ABCDEF";
  std::string output;
  output.reserve(size * 2);
  for (size_t i = 0u; i < size; i++) {
    output.push_back(kSHexDigits[buf[i] >> 4]);
    output.push_back(kSHexDigits[buf[i] & 15]);
  }
  return output;
}

inline std::string StringToHex(const std::string& input) {
  return ToHex((uint8_t*)input.data(), input.size());
}

template <typename T>
std::unique_ptr<T[]> ScopedArray(size_t size) {
  return std::make_unique<T[]>(size);
}

template <typename T>
JumpScoped<std::unique_ptr<T[]>> JumpScopedArray(size_t size) {
  return JumpScoped<std::unique_ptr<T[]>>(ScopedArray<T>(size));
}

template <size_t Alignment = 512>
class AlignedBuffer {
public:
  ALIGNAS(Alignment) uint8_t* buffer_;

public:
  AlignedBuffer(size_t size)
      : buffer_(reinterpret_cast<uint8_t*>(std::aligned_alloc(Alignment, size))) {
  }

  ~AlignedBuffer() {
    if (buffer_ != nullptr) {
      free(buffer_);
      buffer_ = nullptr;
    }
  }

public:
  uint8_t* Get() {
    return buffer_;
  }

  template <typename T>
  T* CastTo() {
    return reinterpret_cast<T*>(buffer_);
  }
};

} // namespace leanstore::utils
