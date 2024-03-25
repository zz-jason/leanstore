#pragma once

#include "JumpMU.hpp"
#include "leanstore/Config.hpp"

#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <cmath>

namespace leanstore {

template <typename T1, typename T2> T1 DownCast(T2 ptr) {
  DCHECK(dynamic_cast<T1>(ptr) != nullptr);
  return static_cast<T1>(ptr);
}

namespace utils {

inline uint32_t GetBitsNeeded(uint64_t input) {
  return std::max(std::floor(std::log2(input)) + 1, 1.0);
}

inline double CalculateMTPS(
    std::chrono::high_resolution_clock::time_point begin,
    std::chrono::high_resolution_clock::time_point end, uint64_t factor) {
  double tps =
      ((factor * 1.0 /
        (std::chrono::duration_cast<std::chrono::microseconds>(end - begin)
             .count() /
         1000000.0)));
  return (tps / 1000000.0);
}

inline void PinThisThread(const uint64_t workerId) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(workerId, &cpuset);
  pthread_t currentThread = pthread_self();
  if (pthread_setaffinity_np(currentThread, sizeof(cpu_set_t), &cpuset) != 0) {
    DLOG(ERROR)
        << "Could not pin a thread, maybe because of over subscription?";
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
  x = __builtin_bswap32(*reinterpret_cast<const uint32_t*>(input)) ^
      (1ul << 31);
  return sizeof(x);
}

inline uint64_t Fold(uint8_t* writer, const int64_t& x) {
  *reinterpret_cast<uint64_t*>(writer) = __builtin_bswap64(x ^ (1ull << 63));
  return sizeof(x);
}

inline uint64_t Unfold(const uint8_t* input, int64_t& x) {
  x = __builtin_bswap64(*reinterpret_cast<const uint64_t*>(input)) ^
      (1ul << 63);
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

template <typename T> std::unique_ptr<T[]> ScopedArray(size_t size) {
  return std::make_unique<T[]>(size);
}

template <typename T>
JumpScoped<std::unique_ptr<T[]>> JumpScopedArray(size_t size) {
  return JumpScoped<std::unique_ptr<T[]>>(ScopedArray<T>(size));
}

template <size_t Alignment = 512> class AlignedBuffer {
public:
  alignas(Alignment) uint8_t* mBuffer;

public:
  AlignedBuffer(size_t size)
      : mBuffer(
            reinterpret_cast<uint8_t*>(std::aligned_alloc(Alignment, size))) {
  }

  ~AlignedBuffer() {
    if (mBuffer != nullptr) {
      free(mBuffer);
      mBuffer = nullptr;
    }
  }

public:
  uint8_t* Get() {
    return mBuffer;
  }

  template <typename T> T* CastTo() {
    return reinterpret_cast<T*>(mBuffer);
  }
};

struct Timer {
  std::atomic<uint64_t>& mTimeCounterUS;

  std::chrono::high_resolution_clock::time_point mStartTimePoint;

  Timer(std::atomic<uint64_t>& timeCounterUS) : mTimeCounterUS(timeCounterUS) {
    if (FLAGS_measure_time) {
      mStartTimePoint = std::chrono::high_resolution_clock::now();
    }
  }

  ~Timer() {
    if (FLAGS_measure_time) {
      auto endTimePoint = std::chrono::high_resolution_clock::now();
      const uint64_t duration =
          std::chrono::duration_cast<std::chrono::microseconds>(endTimePoint -
                                                                mStartTimePoint)
              .count();
      mTimeCounterUS += duration;
    }
  }
};

} // namespace utils
} // namespace leanstore
