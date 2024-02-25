#pragma once

#include "Config.hpp"
#include "JumpMU.hpp"
#include "shared-headers/Units.hpp"

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

inline u32 GetBitsNeeded(u64 input) {
  return std::max(std::floor(std::log2(input)) + 1, 1.0);
}

inline double CalculateMTPS(
    std::chrono::high_resolution_clock::time_point begin,
    std::chrono::high_resolution_clock::time_point end, u64 factor) {
  double tps =
      ((factor * 1.0 /
        (std::chrono::duration_cast<std::chrono::microseconds>(end - begin)
             .count() /
         1000000.0)));
  return (tps / 1000000.0);
}

inline void PinThisThread(const u64 workerId) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(workerId, &cpuset);
  pthread_t currentThread = pthread_self();
  if (pthread_setaffinity_np(currentThread, sizeof(cpu_set_t), &cpuset) != 0) {
    DLOG(ERROR)
        << "Could not pin a thread, maybe because of over subscription?";
  }
}

void PrintBackTrace();

inline u64 AlignUp(u64 x) {
  return (x + 511) & ~511ul;
}

inline u64 AlignDown(u64 x) {
  return x - (x & 511);
}

u32 CRC(const u8* src, u64 size);

// Fold functions convert integers to a lexicographical comparable format
inline u64 Fold(u8* writer, const u64& x) {
  *reinterpret_cast<u64*>(writer) = __builtin_bswap64(x);
  return sizeof(x);
}

inline u64 Fold(u8* writer, const u32& x) {
  *reinterpret_cast<u32*>(writer) = __builtin_bswap32(x);
  return sizeof(x);
}

inline u64 Fold(u8* writer, const u16& x) {
  *reinterpret_cast<u16*>(writer) = __builtin_bswap16(x);
  return sizeof(x);
}

inline u64 Fold(u8* writer, const u8& x) {
  *reinterpret_cast<u8*>(writer) = x;
  return sizeof(x);
}

inline u64 Unfold(const u8* input, u64& x) {
  x = __builtin_bswap64(*reinterpret_cast<const u64*>(input));
  return sizeof(x);
}

inline u64 Unfold(const u8* input, u32& x) {
  x = __builtin_bswap32(*reinterpret_cast<const u32*>(input));
  return sizeof(x);
}

inline u64 Unfold(const u8* input, u16& x) {
  x = __builtin_bswap16(*reinterpret_cast<const u16*>(input));
  return sizeof(x);
}

inline u64 Unfold(const u8* input, u8& x) {
  x = *reinterpret_cast<const u8*>(input);
  return sizeof(x);
}

inline u64 Fold(u8* writer, const s32& x) {
  *reinterpret_cast<u32*>(writer) = __builtin_bswap32(x ^ (1ul << 31));
  return sizeof(x);
}

inline u64 Unfold(const u8* input, s32& x) {
  x = __builtin_bswap32(*reinterpret_cast<const u32*>(input)) ^ (1ul << 31);
  return sizeof(x);
}

inline u64 Fold(u8* writer, const s64& x) {
  *reinterpret_cast<u64*>(writer) = __builtin_bswap64(x ^ (1ull << 63));
  return sizeof(x);
}

inline u64 Unfold(const u8* input, s64& x) {
  x = __builtin_bswap64(*reinterpret_cast<const u64*>(input)) ^ (1ul << 63);
  return sizeof(x);
}

inline std::string ToHex(const u8* buf, size_t size) {
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
  return ToHex((u8*)input.data(), input.size());
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
  alignas(Alignment) u8* mBuffer;

public:
  AlignedBuffer(size_t size)
      : mBuffer(reinterpret_cast<u8*>(std::aligned_alloc(Alignment, size))) {
  }

  ~AlignedBuffer() {
    if (mBuffer != nullptr) {
      free(mBuffer);
      mBuffer = nullptr;
    }
  }

public:
  u8* Get() {
    return mBuffer;
  }

  template <typename T> T* CastTo() {
    return reinterpret_cast<T*>(mBuffer);
  }
};

struct Timer {
  std::atomic<u64>& mTimeCounterUS;

  std::chrono::high_resolution_clock::time_point mStartTimePoint;

  Timer(std::atomic<u64>& timeCounterUS) : mTimeCounterUS(timeCounterUS) {
    if (FLAGS_measure_time) {
      mStartTimePoint = std::chrono::high_resolution_clock::now();
    }
  }

  ~Timer() {
    if (FLAGS_measure_time) {
      auto endTimePoint = std::chrono::high_resolution_clock::now();
      const u64 duration =
          std::chrono::duration_cast<std::chrono::microseconds>(endTimePoint -
                                                                mStartTimePoint)
              .count();
      mTimeCounterUS += duration;
    }
  }
};

} // namespace utils
} // namespace leanstore
