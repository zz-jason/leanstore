#pragma once

#include "leanstore/utils/Log.hpp"

#include <algorithm>
#include <cstring>
#include <random>
#include <string>

namespace leanstore {
namespace utils {

class MersenneTwister {
private:
  static const int sNn = 312;
  static const int sMm = 156;
  static const uint64_t sMatrixA = 0xB5026F5AA96619E9ULL;
  static const uint64_t sUm = 0xFFFFFFFF80000000ULL;
  static const uint64_t sLm = 0x7FFFFFFFULL;

  uint64_t mMt[sNn];
  int mMti;

  void init(uint64_t seed);

public:
  MersenneTwister(uint64_t seed = 19650218ULL);
  uint64_t Rand();
};

} // namespace utils
} // namespace leanstore

static thread_local leanstore::utils::MersenneTwister tlsMtGenerator;
static thread_local std::mt19937 tlsStdGenerator;

namespace leanstore {
namespace utils {

class RandomGenerator {
public:
  //! Get a random number between min inclusive and max exclusive, i.e. in the
  //! range [min, max)
  static uint64_t RandU64(uint64_t min, uint64_t max) {
    uint64_t rand = min + (tlsMtGenerator.Rand() % (max - min));
    LS_DCHECK(min <= rand && rand < max,
              "Random number should be in range [min, max), but min={}, "
              "max={}, rand={}",
              min, max, rand);
    return rand;
  }

  static uint64_t RandU64() {
    return tlsMtGenerator.Rand();
  }

  static uint64_t RandU64Std(uint64_t min, uint64_t max) {
    std::uniform_int_distribution<uint64_t> distribution(min, max - 1);
    return distribution(tlsStdGenerator);
  }

  template <typename T>
  inline static T Rand(T min, T max) {
    uint64_t rand = RandU64(min, max);
    return static_cast<T>(rand);
  }

  static void RandString(uint8_t* dst, uint64_t size) {
    for (uint64_t i = 0; i < size; i++) {
      dst[i] = Rand(48, 123);
    }
  }

  static std::string RandAlphString(size_t len) {
    static constexpr auto kChars = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";
    auto result = std::string(len, '\0');
    std::generate_n(begin(result), len, [&]() {
      auto i = RandU64Std(0, std::strlen(kChars));
      return kChars[i];
    });
    return result;
  }

  static void RandAlphString(size_t len, std::string& result) {
    static constexpr auto kChars = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";
    result.resize(len, '\0');
    std::generate_n(begin(result), len, [&]() {
      auto i = RandU64Std(0, std::strlen(kChars));
      return kChars[i];
    });
  }
};

} // namespace utils
} // namespace leanstore
