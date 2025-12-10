#pragma once

#include <algorithm>
#include <cstring>
#include <random>
#include <string>

namespace leanstore::utils {

class MersenneTwister {
private:
  static const int s_nn = 312;
  static const int s_mm = 156;
  static const uint64_t s_matrix_a = 0xB5026F5AA96619E9ULL;
  static const uint64_t s_um = 0xFFFFFFFF80000000ULL;
  static const uint64_t s_lm = 0x7FFFFFFFULL;

  uint64_t mt_[s_nn];
  int mti_;

  void Init(uint64_t seed);

public:
  explicit MersenneTwister(uint64_t seed = 19650218ULL);
  uint64_t Rand();
};

} // namespace leanstore::utils

inline thread_local leanstore::utils::MersenneTwister tls_mt_generator;
inline thread_local std::mt19937 tls_std_generator;

namespace leanstore::utils {

class RandomGenerator {
public:
  /// Get a random number between min inclusive and max exclusive, i.e. in the
  /// range [min, max)
  static uint64_t RandU64(uint64_t min, uint64_t max) {
    uint64_t rand = min + (tls_mt_generator.Rand() % (max - min));
    return rand;
  }

  static uint64_t RandU64() {
    return tls_mt_generator.Rand();
  }

  static uint64_t RandU64Std(uint64_t min, uint64_t max) {
    std::uniform_int_distribution<uint64_t> distribution(min, max - 1);
    return distribution(tls_std_generator);
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

} // namespace leanstore::utils
