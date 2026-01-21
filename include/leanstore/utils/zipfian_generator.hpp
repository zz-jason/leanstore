#pragma once

#include <cassert>
#include <cstdint>
#include <memory>
#include <random>

namespace leanstore::utils {

class UniformGenerator;
class ZipfianGenerator;
class ScrambledZipfianGenerator;

/// Generates uniformly distributed integers in the range [min, max).
/// Uses a high-quality 64-bit Mersenne Twister engine.
///
/// Not thread-safe, use one generator instance per thread.
class UniformGenerator {
public:
  /// Constructs a uniform generator for range [min, max).
  /// Requires min < max.
  UniformGenerator(uint64_t min, uint64_t max)
      : engine_(std::random_device{}()),
        dist_(min, max - 1) {
    assert(min < max && "UniformGenerator: min must be less than max");
  }

  /// Generate next uniform value in [min, max).
  uint64_t Rand() {
    return dist_(engine_);
  }

private:
  std::mt19937_64 engine_;
  std::uniform_int_distribution<uint64_t> dist_;
};

/// Generates Zipf-distributed integers in the range [min, max).
/// Implements Jim Gray's algorithm with numerical stability improvements.
///
/// The distribution parameter theta must satisfy 0 <= theta < 1. The
/// distribution becomes more skewed as theta approaches 1. When theta = 0, the
/// distribution is uniform.
///
/// Not thread-safe, use one generator instance per thread.
class ZipfianGenerator {

public:
  /// Constructs a Zipfian generator for range [min, max) with skew parameter theta.
  /// Requires min < max and 0 <= theta < 1.
  ZipfianGenerator(uint64_t min, uint64_t max, double theta);

  /// Generate next Zipf-distributed value in [min, max).
  uint64_t Rand();

protected:
  /// Compute zeta(n, theta)=sum(i^(-theta)), for i = 1 to n.
  /// For large n (n > 1e6) uses integral approximation.
  static double Zeta(uint64_t n, double theta);

  /// Large n limit for direct summation in Zeta function.
  static constexpr uint64_t kDirectSummationLimit = 1'000'000;

  uint64_t min_;
  uint64_t max_;
  uint64_t n_;
  double theta_;
  double alpha_;  // 1 / (1 - theta_)
  double zeta_n_; // Zeta(n, theta), cached for efficiency
  double eta_;    // (1 - (2/n)^(1-theta)) / (1 - zeta(2,theta)/zeta(n,theta))

  std::mt19937_64 engine_;
  std::uniform_real_distribution<double> uniform_;
};

/// Generates Zipf-distributed integers in [min, max) with additional scrambling
/// to spread consecutive values across the range. Internally, it uses a larger
/// Zipf range [0, 2*n) to reduce modulo bias, and applies a hash function to
/// scramble the values.
///
/// Not thread-safe, use one generator instance per thread.
class ScrambledZipfianGenerator {
public:
  /// Constructs a scrambled Zipfian generator for range [min, max) with skew theta.
  /// Requires min < max and 0 <= theta < 1.
  ScrambledZipfianGenerator(uint64_t min, uint64_t max, double theta);

  /// Generate next scrambled Zipf-distributed value in [min, max).
  uint64_t Rand();

private:
  /// FNV hash function for scrambling.
  /// From http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
  uint64_t Hash(uint64_t val) {
    constexpr uint64_t kFnvOffsetBasis64 = 0xCBF29CE484222325L;
    constexpr uint64_t kFnvPrime64 = 1099511628211L;
    uint64_t hash_val = kFnvOffsetBasis64;
    for (int i = 0; i < 8; i++) {
      uint64_t octet = val & 0x00ff;
      val = val >> 8;
      hash_val = hash_val ^ octet;
      hash_val = hash_val * kFnvPrime64;
    }
    return hash_val;
  }

  uint64_t min_;
  uint64_t n_;

  // When theta = 0: use uniform generator (no scrambling needed)
  std::unique_ptr<UniformGenerator> uniform_gen_;

  // When theta > 0: use larger Zipf range (n*2 - 1) to reduce modulo bias
  std::unique_ptr<ZipfianGenerator> zipf_gen_large_;
};

} // namespace leanstore::utils