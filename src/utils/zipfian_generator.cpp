#include "leanstore/utils/zipfian_generator.hpp"

#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>

namespace leanstore::utils {

ZipfianGenerator::ZipfianGenerator(uint64_t min, uint64_t max, double theta)
    : min_(min),
      max_(max),
      n_(max - min),
      theta_(theta),
      engine_(std::random_device{}()),
      uniform_(0.0, 1.0) {
  assert(min < max && "ZipfianGenerator: min must be less than max");
  assert(theta >= 0.0 && theta < 1.0 && "ZipfianGenerator: theta must be in [0, 1)");
  if (theta_ == 0.0) {
    // Uniform distribution special case
    alpha_ = 1.0;
    zeta_n_ = static_cast<double>(n_);
    eta_ = 1.0;
  } else {
    alpha_ = 1.0 / (1.0 - theta_);
    zeta_n_ = Zeta(n_, theta_);

    // Compute zeta(2, theta)
    const double zeta2 = std::pow(1.0, -theta_) + std::pow(2.0, -theta_);
    const double ratio = zeta2 / zeta_n_;
    // Guard against division by zero (should not happen for theta < 1)
    if (ratio >= 1.0) {
      // This occurs when n_ is very small (e.g., n_ = 1 or 2).
      // Fall back to uniform distribution.
      eta_ = 1.0;
    } else {
      const double two_over_n = 2.0 / static_cast<double>(n_);
      const double numerator = 1.0 - std::pow(two_over_n, 1.0 - theta_);
      const double denominator = 1.0 - ratio;
      eta_ = numerator / denominator;
    }

    // Ensure eta_ is finite and non-negative
    if (!std::isfinite(eta_) || eta_ < 0.0) {
      eta_ = 1.0;
    }
  }
}

uint64_t ZipfianGenerator::Rand() {
  const double u = uniform_(engine_);
  const double uz = u * zeta_n_;

  // Fast path for first two values (Jim Gray's optimization)
  if (uz < 1.0) {
    return min_;
  }
  if (uz < 1.0 + std::pow(0.5, theta_)) {
    return min_ + 1;
  }

  // Compute v = 1 - eta_ * (1 - u) = eta_ * u - eta_ + 1
  // Using the numerically stable form: 1 - eta_ * (1 - u)
  const double v = 1.0 - eta_ * (1.0 - u);

  // v should be in (0, 1] when eta_ <= 1. If eta_ > 1, clamp to a small positive value.
  const double safe_v = std::max(v, std::numeric_limits<double>::min());
  const double pow_v = std::pow(safe_v, alpha_);

  // result in [0, n_)
  const uint64_t offset = static_cast<uint64_t>(static_cast<double>(n_) * pow_v);
  const uint64_t clamped_offset = (offset >= n_) ? n_ - 1 : offset;
  return min_ + clamped_offset;
}

double ZipfianGenerator::Zeta(uint64_t n, double theta) {
  // zeta(n, 0) = n
  if (theta == 0.0) {
    return static_cast<double>(n);
  }

  // Direct summation for small n
  if (n <= kDirectSummationLimit) {
    double sum = 0.0;
    for (uint64_t i = 1; i <= n; ++i) {
      sum += std::pow(static_cast<double>(i), -theta);
    }
    return sum;
  }

  const double n_pow = std::pow(static_cast<double>(n), 1.0 - theta);
  const double integral = (n_pow - 1.0) / (1.0 - theta);
  const double correction = 0.5 * (1.0 + std::pow(static_cast<double>(n), -theta));
  return integral + correction;
}

ScrambledZipfianGenerator::ScrambledZipfianGenerator(uint64_t min, uint64_t max, double theta)
    : min_(min),
      n_(max - min) {
  assert(min < max && "ScrambledZipfianGenerator: min must be less than max");
  assert(theta >= 0.0 && theta < 1.0 && "ScrambledZipfianGenerator: theta must be in [0, 1)");

  const bool is_theta_zero = std::abs(theta) < std::numeric_limits<double>::epsilon();
  if (is_theta_zero) {
    uniform_gen_ = std::make_unique<UniformGenerator>(min, max);
  } else {
    zipf_gen_large_ = std::make_unique<ZipfianGenerator>(0, n_ * 2 - 1, theta);
  }
}

uint64_t ScrambledZipfianGenerator::Rand() {
  // Uniform distribution (theta = 0): no scrambling needed.
  if (uniform_gen_ != nullptr) {
    return uniform_gen_->Rand();
  }

  // Zipf distribution (theta > 0): use larger range and hash+mod to reduce modulo bias.
  // Get raw Zipf-distributed value in larger range [0, n_*2 - 1)
  const uint64_t raw_large = zipf_gen_large_->Rand();

  // Hash the raw value to break sequential locality
  const uint64_t hash = Hash(raw_large);

  // Map hash to [0, n_) using modulo
  const uint64_t scrambled_offset = hash % n_;

  return min_ + scrambled_offset;
}

} // namespace leanstore::utils