#include "common/lean_test_suite.hpp"
#include "leanstore/utils/zipfian_generator.hpp"

#include "gtest/gtest.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <vector>

namespace leanstore::test {

namespace {

/// Collect empirical distribution from the generator
/// @tparam Generator Type of the generator (must have Rand() method)
/// @param gen Generator instance
/// @param min Minimum value (inclusive)
/// @param max Maximum value (exclusive)
/// @param sample_count Number of samples to draw
/// @return Vector of empirical probabilities
template <typename Generator>
std::vector<double> CollectEmpiricalDistribution(Generator& gen, uint64_t min, uint64_t max,
                                                 int sample_count) {
  const uint64_t n = max - min;
  std::vector<uint64_t> counts(n, 0);
  for (int i = 0; i < sample_count; ++i) {
    uint64_t value = gen.Rand();
    assert(value >= min && value < max);
    ++counts[value - min];
  }

  std::vector<double> empirical(n, 0.0);
  for (size_t i = 0; i < n; ++i) {
    empirical[i] = static_cast<double>(counts[i]) / sample_count;
  }
  return empirical;
}

/// Compute theoretical Zipf probabilities for range [0, n)
/// @param n Number of elements
/// @param theta Zipf distribution parameter
/// @return Vector of probabilities
std::vector<double> ComputeZipfProbabilities(uint64_t n, double theta) {
  std::vector<double> probs(n, 0.0);
  if (theta == 0.0) {
    // Uniform distribution
    double uniform = 1.0 / static_cast<double>(n);
    std::fill(probs.begin(), probs.end(), uniform);
    return probs;
  }

  // Compute zeta(n, theta)
  double zeta = 0.0;
  for (uint64_t i = 1; i <= n; ++i) {
    zeta += 1.0 / std::pow(static_cast<double>(i), theta);
  }

  // Compute probabilities
  for (uint64_t i = 0; i < n; ++i) {
    probs[i] = 1.0 / std::pow(static_cast<double>(i + 1), theta) / zeta;
  }
  return probs;
}

/// Compute total variation distance between two distributions
/// @param p First distribution
/// @param q Second distribution
/// @return Total variation distance
double TotalVariationDistance(const std::vector<double>& p, const std::vector<double>& q) {
  double tvd = 0.0;
  for (size_t i = 0; i < p.size(); ++i) {
    tvd += std::abs(p[i] - q[i]);
  }
  return tvd / 2.0; // Normalize to [0, 1]
}

} // namespace

class ZipfianGeneratorTest : public LeanTestSuite {};

TEST_F(ZipfianGeneratorTest, ZipfianDistribution) {
  auto compare_to_theoretical = [](double theta) {
    constexpr uint64_t kMin = 0;
    constexpr uint64_t kMax = 1000;
    constexpr int kSampleCount = 10000;
    constexpr uint64_t kN = kMax - kMin;

    utils::ZipfianGenerator gen(kMin, kMax, theta);
    std::vector<double> empirical = CollectEmpiricalDistribution(gen, kMin, kMax, kSampleCount);
    std::vector<double> theoretical = ComputeZipfProbabilities(kN, theta);

    // Compute distances to theoretical distribution
    double tvd = TotalVariationDistance(empirical, theoretical);

    // Assert that empirical distribution is close to theoretical
    EXPECT_LT(tvd, 0.2) << std::format("Zipfian generator distribution deviates too much"
                                       " from theoretical for theta={:.2f}. TVD={:.6f}",
                                       theta, tvd);
  };

  compare_to_theoretical(0.00);
  compare_to_theoretical(0.10);
  compare_to_theoretical(0.20);
  compare_to_theoretical(0.30);
  compare_to_theoretical(0.40);
  compare_to_theoretical(0.50);
  compare_to_theoretical(0.60);
  compare_to_theoretical(0.70);
  compare_to_theoretical(0.80);
  compare_to_theoretical(0.90);
  compare_to_theoretical(0.99);
}

TEST_F(ZipfianGeneratorTest, ZipfianDistributionScrambled) {
  auto compare_to_theoretical = [](double theta, double tvd_threshold) {
    constexpr uint64_t kMin = 0;
    constexpr uint64_t kMax = 1000; // N = 1000
    constexpr int kSampleCount = 10000;
    constexpr uint64_t kN = kMax - kMin;

    utils::ScrambledZipfianGenerator gen(kMin, kMax, theta);
    std::vector<double> empirical = CollectEmpiricalDistribution(gen, kMin, kMax, kSampleCount);
    std::vector<double> theoretical = ComputeZipfProbabilities(kN, theta);

    std::vector<double> sorted_empirical = empirical;
    std::vector<double> sorted_theoretical = theoretical;
    std::sort(sorted_empirical.begin(), sorted_empirical.end());
    std::sort(sorted_theoretical.begin(), sorted_theoretical.end());

    // Compute distances to theoretical distribution
    double tvd = TotalVariationDistance(sorted_empirical, sorted_theoretical);

    // Assert that empirical distribution is close to theoretical
    EXPECT_LT(tvd, tvd_threshold) << std::format(
        "Scrambled Zipfian generator distribution deviates too much"
        " from theoretical for theta={:.2f}. TVD={:.6f}",
        theta, tvd);
  };

  compare_to_theoretical(0.00, 0.15);
  compare_to_theoretical(0.10, 0.30);
  compare_to_theoretical(0.20, 0.30);
  compare_to_theoretical(0.30, 0.30);
  compare_to_theoretical(0.40, 0.25);
  compare_to_theoretical(0.50, 0.20);
  compare_to_theoretical(0.60, 0.20);
  compare_to_theoretical(0.70, 0.20);
  compare_to_theoretical(0.80, 0.15);
  compare_to_theoretical(0.90, 0.15);
  compare_to_theoretical(0.99, 0.15);
}

/// Test that scrambling effectively reduces sequential locality in the output
TEST_F(ZipfianGeneratorTest, ScramblingQuality) {
  constexpr uint64_t kMin = 0;
  constexpr uint64_t kMax = 1000;
  constexpr double kTheta = 0.8;
  constexpr int kSequenceLength = 1000;

  // Collect sequences
  utils::ScrambledZipfianGenerator gen(kMin, kMax, kTheta);
  std::vector<uint64_t> sequence;
  sequence.reserve(kSequenceLength);
  for (int i = 0; i < kSequenceLength; ++i) {
    auto val = gen.Rand();
    ASSERT_GE(val, kMin);
    ASSERT_LT(val, kMax);
    sequence.push_back(val);
  }

  // Compute auto-correlation at lag 1 (measure of sequential locality). For
  // good scrambling, consecutive values should be nearly independent. We'll
  // compute the fraction of ascending pairs as a simple metric
  auto compute_ascending_ratio = [](const std::vector<uint64_t>& seq) {
    int ascending = 0;
    for (size_t i = 1; i < seq.size(); ++i) {
      if (seq[i] > seq[i - 1]) {
        ++ascending;
      }
    }
    return static_cast<double>(ascending) / (seq.size() - 1);
  };

  double ratio = compute_ascending_ratio(sequence);
  EXPECT_GT(ratio, 0.4) << "Scrambling has too few ascending pairs: " << ratio;
  EXPECT_LT(ratio, 0.6) << "Scrambling has too many ascending pairs: " << ratio;
}

} // namespace leanstore::test