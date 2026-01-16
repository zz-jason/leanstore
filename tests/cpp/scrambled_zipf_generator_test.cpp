#include "lean_test_suite.hpp"  // NOLINTNEXTLINE(clang-diagnostic-error)
#include "leanstore/utils/scrambled_zipf_generator.hpp"

#include "gtest/gtest.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <unordered_map>
#include <vector>

namespace leanstore::test {

class ScrambledZipfGeneratorTest : public LeanTestSuite {};

TEST_F(ScrambledZipfGeneratorTest, RangeCheck) {  // NOLINT
  constexpr uint64_t kMin = 100;
  constexpr uint64_t kMax = 200;
  constexpr double kTheta = 0.5;
  utils::ScrambledZipfGenerator gen(kMin, kMax, kTheta);

  constexpr int kIterations = 10000;
  for (int i = 0; i < kIterations; ++i) {
    uint64_t value = gen.rand();
    ASSERT_GE(value, kMin);
    ASSERT_LT(value, kMax);
  }
}

TEST_F(ScrambledZipfGeneratorTest, ThetaAffectsDistribution) {  // NOLINT
  // Test that different theta values produce statistically different distributions
  constexpr uint64_t kMin = 0;
  constexpr uint64_t kMax = 200;
  constexpr double kThetaLow = 0.0;
  constexpr double kThetaHigh = 0.9;
  utils::ScrambledZipfGenerator gen_low(kMin, kMax, kThetaLow);
  utils::ScrambledZipfGenerator gen_high(kMin, kMax, kThetaHigh);

  constexpr int kSampleCount = 100000;
  constexpr uint64_t kN = kMax - kMin;
  std::vector<uint64_t> counts_low(kN, 0);
  std::vector<uint64_t> counts_high(kN, 0);

  for (int i = 0; i < kSampleCount; ++i) {
    counts_low[gen_low.rand() - kMin]++;
    counts_high[gen_high.rand() - kMin]++;
  }

  // Chi-square test for independence (two distributions differ)
  double chi2 = 0.0;
  for (size_t bucket = 0; bucket < kN; ++bucket) {
    uint64_t o1 = counts_low[bucket];
    uint64_t o2 = counts_high[bucket];
    if (o1 == 0 && o2 == 0) {
      continue; // skip empty buckets
    }
    double e1 = static_cast<double>(o1 + o2) / 2.0;
    double e2 = e1;
    chi2 += (static_cast<double>(o1) - e1) * (static_cast<double>(o1) - e1) / e1;
    chi2 += (static_cast<double>(o2) - e2) * (static_cast<double>(o2) - e2) / e2;
  }
  // Degrees of freedom approximately (n - 1). Critical value for 0.01 significance
  // with n=200 is about 265. Our chi2 should be large because distributions differ.
  EXPECT_GT(chi2, 300.0)
      << "Distributions for theta=0 and theta=0.9 are not significantly different, chi2=" << chi2;
}

TEST_F(ScrambledZipfGeneratorTest, DifferentRanges) {  // NOLINT
  struct Range {
    uint64_t min_;
    uint64_t max_;
    double theta_;
  };
  std::vector<Range> ranges = {
      {.min_=0, .max_=10, .theta_=0.5},
      {.min_=1000, .max_=2000, .theta_=0.2},
      {.min_=500, .max_=1000, .theta_=0.8},
  };

  for (const auto& r : ranges) {
    utils::ScrambledZipfGenerator gen(r.min_, r.max_, r.theta_);
    constexpr int kIterations = 1000;
    for (int i = 0; i < kIterations; ++i) {
      uint64_t value = gen.rand();
      ASSERT_GE(value, r.min_);
      ASSERT_LT(value, r.max_);
    }
  }
}

} // namespace leanstore::test