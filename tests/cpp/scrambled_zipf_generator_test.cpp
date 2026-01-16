#include "lean_test_suite.hpp"
#include "leanstore/utils/scrambled_zipf_generator.hpp"

#include "gtest/gtest.h"

#include <cstdint>
#include <vector>
#include <cmath>
#include <algorithm>
#include <unordered_map>

namespace leanstore::test {

class ScrambledZipfGeneratorTest : public LeanTestSuite {};

TEST_F(ScrambledZipfGeneratorTest, RangeCheck) {
  constexpr uint64_t min = 100;
  constexpr uint64_t max = 200;
  constexpr double theta = 0.5;
  utils::ScrambledZipfGenerator gen(min, max, theta);

  constexpr int iterations = 10000;
  for (int i = 0; i < iterations; ++i) {
    uint64_t value = gen.rand();
    ASSERT_GE(value, min);
    ASSERT_LT(value, max);
  }
}

TEST_F(ScrambledZipfGeneratorTest, ThetaAffectsDistribution) {
  // Test that different theta values produce statistically different distributions
  constexpr uint64_t min = 0;
  constexpr uint64_t max = 200;
  constexpr double theta_low = 0.0;
  constexpr double theta_high = 0.9;
  utils::ScrambledZipfGenerator gen_low(min, max, theta_low);
  utils::ScrambledZipfGenerator gen_high(min, max, theta_high);

  constexpr int sample_count = 100000;
  constexpr uint64_t n = max - min;
  std::vector<uint64_t> counts_low(n, 0);
  std::vector<uint64_t> counts_high(n, 0);

  for (int i = 0; i < sample_count; ++i) {
    counts_low[gen_low.rand() - min]++;
    counts_high[gen_high.rand() - min]++;
  }

  // Chi-square test for independence (two distributions differ)
  double chi2 = 0.0;
  for (size_t bucket = 0; bucket < n; ++bucket) {
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
  EXPECT_GT(chi2, 300.0) << "Distributions for theta=0 and theta=0.9 are not significantly different, chi2=" << chi2;
}

TEST_F(ScrambledZipfGeneratorTest, DifferentRanges) {
  struct Range {
    uint64_t min;
    uint64_t max;
    double theta;
  };
  std::vector<Range> ranges = {
    {0, 10, 0.5},
    {1000, 2000, 0.2},
    {500, 1000, 0.8},
  };

  for (const auto& r : ranges) {
    utils::ScrambledZipfGenerator gen(r.min, r.max, r.theta);
    constexpr int iterations = 1000;
    for (int i = 0; i < iterations; ++i) {
      uint64_t value = gen.rand();
      ASSERT_GE(value, r.min);
      ASSERT_LT(value, r.max);
    }
  }
}

} // namespace leanstore::test