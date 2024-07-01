#pragma once

#include "ZipfGenerator.hpp"

namespace leanstore::utils {

class ScrambledZipfGenerator {
public:
  uint64_t min;
  uint64_t max;
  uint64_t n;
  ZipfGenerator zipf_generator;

  ScrambledZipfGenerator(uint64_t minInclusive, uint64_t maxExclusive, double theta)
      : min(minInclusive),
        max(maxExclusive),
        n(max - min),
        zipf_generator((max - min) * 2, theta) {
  }

  uint64_t rand();
};

} // namespace leanstore::utils
