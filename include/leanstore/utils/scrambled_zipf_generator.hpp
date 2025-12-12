#pragma once

#include "zipf_generator.hpp"

namespace leanstore::utils {

class ScrambledZipfGenerator {
public:
  // NOLINTBEGIN
  // TODO: Fix NOLINT issues

  uint64_t min;
  uint64_t max;
  uint64_t n;
  ZipfGenerator zipf_generator;

  ScrambledZipfGenerator(uint64_t min_inclusive, uint64_t max_exclusive, double theta)
      : min(min_inclusive),
        max(max_exclusive),
        n(max - min),
        zipf_generator((max - min) * 2, theta) {
  }

  uint64_t rand();

  // NOLINTEND
};

} // namespace leanstore::utils
