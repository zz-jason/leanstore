#pragma once

#include "FNVHash.hpp"
#include "ZipfGenerator.hpp"
#include "shared-headers/Units.hpp"

namespace leanstore {
namespace utils {

class ScrambledZipfGenerator {
public:
  uint64_t min, max, n;
  ZipfGenerator zipf_generator;
  // 10000000000ul
  // [min, max)
  ScrambledZipfGenerator(uint64_t min, uint64_t max, double theta)
      : min(min), max(max), n(max - min),
        zipf_generator((max - min) * 2, theta) {
  }
  uint64_t rand();
};

} // namespace utils
} // namespace leanstore
