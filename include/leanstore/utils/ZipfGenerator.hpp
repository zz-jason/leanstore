#pragma once

#include <cstdint>

namespace leanstore {
namespace utils {

// A Zipf distributed random number generator
// Based on Jim Gray Algorithm as described in "Quickly Generating
// Billion-Record..."
class ZipfGenerator {
private:
  uint64_t n;
  double theta;

  double alpha, zetan, eta;

  double zeta(uint64_t n, double theta);

public:
  // [0, n)
  ZipfGenerator(uint64_t exN, double theta);
  // uint64_t rand(uint64_t new_n);
  uint64_t rand();
};

} // namespace utils
} // namespace leanstore
