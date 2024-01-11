#pragma once
#include "shared-headers/Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <cstdint>
#include <random>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
// A Zipf distributed random number generator
// Based on Jim Gray Algorithm as described in "Quickly Generating Billion-Record..."
// -------------------------------------------------------------------------------------
class ZipfGenerator
{
   // -------------------------------------------------------------------------------------
  private:
   u64 n;
   double theta;
   // -------------------------------------------------------------------------------------
   double alpha, zetan, eta;
   // -------------------------------------------------------------------------------------
   double zeta(u64 n, double theta);

  public:
   // [0, n)
   ZipfGenerator(uint64_t ex_n, double theta);
   // uint64_t rand(u64 new_n);
   uint64_t rand();
};
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
   // -------------------------------------------------------------------------------------
