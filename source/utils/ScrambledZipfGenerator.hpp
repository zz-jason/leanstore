#include "FNVHash.hpp"
#include "shared-headers/Units.hpp"
#include "ZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace utils {
// -------------------------------------------------------------------------------------
class ScrambledZipfGenerator {
public:
  u64 min, max, n;
  ZipfGenerator zipf_generator;
  // 10000000000ul
  // [min, max)
  ScrambledZipfGenerator(u64 min, u64 max, double theta)
      : min(min), max(max), n(max - min),
        zipf_generator((max - min) * 2, theta) {
  }
  u64 rand();
};
// -------------------------------------------------------------------------------------
} // namespace utils
} // namespace leanstore
