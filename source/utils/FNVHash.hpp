#include "shared-headers/Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace utils {
// -------------------------------------------------------------------------------------
class FNV {
private:
  static constexpr uint64_t FNV_OFFSET_BASIS_64 = 0xCBF29CE484222325L;
  static constexpr uint64_t FNV_PRIME_64 = 1099511628211L;

public:
  static uint64_t hash(uint64_t val);
};
// -------------------------------------------------------------------------------------
} // namespace utils
} // namespace leanstore
