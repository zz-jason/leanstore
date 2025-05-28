#pragma once

#include <cstdint>

namespace leanstore {
namespace utils {

class FNV {
private:
  static constexpr uint64_t kFnvOffsetBasis64 = 0xCBF29CE484222325L;
  static constexpr uint64_t kFnvPrime64 = 1099511628211L;

public:
  static uint64_t Hash(uint64_t val);
};

} // namespace utils
} // namespace leanstore
