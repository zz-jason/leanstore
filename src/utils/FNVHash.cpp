#include "leanstore/utils/FNVHash.hpp"

namespace leanstore {
namespace utils {

uint64_t FNV::Hash(uint64_t val) {
  // from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
  uint64_t hashVal = kFnvOffsetBasis64;
  for (int i = 0; i < 8; i++) {
    uint64_t octet = val & 0x00ff;
    val = val >> 8;

    hashVal = hashVal ^ octet;
    hashVal = hashVal * kFnvPrime64;
  }
  return hashVal;
}

} // namespace utils
} // namespace leanstore
