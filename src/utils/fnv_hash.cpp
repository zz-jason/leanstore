#include "leanstore/utils/fnv_hash.hpp"

namespace leanstore::utils {

uint64_t FNV::Hash(uint64_t val) {
  // from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
  uint64_t hash_val = kFnvOffsetBasis64;
  for (int i = 0; i < 8; i++) {
    uint64_t octet = val & 0x00ff;
    val = val >> 8;

    hash_val = hash_val ^ octet;
    hash_val = hash_val * kFnvPrime64;
  }
  return hash_val;
}

} // namespace leanstore::utils
