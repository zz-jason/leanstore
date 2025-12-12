#include "leanstore/utils/scrambled_zipf_generator.hpp"

#include "leanstore/utils/fnv_hash.hpp"

#include <cstdint>

namespace leanstore::utils {

uint64_t ScrambledZipfGenerator::rand() {
  uint64_t zipf_value = zipf_generator.rand();
  return min + (FNV::Hash(zipf_value) % n);
}

} // namespace leanstore::utils
