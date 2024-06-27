#include "leanstore/utils/ScrambledZipfGenerator.hpp"

#include "leanstore/utils/FNVHash.hpp"

namespace leanstore::utils {

uint64_t ScrambledZipfGenerator::rand() {
  uint64_t zipfValue = zipf_generator.rand();
  return min + (FNV::Hash(zipfValue) % n);
}

} // namespace leanstore::utils
