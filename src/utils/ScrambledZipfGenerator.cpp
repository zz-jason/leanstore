#include "ScrambledZipfGenerator.hpp"

#include "FNVHash.hpp"

namespace leanstore {
namespace utils {

uint64_t ScrambledZipfGenerator::rand() {
  uint64_t zipfValue = zipf_generator.rand();
  return min + (FNV::Hash(zipfValue) % n);
}

} // namespace utils
} // namespace leanstore
