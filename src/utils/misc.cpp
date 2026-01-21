#include "leanstore/utils/misc.hpp"

#include <crc32c/crc32c.h>

#include <cstddef>
#include <cstdint>

namespace leanstore::utils {

uint32_t Crc32(const uint8_t* src, uint64_t size) {
  return crc32c::Crc32c(src, size);
}

Crc32Calculator& Crc32Calculator::Update(const void* data, size_t count) {
  crc32_ = crc32c::Extend(crc32_, (const uint8_t*)data, count);
  return *this;
}

} // namespace leanstore::utils
