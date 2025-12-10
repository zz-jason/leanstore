#include "leanstore/utils/misc.hpp"

#include <crc32c/crc32c.h>

#include <cstddef>
#include <cstdint>

namespace leanstore::utils {

uint32_t CRC(const uint8_t* src, uint64_t size) {
  return crc32c::Crc32c(src, size);
}

Crc32& Crc32::Update(const void* data, size_t count) {
  crc_ = crc32c::Extend(crc_, (const uint8_t*)data, count);
  return *this;
}

} // namespace leanstore::utils
