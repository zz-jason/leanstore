#include "leanstore/utils/misc.hpp"

#include <crc32c/crc32c.h>

namespace leanstore::utils {

uint32_t CRC(const uint8_t* src, uint64_t size) {
  return crc32c::Crc32c(src, size);
}

} // namespace leanstore::utils
