#include "Misc.hpp"

#include "shared-headers/CRC.hpp"

#include <execinfo.h>

namespace leanstore {
namespace utils {

void PrintBackTrace() {
  void* buffer[10];
  size_t size;
  char** strings;
  size_t i;

  size = backtrace(buffer, 10);
  strings = backtrace_symbols(buffer, size);

  for (i = 0; i < size; i++)
    printf("%s\n", strings[i]);

  free(strings);
}

u32 CRC(const u8* src, u64 size) {
  return CRC::Calculate(src, size, CRC::CRC_32());
}

} // namespace utils
} // namespace leanstore
