#include "Misc.hpp"

#include "Exceptions.hpp"

#include "Config.hpp"

#include "CRC.hpp"

#include <execinfo.h>

#include <atomic>

namespace leanstore {
namespace utils {

void PrintBackTrace() {
  void* array[10];
  size_t size;
  char** strings;
  size_t i;

  size = backtrace(array, 10);
  strings = backtrace_symbols(array, size);

  for (i = 0; i < size; i++)
    printf("%s\n", strings[i]);

  free(strings);
}
// -------------------------------------------------------------------------------------
u32 CRC(const u8* src, u64 size) {
  return CRC::Calculate(src, size, CRC::CRC_32());
}
// -------------------------------------------------------------------------------------
} // namespace utils
} // namespace leanstore
