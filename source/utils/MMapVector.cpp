#include "FVector.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace utils {

void writeBinary(const char* pathname, std::vector<std::string>& v) {
  std::cout << "Writing binary file : " << pathname << std::endl;
  int fd = open(pathname, O_RDWR | O_CREAT | O_TRUNC,
                S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  POSIX_CHECK(fd != -1);
  uint64_t fileSize = 8 + 16 * v.size();
  for (auto s : v)
    fileSize += s.size() + 1;
  POSIX_CHECK(posix_fallocate(fd, 0, fileSize) == 0);
  auto data = reinterpret_cast<FVector<std::string_view>::Data*>(
      mmap(nullptr, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
  data->count = v.size();
  POSIX_CHECK(data != MAP_FAILED);
  uint64_t offset = 8 + 16 * v.size();
  char* dst = reinterpret_cast<char*>(data);
  uint64_t slot = 0;
  for (auto s : v) {
    data->slot[slot].size = s.size();
    data->slot[slot].offset = offset;
    memcpy(dst + offset, s.data(), s.size());
    offset += s.size();
    slot++;
  }
  POSIX_CHECK(close(fd) == 0);
}
} // namespace utils
} // namespace leanstore