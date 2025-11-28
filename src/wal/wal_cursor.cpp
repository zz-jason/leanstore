#include "leanstore/cpp/wal/wal_cursor.hpp"

#include "leanstore/cpp/base/error.hpp"
#include "wal/wal_file_cursor.hpp"

#include <cassert>
#include <cerrno>
#include <cstring>
#include <memory>
#include <string_view>

#include <fcntl.h>

namespace leanstore {

Result<std::unique_ptr<WalCursor>> WalCursor::New(std::string_view wal_path) {
  auto fd = open(wal_path.data(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return Error::FileOpen(wal_path, errno, strerror(errno));
  }

  std::unique_ptr<WalCursor> cursor = std::make_unique<WalFileCursor>(wal_path, fd);
  return cursor;
}

} // namespace leanstore