#include "leanstore/wal/wal_cursor.hpp"

#include "leanstore/base/error.hpp"
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

Result<std::vector<std::unique_ptr<WalCursor>>> WalCursor::NewWalCursors(
    const std::vector<std::string>& wal_file_paths) {
  std::vector<std::unique_ptr<WalCursor>> wal_cursors;
  for (const auto& wal_file_path : wal_file_paths) {
    auto cursor = WalCursor::New(wal_file_path);
    if (cursor) {
      wal_cursors.push_back(std::move(cursor.value()));
    } else {
      return std::move(cursor.error());
    }
  }
  return wal_cursors;
}

} // namespace leanstore