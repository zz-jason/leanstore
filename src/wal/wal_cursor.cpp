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

Optional<Error> WalCursor::Foreach(const std::function<bool(lean_wal_record& record)>& func) {
  // Seek to the begining.
  if (auto err = Seek(0); err) {
    return std::move(*err);
  }

  // Iterate through all WAL records.
  while (Valid()) {
    // Apply the custom function for the current record.
    auto cont_next = func(const_cast<lean_wal_record&>(CurrentRecord()));
    if (!cont_next) {
      return std::nullopt;
    }

    // Advance to the next record.
    if (auto err = Next(); err) {
      return std::move(*err);
    }
  }

  return std::nullopt;
}

} // namespace leanstore