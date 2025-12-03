#pragma once

#include "leanstore/common/types.h"
#include "leanstore/common/wal_record.h"
#include "leanstore/cpp/base/error.hpp"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/cpp/wal/wal_cursor.hpp"

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace leanstore {

/// WAL file cursor implementation that reads WAL records from a file.
class WalFileCursor : public WalCursor {
public:
  /// Creates a new WAL file cursor for the specified WAL file descriptor.
  /// @param wal_path The path to the WAL file. Used for error reporting.
  /// @param wal_fd The file descriptor of the WAL file. Must be opened for reading.
  WalFileCursor(std::string_view wal_path, int wal_fd)
      : wal_path_(wal_path),
        wal_fd_(wal_fd),
        read_offset_(0),
        is_valid_(false),
        wal_buffer_() {
    LEAN_DCHECK(wal_fd_ >= 0, "Invalid WAL file descriptor");
    wal_buffer_.resize(kDefaultBufferSize);
  }

  ~WalFileCursor() override {
    if (wal_fd_ >= 0) {
      close(wal_fd_);
      wal_fd_ = kInvalidFd;
    }
  }

  Optional<Error> Seek(lean_lid_t lsn) override {
    read_offset_ = static_cast<int64_t>(lsn);
    is_valid_ = true;
    return LoadCurrentRecord();
  }

  Optional<Error> Next() override {
    LEAN_DCHECK(Valid(), "Can only advance when the cursor is valid");
    read_offset_ += wal_buffer_.size();
    return LoadCurrentRecord();
  }

  bool Valid() const override {
    return is_valid_;
  }

  const lean_wal_record& CurrentRecord() const override {
    LEAN_DCHECK(Valid(), "Can only get current record when the cursor is valid");
    return *reinterpret_cast<const lean_wal_record*>(wal_buffer_.data());
  }

private:
  /// Loads the current WAL record into the buffer.
  Optional<Error> LoadCurrentRecord();

  static constexpr auto kInvalidFd = -1;
  static constexpr auto kDefaultBufferSize = 1024 * 1024;

  const std::string wal_path_;
  int wal_fd_;
  int64_t read_offset_;
  bool is_valid_;
  std::string wal_buffer_;
};

} // namespace leanstore