#include "wal/wal_file_cursor.hpp"

#include "leanstore/common/wal_record.h"
#include "leanstore/cpp/base/error.hpp"
#include "leanstore/utils/log.hpp"

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <format>
#include <optional>
#include <string>

namespace leanstore {

Optional<Error> WalFileCursor::LoadCurrentRecord() {
  static constexpr int64_t kWalHeaderSize = sizeof(lean_wal_record);

  wal_buffer_.resize(kWalHeaderSize);
  auto bytes_read = pread(wal_fd_, wal_buffer_.data(), kWalHeaderSize, read_offset_);

  // EOF
  if (bytes_read == 0) {
    is_valid_ = false;
    return std::nullopt;
  }

  // Error reading
  if (bytes_read != kWalHeaderSize) {
    is_valid_ = false;
    return Error::General(
        std::format("Error reading WAL header, path={}, expected_read={}, actual_read={}",
                    wal_path_, kWalHeaderSize, bytes_read));
  }

  auto* record = reinterpret_cast<lean_wal_record*>(wal_buffer_.data());
  auto full_size = record->size_;
  LEAN_DCHECK(full_size >= kWalHeaderSize,
              "Corrupted WAL record detected, path={}, read_offset_={}, full_size={}", wal_path_,
              read_offset_, full_size);

  wal_buffer_.resize(full_size);
  bytes_read = pread(wal_fd_, wal_buffer_.data() + kWalHeaderSize, full_size - kWalHeaderSize,
                     read_offset_ + kWalHeaderSize);

  if (bytes_read != full_size - kWalHeaderSize) {
    is_valid_ = false;
    return Error::General(std::format("Failed to read the full WAL record, path={}, "
                                      "read_offset_={}, expected_read={}, actual_read={}",
                                      wal_path_, read_offset_ + kWalHeaderSize,
                                      full_size - kWalHeaderSize, bytes_read));
  }

  return std::nullopt;
}

} // namespace leanstore