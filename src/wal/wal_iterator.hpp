#pragma once

#include "leanstore/common/wal_record.h"
#include "leanstore/utils/log.hpp"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <format>
#include <memory>
#include <string>
#include <string_view>

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

namespace leanstore {

class WalIterator {
public:
  static std::unique_ptr<WalIterator> New(std::string_view wal_path);

  WalIterator() = default;

  virtual ~WalIterator() = default;

  virtual lean_wal_record* Next() = 0;

  bool HasError() {
    return !error_.empty();
  }

  std::string GetError() const {
    return error_;
  }

protected:
  std::string error_;
};

class WalFileIterator : public WalIterator {
public:
  WalFileIterator(std::string_view wal_path);
  ~WalFileIterator() override;
  lean_wal_record* Next() override;

private:
  static constexpr auto kDefaultFd = -1;
  static constexpr auto kDefaultBufferSize = 1024 * 1024;

  std::string wal_path_;
  int wal_fd_;
  int64_t read_offset_;
  std::string wal_buffer_;
};

inline std::unique_ptr<WalIterator> WalIterator::New(std::string_view wal_path) {
  return std::make_unique<WalFileIterator>(wal_path);
}

inline WalFileIterator::WalFileIterator(std::string_view wal_path)
    : wal_path_(wal_path),
      wal_fd_(kDefaultFd),
      read_offset_(0),
      wal_buffer_() {
  wal_fd_ = open(wal_path.data(), O_RDONLY | O_CLOEXEC);
  if (wal_fd_ < 0) {
    error_ = std::format("Failed to open WAL file, path={}, error={}", wal_path_, strerror(errno));
    wal_fd_ = kDefaultFd;
  } else {
    wal_buffer_.resize(kDefaultBufferSize);
  }
}

inline WalFileIterator::~WalFileIterator() {
  if (wal_fd_ >= 0) {
    close(wal_fd_);
    wal_fd_ = kDefaultFd;
  }
}

inline lean_wal_record* WalFileIterator::Next() {
  if (HasError()) {
    return nullptr;
  }

  static constexpr int64_t kWalHeaderSize = sizeof(lean_wal_record);

  wal_buffer_.resize(kWalHeaderSize);
  auto bytes_read = pread(wal_fd_, wal_buffer_.data(), kWalHeaderSize, read_offset_);

  // EOF
  if (bytes_read == 0) {
    return nullptr;
  }

  // Error reading
  if (bytes_read != kWalHeaderSize) {
    error_ = std::format("Error reading WAL header, path={}, expected_read={}, actual_read={}",
                         wal_path_, kWalHeaderSize, bytes_read);
    return nullptr;
  }

  auto* record = reinterpret_cast<lean_wal_record*>(wal_buffer_.data());
  auto full_size = record->size_;

  LEAN_DCHECK(full_size >= kWalHeaderSize,
              "Corrupted WAL record detected, path={}, read_offset_={}, full_size={}", wal_path_,
              read_offset_, full_size);

  // Resize buffer if needed
  wal_buffer_.resize(full_size);
  bytes_read = pread(wal_fd_,
                     wal_buffer_.data() + kWalHeaderSize, // buffer to store the remaining data
                     full_size - kWalHeaderSize,          // size of the remaining data
                     read_offset_ + kWalHeaderSize        // offset to read from
  );

  LEAN_DCHECK(bytes_read == full_size - kWalHeaderSize,
              "Failed to read the full WAL record, path={}, read_offset_={}, expected_read={}, "
              "actual_read={}",
              wal_path_, read_offset_ + kWalHeaderSize, full_size - kWalHeaderSize, bytes_read);
  if (bytes_read != full_size - kWalHeaderSize) {
    error_ = std::format("Failed to read the full WAL record, path={}, read_offset_={}, "
                         "expected_read={}, actual_read={}",
                         wal_path_, read_offset_ + kWalHeaderSize, full_size - kWalHeaderSize,
                         bytes_read);
    return nullptr;
  }

  read_offset_ += full_size;
  return reinterpret_cast<lean_wal_record*>(wal_buffer_.data());
}

} // namespace leanstore