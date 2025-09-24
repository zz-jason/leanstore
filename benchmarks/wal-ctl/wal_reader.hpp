#pragma once

#include "leanstore/common/wal_format.h"
#include "leanstore/utils/log.hpp"
#include "wal_ctl_exception.hpp"

#include <cstring>
#include <format>
#include <memory>
#include <string>

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

namespace leanstore {

class WalReader {
public:
  WalReader(const std::string& wal_path) : wal_path_(wal_path) {
  }

  void Init() {
    wal_fd_ = open(wal_path_.c_str(), O_RDONLY | O_CLOEXEC);
    if (wal_fd_ < 0) {
      Log::Error("Failed to open WAL file, wal_path={}, error={}", wal_path_, strerror(errno));
      wal_fd_ = kFdInvalid;
    }
  }

  void Deinit() {
    if (wal_fd_ >= 0) {
      close(wal_fd_);
      wal_fd_ = kFdInvalid;
    }
  }

  bool Next();

  lean_wal_record* Current();

private:
  static constexpr int kFdInvalid = -1;

  std::string wal_path_;
  int wal_fd_ = kFdInvalid;
  uint64_t read_offset_ = 0ull;
  char* buffer_ = nullptr;
};

inline bool WalReader::Next() {
  ssize_t bytes_read = pread(wal_fd_, &current_record_, sizeof(current_record_), read_offset_);
  if (bytes_read > 0) {
    read_offset_ += bytes_read;
    is_valid_ = true;
    return true;
  }
  is_valid_ = false;
  return false;
}

inline lean_wal_record* WalReader::Current() {
  return is_valid_ ? &current_record_ : nullptr;
}

} // namespace leanstore