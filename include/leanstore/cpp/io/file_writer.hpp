#pragma once

#include "leanstore/cpp/base/error.hpp"
#include "leanstore/cpp/base/likely.hpp"
#include "leanstore/cpp/base/result.hpp"
#include "leanstore/cpp/base/slice.hpp"

#include <cassert>

#include <fcntl.h>
#include <unistd.h>

namespace leanstore {

class FileWriter {
public:
  static Result<FileWriter> New(std::string_view file_path);

  FileWriter(const FileWriter&) = delete;                      // no copy constructor
  FileWriter& operator=(const FileWriter&) = delete;           // no copy assignment
  FileWriter& operator=(FileWriter&& other) noexcept = delete; // no move assignment

  /// Only move constructor is allowed.
  FileWriter(FileWriter&& other) noexcept : fd_(other.fd_), offset_(other.offset_) {
    other.fd_ = kInvalidFd;
  }

  ~FileWriter() {
    assert(fd_ == kInvalidFd && "FileWriter must be closed before destruction");
  }

  /// Append data to the file. Returns error if write fails.
  Result<void> Append(Slice data);

  /// Returns the number of bytes written so far.
  int64_t BytesWritten() const {
    return offset_;
  }

  /// Close the file. Should be explicitly called to ensure data is flushed to
  /// disk. Errors during close are returned as Result.
  Result<void> Close();

private:
  static constexpr int kInvalidFd = -1;

  explicit FileWriter(int fd) : fd_(fd), offset_(0) {
    assert(fd_ >= 0);
  }

  /// File descriptor for the opened file.
  int fd_;

  /// Current offset in the file.
  int64_t offset_;
};

inline Result<FileWriter> FileWriter::New(std::string_view file_path) {
  int fd = open(file_path.data(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (LEAN_UNLIKELY(fd < 0)) {
    return Error::FileOpen(file_path, errno, strerror(errno));
  }
  return FileWriter(fd);
}

inline Result<void> FileWriter::Close() {
  if (fd_ >= 0) {
    if (fdatasync(fd_) < 0) {
      return Error::FileSync("Failed to sync file", errno, strerror(errno));
    }
    if (close(fd_) < 0) {
      return Error::FileClose("Failed to close file", errno, strerror(errno));
    }
    fd_ = kInvalidFd;
  }
  return {};
}

inline Result<void> FileWriter::Append(Slice data) {
  if (LEAN_UNLIKELY(fd_ < 0)) {
    return Error::FileWrite("File is not open");
  }

  // Write data to the file at the current offset.
  auto written = pwrite(fd_, data.data(), data.size(), offset_);
  if (LEAN_UNLIKELY(written < 0)) {
    return Error::FileWrite("Failed to write to file", errno, strerror(errno));
  }
  if (LEAN_UNLIKELY(static_cast<size_t>(written) != data.size())) {
    return Error::FileWrite("Partial write to file", -1, "");
  }

  offset_ += written;
  return {};
}

} // namespace leanstore