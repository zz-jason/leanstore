#include "wal_printer.hpp"
#include <format>
#include <iostream>

#include <fcntl.h>
#include <unistd.h>

namespace leanstore {

void WalPrinter::Run() {
  // open the file
  int fd = open(wal_path_.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    perror("open");
    exit(EXIT_FAILURE);
  }

  // read and print the records
  static constexpr size_t kHeaderSize = sizeof(lean_wal_record);
  uint8_t header_buf[kHeaderSize];
  uint64_t read_offset = 0ull;

  while (true) {
    ssize_t bytes_read = pread(fd, header_buf, kHeaderSize, read_offset);
    if (bytes_read == 0) {
      break; // EOF
    }

    if (bytes_read < 0) {
      std::cerr << std::format("Error reading WAL record, {}: ", wal_path_);
      perror("read");
      exit(EXIT_FAILURE);
    } else if (bytes_read < static_cast<ssize_t>(kHeaderSize)) {
      fprintf(stderr, "Incomplete WAL record header\n");
      exit(EXIT_FAILURE);
    }

    // parse header
    current_record_ = reinterpret_cast<lean_wal_record*>(header_buf);
    if (current_record_->size_ < kHeaderSize) {
      fprintf(stderr, "Invalid WAL record size: %u\n", current_record_->size_);
      exit(EXIT_FAILURE);
    }
  }
}

} // namespace leanstore