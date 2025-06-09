#pragma once

#include "leanstore/utils/log.hpp"

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <expected>
#include <vector>

#include <fcntl.h>
#include <libaio.h>

namespace leanstore {

extern void CoroRead(int32_t fd, void* buf, size_t count, uint64_t offset);
extern void CoroWrite(int32_t fd, const void* buf, size_t count, uint64_t offset);
extern void CoroFsync(int32_t fd);

class CoroIo {
public:
  CoroIo(uint64_t max_batch_size)
      : max_reqs_(max_batch_size),
        num_reqs_(0),
        io_events_(max_batch_size) {
    auto start_ts = std::chrono::steady_clock::now();

    std::memset(&aio_ctx_, 0, sizeof(aio_ctx_));
    auto ret = io_setup(max_reqs_, &aio_ctx_);
    if (ret < 0) {
      Log::Fatal("io_setup failed, error={}", ret);
    }
    auto end_ts = std::chrono::steady_clock::now();
    auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::microseconds>(end_ts - start_ts).count() / 1000.0;

    Log::Info("CoroIo initialized, max_reqs={}, elapsed={}ms", max_reqs_, elapsed_ms);
  }

  ~CoroIo() {
    auto start_ts = std::chrono::steady_clock::now();

    auto ret = io_destroy(aio_ctx_);
    if (ret < 0) {
      Log::Fatal("io_destroy failed, error={}", ret);
    }

    auto elapsed_ms = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - start_ts)
                          .count() /
                      1000.0;
    Log::Info("CoroIo deinitialized, elapsed={}ms", elapsed_ms);
  }

  /// Read data from a file descriptor asynchronously.
  /// The coroutine will yield until the read operation is complete.
  void Read(int32_t fd, void* buf, size_t count, uint64_t offset);

  /// Write data to a file descriptor asynchronously.
  /// The coroutine will yield until the write operation is complete.
  void Write(int32_t fd, const void* buf, size_t count, uint64_t offset);

  /// Synchronously flush the file descriptor to disk.
  ///
  /// The coroutine will yield until the fsync operation is complete. This is
  /// necessary to ensure that all data written to the file descriptor is safely
  /// stored on disk.
  void Fsync(int32_t fd);

  /// Poll for completed IO requests.
  ///
  /// It will change the state of the coroutines that have completed IO to
  /// running, after that the coroutine can be resumed in the next round of
  /// scheduling.
  ///
  /// This should be called periodically to check for completed IO operations
  /// during Coroutine scheduling in the current thread.
  void Poll();

private:
  size_t max_reqs_;
  size_t num_reqs_;
  io_context_t aio_ctx_;
  std::vector<io_event> io_events_;
};

} // namespace leanstore