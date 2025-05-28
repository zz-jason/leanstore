#pragma once

#include "leanstore/utils/error.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/result.hpp"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <expected>
#include <format>
#include <vector>

#include <fcntl.h>
#include <libaio.h>

namespace leanstore::utils {

constexpr size_t kAlignment = 512;

class AsyncIo {
public:
  AsyncIo(uint64_t max_batch_size)
      : max_reqs_(max_batch_size),
        num_reqs_(0),
        iocbs_(max_batch_size),
        iocb_ptrs_(max_batch_size),
        io_events_(max_batch_size) {
    for (uint64_t i = 0; i < max_batch_size; i++) {
      iocb_ptrs_[i] = &iocbs_[i];
    }

    std::memset(&aio_ctx_, 0, sizeof(aio_ctx_));
    auto ret = io_setup(max_reqs_, &aio_ctx_);
    if (ret < 0) {
      Log::Fatal("io_setup failed, error={}", ret);
    }
  }

  ~AsyncIo() {
    auto ret = io_destroy(aio_ctx_);
    if (ret < 0) {
      Log::Fatal("io_destroy failed, error={}", ret);
    }
  }

  size_t GetNumRequests() {
    return num_reqs_;
  }

  bool IsFull() {
    return num_reqs_ >= max_reqs_;
  }

  bool IsEmpty() {
    return num_reqs_ <= 0;
  }

  void PrepareRead(int32_t fd, void* buf, size_t count, uint64_t offset) {
    LS_DCHECK((reinterpret_cast<uint64_t>(buf) & (kAlignment - 1)) == 0);
    LS_DCHECK(!IsFull());
    auto slot = num_reqs_++;
    io_prep_pread(&iocbs_[slot], fd, buf, count, offset);
    iocbs_[slot].data = buf;
  }

  void PrepareWrite(int32_t fd, void* buf, size_t count, uint64_t offset) {
    LS_DCHECK((reinterpret_cast<uint64_t>(buf) & (kAlignment - 1)) == 0);
    LS_DCHECK(!IsFull());
    auto slot = num_reqs_++;
    io_prep_pwrite(&iocbs_[slot], fd, buf, count, offset);
    iocbs_[slot].data = buf;
  }

  // Even for direct IO, fsync is still needed to flush file metadata.
  void PrepareFsync(int32_t fd) {
    LS_DCHECK(!IsFull());
    auto slot = num_reqs_++;
    io_prep_fsync(&iocbs_[slot], fd);
  }

  Result<uint64_t> SubmitAll() {
    if (IsEmpty()) {
      return 0;
    }

    int ret = io_submit(aio_ctx_, num_reqs_, &iocb_ptrs_[0]);
    if (ret < 0) {
      return std::unexpected(
          utils::Error::ErrorAio(ret, std::format("io_submit({}, {}, {})", (void*)&aio_ctx_,
                                                  num_reqs_, (void*)&iocb_ptrs_[0])));
    }

    // return requests submitted
    return ret;
  }

  Result<uint64_t> WaitAll(timespec* timeout = nullptr) {
    if (IsEmpty()) {
      return 0;
    }

    int ret = io_getevents(aio_ctx_, num_reqs_, num_reqs_, &io_events_[0], timeout);
    if (ret < 0) {
      return std::unexpected(utils::Error::ErrorAio(ret, "io_getevents"));
    }

    // reset pending requests, allowing new writes
    num_reqs_ = 0;

    // return requests completed
    return ret;
  }

  const io_event* GetIoEvent(size_t i) const {
    return &io_events_[i];
  }

  Result<int32_t> Create4DirectIo(const char* file) {
    int flags = O_TRUNC | O_CREAT | O_RDWR | O_DIRECT;
    auto fd = open(file, flags, 0666);
    if (fd == -1) {
      return std::unexpected(utils::Error::FileOpen(file, errno, strerror(errno)));
    }
    return fd;
  }

private:
  size_t max_reqs_;
  size_t num_reqs_;
  io_context_t aio_ctx_;
  std::vector<iocb> iocbs_;
  std::vector<iocb*> iocb_ptrs_;
  std::vector<io_event> io_events_;
};

} // namespace leanstore::utils