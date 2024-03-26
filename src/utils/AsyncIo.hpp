#pragma once

#include "utils/Error.hpp"

#include <glog/logging.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <expected>
#include <format>
#include <vector>

#include <fcntl.h>
#include <libaio.h>

namespace leanstore::utils {

class AsyncIo {
public:
  AsyncIo(uint64_t maxBatchSize)
      : mMaxReqs(maxBatchSize),
        mNumReqs(0),
        mIocbs(maxBatchSize),
        mIocbPtrs(maxBatchSize),
        mIoEvents(maxBatchSize) {
    for (uint64_t i = 0; i < maxBatchSize; i++) {
      mIocbPtrs[i] = &mIocbs[i];
    }

    std::memset(&mAioCtx, 0, sizeof(mAioCtx));
    auto ret = io_setup(mMaxReqs, &mAioCtx);
    LOG_IF(FATAL, ret < 0) << std::format("io_setup failed, error={}", ret);
  }

  ~AsyncIo() {
    auto ret = io_destroy(mAioCtx);
    LOG_IF(FATAL, ret < 0) << std::format("io_destroy failed, error={}", ret);
  }

  size_t GetNumRequests() {
    return mNumReqs;
  }

  bool IsFull() {
    return mNumReqs >= mMaxReqs;
  }

  bool IsEmpty() {
    return mNumReqs <= 0;
  }

  void PrepareWrite(int32_t fd, void* buf, size_t count, uint64_t offset) {
    DCHECK(!IsFull());
    auto slot = mNumReqs++;
    io_prep_pwrite(&mIocbs[slot], fd, buf, count, offset);
    mIocbs[slot].data = buf;
  }

  // Even for direct IO, fsync is still needed to flush file metadata.
  void PrepareFsync(int32_t fd) {
    DCHECK(!IsFull());
    auto slot = mNumReqs++;
    io_prep_fsync(&mIocbs[slot], fd);
  }

  [[nodiscard]] std::expected<uint64_t, utils::Error> SubmitAll() {
    if (IsEmpty()) {
      return 0;
    }

    int ret = io_submit(mAioCtx, mNumReqs, &mIocbPtrs[0]);
    if (ret < 0) {
      return std::unexpected(utils::Error::ErrorAio(
          ret, std::format("io_submit({}, {}, {})", (void*)&mAioCtx, mNumReqs,
                           (void*)&mIocbPtrs[0])));
    }

    // return requests submitted
    return ret;
  }

  [[nodiscard]] std::expected<uint64_t, utils::Error> WaitAll(
      timespec* timeout = nullptr) {
    if (IsEmpty()) {
      return 0;
    }

    int ret = io_getevents(mAioCtx, mNumReqs, mNumReqs, &mIoEvents[0], timeout);
    if (ret < 0) {
      return std::unexpected(utils::Error::ErrorAio(ret, "io_getevents"));
    }

    // reset pending requests, allowing new writes
    mNumReqs = 0;

    // return requests completed
    return ret;
  }

  const io_event* GetIoEvent(size_t i) const {
    return &mIoEvents[i];
  }

  [[nodiscard]] static std::expected<int32_t, Error> Create4DirectIo(
      const char* file) {
    int flags = O_TRUNC | O_CREAT | O_RDWR | O_DIRECT;
    auto fd = open(file, flags, 0666);
    if (fd == -1) {
      return std::unexpected(
          utils::Error::FileOpen(file, errno, strerror(errno)));
    }
    return fd;
  }

private:
  size_t mMaxReqs;
  size_t mNumReqs;
  io_context_t mAioCtx;
  std::vector<iocb> mIocbs;
  std::vector<iocb*> mIocbPtrs;
  std::vector<io_event> mIoEvents;
};

} // namespace leanstore::utils