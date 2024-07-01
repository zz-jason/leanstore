#pragma once

#include "leanstore/utils/Error.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/Result.hpp"

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
    if (ret < 0) {
      Log::Fatal("io_setup failed, error={}", ret);
    }
  }

  ~AsyncIo() {
    auto ret = io_destroy(mAioCtx);
    if (ret < 0) {
      Log::Fatal("io_destroy failed, error={}", ret);
    }
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

  void PrepareRead(int32_t fd, void* buf, size_t count, uint64_t offset) {
    LS_DCHECK((reinterpret_cast<uint64_t>(buf) & (kAlignment - 1)) == 0);
    LS_DCHECK(!IsFull());
    auto slot = mNumReqs++;
    io_prep_pread(&mIocbs[slot], fd, buf, count, offset);
    mIocbs[slot].data = buf;
  }

  void PrepareWrite(int32_t fd, void* buf, size_t count, uint64_t offset) {
    LS_DCHECK((reinterpret_cast<uint64_t>(buf) & (kAlignment - 1)) == 0);
    LS_DCHECK(!IsFull());
    auto slot = mNumReqs++;
    io_prep_pwrite(&mIocbs[slot], fd, buf, count, offset);
    mIocbs[slot].data = buf;
  }

  // Even for direct IO, fsync is still needed to flush file metadata.
  void PrepareFsync(int32_t fd) {
    LS_DCHECK(!IsFull());
    auto slot = mNumReqs++;
    io_prep_fsync(&mIocbs[slot], fd);
  }

  Result<uint64_t> SubmitAll() {
    if (IsEmpty()) {
      return 0;
    }

    int ret = io_submit(mAioCtx, mNumReqs, &mIocbPtrs[0]);
    if (ret < 0) {
      return std::unexpected(
          utils::Error::ErrorAio(ret, std::format("io_submit({}, {}, {})", (void*)&mAioCtx,
                                                  mNumReqs, (void*)&mIocbPtrs[0])));
    }

    // return requests submitted
    return ret;
  }

  Result<uint64_t> WaitAll(timespec* timeout = nullptr) {
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

  Result<int32_t> Create4DirectIo(const char* file) {
    int flags = O_TRUNC | O_CREAT | O_RDWR | O_DIRECT;
    auto fd = open(file, flags, 0666);
    if (fd == -1) {
      return std::unexpected(utils::Error::FileOpen(file, errno, strerror(errno)));
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