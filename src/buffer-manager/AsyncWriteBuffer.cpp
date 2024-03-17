#include "buffer-manager/AsyncWriteBuffer.hpp"

#include "leanstore/Exceptions.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "utils/Error.hpp"

#include <glog/logging.h>

#include <cstring>
#include <expected>

namespace leanstore::storage {

AsyncWriteBuffer::AsyncWriteBuffer(int fd, uint64_t pageSize,
                                   uint64_t maxBatchSize)
    : mFd(fd),
      mPageSize(pageSize),
      mMaxBatchSize(maxBatchSize),
      mPendingRequests(0),
      mWriteBuffer(pageSize * maxBatchSize),
      mWriteCommands(maxBatchSize),
      mIocbs(maxBatchSize),
      mIocbPtrs(maxBatchSize),
      mIoEvents(maxBatchSize) {
  memset(&mAioCtx, 0, sizeof(mAioCtx));
  auto ret = io_setup(maxBatchSize, &mAioCtx);
  LOG_IF(FATAL, ret < 0) << "Failed to create AIO context, error=" << ret;
  for (uint64_t i = 0; i < maxBatchSize; i++) {
    mIocbPtrs[i] = &mIocbs[i];
  }
}

AsyncWriteBuffer::~AsyncWriteBuffer() {
  auto ret = io_destroy(mAioCtx);
  LOG_IF(FATAL, ret < 0) << "Failed to destroy AIO context, error=" << ret;
}

bool AsyncWriteBuffer::IsFull() {
  return !(mPendingRequests < mMaxBatchSize);
}

void AsyncWriteBuffer::Add(BufferFrame& bf, PID pageId) {
  DCHECK(!IsFull());
  DCHECK(uint64_t(&bf.mPage) % 512 == 0) << "Page is not aligned to 512 bytes";
  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().dt_page_writes[bf.mPage.mBTreeId]++;
  }

  auto slot = mPendingRequests++;
  mWriteCommands[slot].Reset(&bf, pageId);
  bf.mPage.mMagicDebuging = pageId;
  void* writeBufferSlotPtr = getWriteBuffer(slot);
  std::memcpy(writeBufferSlotPtr, &bf.mPage, mPageSize);
  io_prep_pwrite(&mIocbs[slot], mFd, writeBufferSlotPtr, mPageSize,
                 mPageSize * pageId);
  mIocbs[slot].data = writeBufferSlotPtr;
}

std::expected<uint64_t, utils::Error> AsyncWriteBuffer::SubmitAll() {
  if (mPendingRequests <= 0) {
    return 0;
  }
  int ret = io_submit(mAioCtx, mPendingRequests, &mIocbPtrs[0]);
  if (ret < 0) {
    return std::unexpected(utils::Error::ErrorAio(ret, "io_submit"));
  }
  DCHECK(uint64_t(ret) == mPendingRequests)
      << "Failed to submit all IO requests, expected=" << mPendingRequests;

  // return requests submitted
  return ret;
}

std::expected<uint64_t, utils::Error> AsyncWriteBuffer::WaitAll() {
  if (mPendingRequests <= 0) {
    return 0;
  }
  int ret = io_getevents(mAioCtx, mPendingRequests, mPendingRequests,
                         &mIoEvents[0], NULL);
  if (ret < 0) {
    return std::unexpected(utils::Error::ErrorAio(ret, "io_getevents"));
  }
  DCHECK(uint64_t(ret) == mPendingRequests)
      << "Failed to get all IO events, expected=" << mPendingRequests;

  // reset pending requests, allowing new writes
  mPendingRequests = 0;

  // return requests completed
  return ret;
}

void AsyncWriteBuffer::IterateFlushedBfs(
    std::function<void(BufferFrame&, uint64_t)> callback,
    uint64_t numFlushedBfs) {
  for (uint64_t i = 0; i < numFlushedBfs; i++) {
    const auto slot =
        (uint64_t(mIoEvents[i].data) - uint64_t(mWriteBuffer.Get())) /
        mPageSize;

    DCHECK(mIoEvents[i].res == mPageSize);
    explainIfNot(mIoEvents[i].res2 == 0);
    auto* flushedPage = reinterpret_cast<Page*>(getWriteBuffer(slot));
    auto flushedGsn = flushedPage->mGSN;
    auto* flushedBf = mWriteCommands[slot].mBf;
    callback(*flushedBf, flushedGsn);
  }
}

} // namespace leanstore::storage
