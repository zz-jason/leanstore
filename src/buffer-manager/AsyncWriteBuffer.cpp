#include "buffer-manager/AsyncWriteBuffer.hpp"

#include "leanstore/Exceptions.hpp"
#include "profiling/counters/WorkerCounters.hpp"

#include <glog/logging.h>

#include <cerrno>
#include <cstring>

namespace leanstore::storage {

AsyncWriteBuffer::AsyncWriteBuffer(int fd, uint64_t pageSize,
                                   uint64_t maxBatchSize)
    : mFd(fd),
      mPageSize(pageSize),
      mMaxBatchSize(maxBatchSize),
      mPendingRequests(0),
      mWriteBuffer(pageSize * maxBatchSize),
      mWriteCommands(maxBatchSize),
      mIocbsNew(maxBatchSize),
      mIoEvents(maxBatchSize) {
  memset(&mAioCtx, 0, sizeof(mAioCtx));
  auto ret = io_setup(maxBatchSize, &mAioCtx);
  if (ret != 0) {
    throw ex::GenericException("io_setup failed, ret code = " +
                               std::to_string(ret));
  }
}

bool AsyncWriteBuffer::IsFull() {
  return !(mPendingRequests < mMaxBatchSize);
}

void AsyncWriteBuffer::AddToIOBatch(BufferFrame& bf, PID pageId) {
  DCHECK(!IsFull());
  DCHECK(uint64_t(&bf.mPage) % 512 == 0) << "Page is not aligned to 512 bytes";
  // COUNTERS_BLOCK() {
  //   WorkerCounters::MyCounters().dt_page_writes[bf.mPage.mBTreeId]++;
  // }

  auto slot = mPendingRequests++;
  mWriteCommands[slot].Reset(&bf, pageId);
  bf.mPage.mMagicDebuging = pageId;
  void* writeBufferSlotPtr = getWriteBuffer(slot);
  std::memcpy(writeBufferSlotPtr, &bf.mPage, mPageSize);
  io_prep_pwrite(&mIocbsNew[slot][0], mFd, writeBufferSlotPtr, mPageSize,
                 mPageSize * pageId);
  mIocbsNew[slot][0].data = writeBufferSlotPtr;
}

uint64_t AsyncWriteBuffer::SubmitIORequest() {
  if (mPendingRequests > 0) {
    int retCode = io_submit(mAioCtx, mPendingRequests,
                            reinterpret_cast<iocb**>(&mIocbsNew[0]));
    DCHECK(retCode == int32_t(mPendingRequests))
        << std::format("io_submit failed, retCode={}, errno={}, error={}",
                       retCode, errno, strerror(errno));
    return mPendingRequests;
  }
  return 0;
}

uint64_t AsyncWriteBuffer::WaitIORequestToComplete() {
  if (mPendingRequests > 0) {
    auto doneRequests = io_getevents(mAioCtx, mPendingRequests,
                                     mPendingRequests, &mIoEvents[0], NULL);
    LOG_IF(FATAL, uint32_t(doneRequests) != mPendingRequests)
        << "Failed to complete all the IO requests"
        << ", expected=" << mPendingRequests << ", completed=" << doneRequests;
    mPendingRequests = 0;
    return doneRequests;
  }
  return 0;
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
