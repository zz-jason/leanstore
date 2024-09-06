#include "leanstore/buffer-manager/AsyncWriteBuffer.hpp"

#include "leanstore/buffer-manager/BufferFrame.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/Result.hpp"

namespace leanstore::storage {

AsyncWriteBuffer::AsyncWriteBuffer(int fd, uint64_t pageSize, uint64_t maxBatchSize)
    : mFd(fd),
      mPageSize(pageSize),
      mAIo(maxBatchSize),
      mWriteBuffer(pageSize * maxBatchSize),
      mWriteCommands(maxBatchSize) {
}

AsyncWriteBuffer::~AsyncWriteBuffer() {
}

bool AsyncWriteBuffer::IsFull() {
  return mAIo.IsFull();
}

void AsyncWriteBuffer::Add(const BufferFrame& bf) {
  LS_DCHECK(uint64_t(&bf) % 512 == 0, "BufferFrame is not aligned to 512 bytes");

  // record the written buffer frame and page id for later use
  auto pageId = bf.mHeader.mPageId;
  auto slot = mAIo.GetNumRequests();
  mWriteCommands[slot].Reset(&bf, pageId);

  // copy the page content to write buffer
  auto* buffer = copyToBuffer(&bf.mPage, slot);

  mAIo.PrepareWrite(mFd, buffer, mPageSize, mPageSize * pageId);
}

Result<uint64_t> AsyncWriteBuffer::SubmitAll() {
  return mAIo.SubmitAll();
}

Result<uint64_t> AsyncWriteBuffer::WaitAll() {
  return mAIo.WaitAll();
}

void AsyncWriteBuffer::IterateFlushedBfs(
    std::function<void(BufferFrame& flushedBf, uint64_t flushedPsn)> callback,
    uint64_t numFlushedBfs) {
  for (uint64_t i = 0; i < numFlushedBfs; i++) {
    const auto slot = (reinterpret_cast<uint64_t>(mAIo.GetIoEvent(i)->data) -
                       reinterpret_cast<uint64_t>(mWriteBuffer.Get())) /
                      mPageSize;
    auto* flushedPage = reinterpret_cast<Page*>(getWriteBuffer(slot));
    auto flushedPsn = flushedPage->mPsn;
    auto* flushedBf = mWriteCommands[slot].mBf;
    callback(*const_cast<BufferFrame*>(flushedBf), flushedPsn);
  }
}

} // namespace leanstore::storage
