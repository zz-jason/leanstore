#include "AsyncWriteBuffer.hpp"

#include "leanstore/Exceptions.hpp"
#include "profiling/counters/WorkerCounters.hpp"

#include <glog/logging.h>

#include <cstring>

namespace leanstore {
namespace storage {

AsyncWriteBuffer::AsyncWriteBuffer(int fd, uint64_t pageSize,
                                   uint64_t maxBatchSize)
    : fd(fd),
      page_size(pageSize),
      batch_max_size(maxBatchSize),
      mWriteBuffer(pageSize * maxBatchSize) {
  write_buffer_commands = std::make_unique<WriteCommand[]>(maxBatchSize);
  iocbs = std::make_unique<struct iocb[]>(maxBatchSize);
  iocbs_ptr = std::make_unique<struct iocb*[]>(maxBatchSize);
  events = std::make_unique<struct io_event[]>(maxBatchSize);

  memset(&aio_context, 0, sizeof(aio_context));
  const int ret = io_setup(maxBatchSize, &aio_context);
  if (ret != 0) {
    throw ex::GenericException("io_setup failed, ret code = " +
                               std::to_string(ret));
  }
}

bool AsyncWriteBuffer::full() {
  if (pending_requests >= batch_max_size - 2) {
    return true;
  }
  return false;
}

void AsyncWriteBuffer::AddToIOBatch(BufferFrame& bf, PID pageId) {
  DCHECK(!full());
  DCHECK(uint64_t(&bf.page) % 512 == 0);
  DCHECK(pending_requests <= batch_max_size);
  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().dt_page_writes[bf.page.mBTreeId]++;
  }

  auto slot = pending_requests++;
  write_buffer_commands[slot].bf = &bf;
  write_buffer_commands[slot].mPageId = pageId;
  bf.page.mMagicDebuging = pageId;
  void* writeBufferSlotPtr = GetWriteBuffer(slot);
  std::memcpy(writeBufferSlotPtr, &bf.page, page_size);
  io_prep_pwrite(/* iocb */ &iocbs[slot], /* fd */ fd,
                 /* buf */ writeBufferSlotPtr, /* count */ page_size,
                 /* offset */ page_size * pageId);
  iocbs[slot].data = writeBufferSlotPtr;
  iocbs_ptr[slot] = &iocbs[slot];
}

uint64_t AsyncWriteBuffer::SubmitIORequest() {
  if (pending_requests > 0) {
    int retCode = io_submit(aio_context, pending_requests, iocbs_ptr.get());
    DCHECK(retCode == int32_t(pending_requests));
    return pending_requests;
  }
  return 0;
}

uint64_t AsyncWriteBuffer::WaitIORequestToComplete() {
  if (pending_requests > 0) {
    const int doneRequests = io_getevents(
        /* ctx */ aio_context, /* min_nr */ pending_requests,
        /* nr */ pending_requests, /* io_event */ events.get(),
        /* timeout */ NULL);
    LOG_IF(FATAL, uint32_t(doneRequests) != pending_requests)
        << "Failed to complete all the IO requests"
        << ", expected=" << pending_requests << ", completed=" << doneRequests;
    pending_requests = 0;
    return doneRequests;
  }
  return 0;
}

void AsyncWriteBuffer::IterateFlushedBfs(
    std::function<void(BufferFrame&, uint64_t)> callback,
    uint64_t numFlushedBfs) {
  for (uint64_t i = 0; i < numFlushedBfs; i++) {
    const auto slot =
        (uint64_t(events[i].data) - uint64_t(mWriteBuffer.Get())) / page_size;

    DCHECK(events[i].res == page_size);
    explainIfNot(events[i].res2 == 0);
    auto* flushedPage = reinterpret_cast<Page*>(GetWriteBuffer(slot));
    auto flushedLSN = flushedPage->mPSN;
    auto* flushedBf = write_buffer_commands[slot].bf;
    callback(*flushedBf, flushedLSN);
  }
}

} // namespace storage
} // namespace leanstore
