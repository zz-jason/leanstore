#include "AsyncWriteBuffer.hpp"

#include "Tracing.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "shared-headers/Exceptions.hpp"

#include <glog/logging.h>

#include <cstring>

namespace leanstore {
namespace storage {

AsyncWriteBuffer::AsyncWriteBuffer(int fd, u64 pageSize, u64 maxBatchSize)
    : fd(fd),
      page_size(pageSize),
      batch_max_size(maxBatchSize),
      mWriteBuffer(pageSize * maxBatchSize) {
  write_buffer_commands = make_unique<WriteCommand[]>(maxBatchSize);
  iocbs = make_unique<struct iocb[]>(maxBatchSize);
  iocbs_ptr = make_unique<struct iocb*[]>(maxBatchSize);
  events = make_unique<struct io_event[]>(maxBatchSize);

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
  DCHECK(u64(&bf.page) % 512 == 0);
  DCHECK(pending_requests <= batch_max_size);
  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().dt_page_writes[bf.page.mBTreeId]++;
  }

  PARANOID_BLOCK() {
    if (FLAGS_pid_tracing && !FLAGS_reclaim_page_ids) {
      Tracing::mutex.lock();
      if (Tracing::ht.contains(pageId)) {
        auto& entry = Tracing::ht[pageId];
        DCHECK(std::get<0>(entry) == bf.page.mBTreeId);
      }
      Tracing::mutex.unlock();
    }
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

u64 AsyncWriteBuffer::SubmitIORequest() {
  if (pending_requests > 0) {
    int retCode = io_submit(aio_context, pending_requests, iocbs_ptr.get());
    DCHECK(retCode == s32(pending_requests));
    return pending_requests;
  }
  return 0;
}

u64 AsyncWriteBuffer::WaitIORequestToComplete() {
  if (pending_requests > 0) {
    const int doneRequests = io_getevents(
        /* ctx */ aio_context, /* min_nr */ pending_requests,
        /* nr */ pending_requests, /* io_event */ events.get(),
        /* timeout */ NULL);
    LOG_IF(FATAL, u32(doneRequests) != pending_requests)
        << "Failed to complete all the IO requests"
        << ", expected=" << pending_requests << ", completed=" << doneRequests;
    pending_requests = 0;
    return doneRequests;
  }
  return 0;
}

void AsyncWriteBuffer::IterateFlushedBfs(
    std::function<void(BufferFrame&, u64)> callback, u64 numFlushedBfs) {
  for (u64 i = 0; i < numFlushedBfs; i++) {
    const auto slot =
        (u64(events[i].data) - u64(mWriteBuffer.Get())) / page_size;

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
