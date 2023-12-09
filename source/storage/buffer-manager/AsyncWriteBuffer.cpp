#include "AsyncWriteBuffer.hpp"
#include "Exceptions.hpp"
#include "Tracing.hpp"
#include "profiling/counters/WorkerCounters.hpp"

#include "gflags/gflags.h"
#include <glog/logging.h>

#include <cstring>
#include <signal.h>

namespace leanstore {
namespace storage {

AsyncWriteBuffer::AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size)
    : fd(fd), page_size(page_size), batch_max_size(batch_max_size) {
  write_buffer = make_unique<Page[]>(batch_max_size);
  write_buffer_commands = make_unique<WriteCommand[]>(batch_max_size);
  iocbs = make_unique<struct iocb[]>(batch_max_size);
  iocbs_ptr = make_unique<struct iocb*[]>(batch_max_size);
  events = make_unique<struct io_event[]>(batch_max_size);

  memset(&aio_context, 0, sizeof(aio_context));
  const int ret = io_setup(batch_max_size, &aio_context);
  if (ret != 0) {
    throw ex::GenericException("io_setup failed, ret code = " +
                               std::to_string(ret));
  }
}

bool AsyncWriteBuffer::full() {
  if (pending_requests >= batch_max_size - 2) {
    return true;
  } else {
    return false;
  }
}

void AsyncWriteBuffer::AddToIOBatch(BufferFrame& bf, PID pageId) {
  DCHECK(!full());
  DCHECK(u64(&bf.page) % 512 == 0);
  DCHECK(pending_requests <= batch_max_size);
  COUNTERS_BLOCK() {
    WorkerCounters::myCounters().dt_page_writes[bf.page.mBTreeId]++;
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
  std::memcpy(&write_buffer[slot], &bf.page, page_size);
  void* write_buffer_slot_ptr = &write_buffer[slot];
  io_prep_pwrite(/* iocb */ &iocbs[slot], /* fd */ fd,
                 /* buf */ write_buffer_slot_ptr, /* count */ page_size,
                 /* offset */ page_size * pageId);
  iocbs[slot].data = write_buffer_slot_ptr;
  iocbs_ptr[slot] = &iocbs[slot];
}

u64 AsyncWriteBuffer::SubmitIORequest() {
  if (pending_requests > 0) {
    int ret_code = io_submit(aio_context, pending_requests, iocbs_ptr.get());
    DCHECK(ret_code == s32(pending_requests));
    return pending_requests;
  }
  return 0;
}

u64 AsyncWriteBuffer::WaitIORequestToComplete() {
  if (pending_requests > 0) {
    const int done_requests = io_getevents(
        /* ctx */ aio_context, /* min_nr */ pending_requests,
        /* nr */ pending_requests, /* io_event */ events.get(),
        /* timeout */ NULL);
    LOG_IF(FATAL, u32(done_requests) != pending_requests)
        << "Failed to complete all the IO requests"
        << ", expected=" << pending_requests << ", completed=" << done_requests;
    pending_requests = 0;
    return done_requests;
  }
  return 0;
}

void AsyncWriteBuffer::IterateFlushedBfs(
    std::function<void(BufferFrame&, u64)> callback, u64 numFlushedBfs) {
  for (u64 i = 0; i < numFlushedBfs; i++) {
    const auto slot =
        (u64(events[i].data) - u64(write_buffer.get())) / page_size;

    DCHECK(events[i].res == page_size);
    explainIfNot(events[i].res2 == 0);
    auto flushedLSN = write_buffer[slot].mPSN;
    auto flushedBf = write_buffer_commands[slot].bf;
    callback(*flushedBf, flushedLSN);
  }
}

} // namespace storage
} // namespace leanstore
