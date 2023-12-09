#pragma once

#include "BufferFrame.hpp"
#include "Units.hpp"

#include <functional>
#include <libaio.h>
#include <list>
#include <unordered_map>

namespace leanstore {
namespace storage {

class AsyncWriteBuffer {
private:
  struct WriteCommand {
    BufferFrame* bf;
    PID mPageId;
  };

public:
  io_context_t aio_context;
  int fd;
  u64 page_size;
  u64 batch_max_size;
  u64 pending_requests = 0;

  std::unique_ptr<Page[]> write_buffer;
  std::unique_ptr<WriteCommand[]> write_buffer_commands;
  std::unique_ptr<struct iocb[]> iocbs;
  std::unique_ptr<struct iocb*[]> iocbs_ptr;
  std::unique_ptr<struct io_event[]> events;

  AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size);

  bool full();

  void AddToIOBatch(BufferFrame& bf, PID pageId);

  u64 SubmitIORequest();

  u64 WaitIORequestToComplete();

  void IterateFlushedBfs(std::function<void(BufferFrame&, u64)> callback,
                         u64 n_events);
};

} // namespace storage
} // namespace leanstore
