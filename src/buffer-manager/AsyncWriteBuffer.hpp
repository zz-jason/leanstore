#pragma once

#include "BufferFrame.hpp"
#include "leanstore/Units.hpp"
#include "utils/Misc.hpp"

#include <functional>

#include <libaio.h>

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
  uint64_t page_size;
  uint64_t batch_max_size;
  uint64_t pending_requests = 0;

  utils::AlignedBuffer<512> mWriteBuffer;
  std::unique_ptr<WriteCommand[]> write_buffer_commands;
  std::unique_ptr<struct iocb[]> iocbs;
  std::unique_ptr<struct iocb*[]> iocbs_ptr;
  std::unique_ptr<struct io_event[]> events;

  AsyncWriteBuffer(int fd, uint64_t page_size, uint64_t batch_max_size);

  bool full();

  uint8_t* GetWriteBuffer(uint64_t slot) {
    return &mWriteBuffer.Get()[slot * page_size];
  }

  void AddToIOBatch(BufferFrame& bf, PID pageId);

  uint64_t SubmitIORequest();

  uint64_t WaitIORequestToComplete();

  void IterateFlushedBfs(std::function<void(BufferFrame&, uint64_t)> callback,
                         uint64_t n_events);
};

} // namespace storage
} // namespace leanstore
