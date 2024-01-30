#pragma once

#include "BufferFrame.hpp"
#include "shared-headers/Units.hpp"
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
  u64 page_size;
  u64 batch_max_size;
  u64 pending_requests = 0;

  utils::AlignedBuffer<512> mWriteBuffer;
  std::unique_ptr<WriteCommand[]> write_buffer_commands;
  std::unique_ptr<struct iocb[]> iocbs;
  std::unique_ptr<struct iocb*[]> iocbs_ptr;
  std::unique_ptr<struct io_event[]> events;

  AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size);

  bool full();

  u8* GetWriteBuffer(u64 slot) {
    return &mWriteBuffer.Get()[slot * page_size];
  }

  void AddToIOBatch(BufferFrame& bf, PID pageId);

  u64 SubmitIORequest();

  u64 WaitIORequestToComplete();

  void IterateFlushedBfs(std::function<void(BufferFrame&, u64)> callback,
                         u64 n_events);
};

} // namespace storage
} // namespace leanstore
