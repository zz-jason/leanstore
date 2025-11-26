#include "leanstore/buffer-manager/async_write_buffer.hpp"

#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/cpp/base/result.hpp"
#include "leanstore/utils/log.hpp"

namespace leanstore {

AsyncWriteBuffer::AsyncWriteBuffer(int fd, uint64_t page_size, uint64_t max_batch_size)
    : fd_(fd),
      page_size_(page_size),
      aio_(max_batch_size),
      write_buffer_(page_size * max_batch_size),
      write_commands_(max_batch_size) {
}

AsyncWriteBuffer::~AsyncWriteBuffer() {
}

bool AsyncWriteBuffer::IsFull() {
  return aio_.IsFull();
}

void AsyncWriteBuffer::Add(const BufferFrame& bf) {
  LEAN_DCHECK(uint64_t(&bf) % 512 == 0, "BufferFrame is not aligned to 512 bytes");

  // record the written buffer frame and page id for later use
  auto page_id = bf.header_.page_id_;
  auto slot = aio_.GetNumRequests();
  write_commands_[slot].Reset(&bf, page_id);

  // copy the page content to write buffer
  auto* buffer = CopyToBuffer(&bf.page_, slot);

  aio_.PrepareWrite(fd_, buffer, page_size_, page_size_ * page_id);
}

Result<uint64_t> AsyncWriteBuffer::SubmitAll() {
  return aio_.SubmitAll();
}

Result<uint64_t> AsyncWriteBuffer::WaitAll() {
  return aio_.WaitAll();
}

void AsyncWriteBuffer::IterateFlushedBfs(
    std::function<void(BufferFrame& flushed_bf, uint64_t flushed_psn)> callback,
    uint64_t num_flushed_bfs) {
  for (uint64_t i = 0; i < num_flushed_bfs; i++) {
    const auto slot = (reinterpret_cast<uint64_t>(aio_.GetIoEvent(i)->data) -
                       reinterpret_cast<uint64_t>(write_buffer_.Get())) /
                      page_size_;
    auto* flushed_page = reinterpret_cast<Page*>(GetWriteBuffer(slot));
    auto flushed_psn = flushed_page->psn_;
    auto* flushed_bf = write_commands_[slot].bf_;
    callback(*const_cast<BufferFrame*>(flushed_bf), flushed_psn);
  }
}

} // namespace leanstore
