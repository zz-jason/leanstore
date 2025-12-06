#pragma once

#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/common/types.h"
#include "leanstore/cpp/base/result.hpp"
#include "leanstore/cpp/io/async_io.hpp"
#include "leanstore/utils/misc.hpp"

#include <cstdint>
#include <functional>
#include <vector>

#include <libaio.h>

namespace leanstore {

/// A batched asynchronous writer for buffer frames. It batches writes to the
/// disk to reduce the number of syscalls.
/// Typical usage:
///
///  AsyncWriteBuffer writeBuffer(fd, pageSize, maxBatchSize);
///  while (!IsFull()) {
///    writeBuffer.Add(bf, pageId);
///  }
///  writeBuffer.SubmitAll();
///  writeBuffer.WaitAll();
///  writeBuffer.IterateFlushedBfs([](BufferFrame& flushedBf, uint64_t
///  flushedPsn) {
///    // do something with flushedBf
///  }, numFlushedBfs);
///
class AsyncWriteBuffer {
private:
  struct WriteCommand {
    const BufferFrame* bf_;
    lean_pid_t page_id_;

    void Reset(const BufferFrame* bf, lean_pid_t page_id) {
      bf_ = bf;
      page_id_ = page_id;
    }
  };

  int fd_;
  uint64_t page_size_;
  utils::AsyncIo aio_;

  utils::AlignedBuffer<512> write_buffer_;
  std::vector<WriteCommand> write_commands_;

public:
  AsyncWriteBuffer(int fd, uint64_t page_size, uint64_t max_batch_size);

  ~AsyncWriteBuffer();

  /// Check if the write buffer is full
  bool IsFull();

  /// Add a buffer frame to the write buffer:
  /// - record the buffer frame to write commands for later use
  /// - copy the page content in buffer frame to the write buffer
  /// - prepare the io request
  void Add(const BufferFrame& bf);

  /// Submit the write buffer to the AIO context to be written to the disk
  Result<uint64_t> SubmitAll();

  /// Wait for the IO request to complete
  Result<uint64_t> WaitAll();

  uint64_t GetPendingRequests() {
    return aio_.GetNumRequests();
  }

  void IterateFlushedBfs(
      std::function<void(BufferFrame& flushed_bf, uint64_t flushed_psn)> callback,
      uint64_t num_flushed_bfs);

private:
  void* CopyToBuffer(const Page* page, size_t slot) {
    void* dest = GetWriteBuffer(slot);
    std::memcpy(dest, page, page_size_);
    return dest;
  }

  uint8_t* GetWriteBuffer(size_t slot) {
    return &write_buffer_.Get()[slot * page_size_];
  }
};

} // namespace leanstore
