#pragma once

#include "buffer-manager/BufferFrame.hpp"
#include "leanstore/Units.hpp"
#include "utils/Error.hpp"
#include "utils/Misc.hpp"

#include <gflags/gflags.h>

#include <cstdint>
#include <expected>
#include <functional>
#include <vector>

#include <libaio.h>

namespace leanstore::storage {

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
///  flushedGsn) {
///    // do something with flushedBf
///  }, numFlushedBfs);
///
class AsyncWriteBuffer {
private:
  struct WriteCommand {
    BufferFrame* mBf;
    PID mPageId;

    void Reset(BufferFrame* bf, PID pageId) {
      mBf = bf;
      mPageId = pageId;
    }
  };

  io_context_t mAioCtx;
  int mFd;
  uint64_t mPageSize;
  uint64_t mMaxBatchSize;
  uint64_t mPendingRequests;

  utils::AlignedBuffer<512> mWriteBuffer;
  std::vector<WriteCommand> mWriteCommands;
  std::vector<iocb> mIocbs;
  std::vector<iocb*> mIocbPtrs;
  std::vector<io_event> mIoEvents;

public:
  AsyncWriteBuffer(int fd, uint64_t pageSize, uint64_t maxBatchSize);

  ~AsyncWriteBuffer();

  /// Check if the write buffer is full
  bool IsFull();

  /// Add a buffer frame to the write buffer:
  /// - record the buffer frame and page id to write commands for later use
  /// - copy the page content in buffer frame to the write buffer
  /// - prepare the io request
  void Add(BufferFrame& bf, PID pageId);

  /// Submit the write buffer to the AIO context to be written to the disk
  std::expected<uint64_t, utils::Error> SubmitAll();

  /// Wait for the IO request to complete
  std::expected<uint64_t, utils::Error> WaitAll();

  uint64_t GetPendingRequests() {
    return mPendingRequests;
  }

  void IterateFlushedBfs(
      std::function<void(BufferFrame& flushedBf, uint64_t flushedGsn)> callback,
      uint64_t numFlushedBfs);

private:
  uint8_t* getWriteBuffer(uint64_t slot) {
    return &mWriteBuffer.Get()[slot * mPageSize];
  }
};

} // namespace leanstore::storage
