#pragma once

#include "leanstore/Units.hpp"
#include "leanstore/buffer-manager/BufferFrame.hpp"
#include "leanstore/utils/AsyncIo.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Result.hpp"

#include <cstdint>
#include <functional>
#include <vector>

#include <libaio.h>

namespace leanstore::storage {

//! A batched asynchronous writer for buffer frames. It batches writes to the
//! disk to reduce the number of syscalls.
//! Typical usage:
///
//!  AsyncWriteBuffer writeBuffer(fd, pageSize, maxBatchSize);
//!  while (!IsFull()) {
//!    writeBuffer.Add(bf, pageId);
//!  }
//!  writeBuffer.SubmitAll();
//!  writeBuffer.WaitAll();
//!  writeBuffer.IterateFlushedBfs([](BufferFrame& flushedBf, uint64_t
//!  flushedPsn) {
//!    // do something with flushedBf
//!  }, numFlushedBfs);
///
class AsyncWriteBuffer {
private:
  struct WriteCommand {
    const BufferFrame* mBf;
    PID mPageId;

    void Reset(const BufferFrame* bf, PID pageId) {
      mBf = bf;
      mPageId = pageId;
    }
  };

  int mFd;
  uint64_t mPageSize;
  utils::AsyncIo mAIo;

  utils::AlignedBuffer<512> mWriteBuffer;
  std::vector<WriteCommand> mWriteCommands;

public:
  AsyncWriteBuffer(int fd, uint64_t pageSize, uint64_t maxBatchSize);

  ~AsyncWriteBuffer();

  //! Check if the write buffer is full
  bool IsFull();

  //! Add a buffer frame to the write buffer:
  //! - record the buffer frame to write commands for later use
  //! - copy the page content in buffer frame to the write buffer
  //! - prepare the io request
  void Add(const BufferFrame& bf);

  //! Submit the write buffer to the AIO context to be written to the disk
  Result<uint64_t> SubmitAll();

  //! Wait for the IO request to complete
  Result<uint64_t> WaitAll();

  uint64_t GetPendingRequests() {
    return mAIo.GetNumRequests();
  }

  void IterateFlushedBfs(std::function<void(BufferFrame& flushedBf, uint64_t flushedPsn)> callback,
                         uint64_t numFlushedBfs);

private:
  void* copyToBuffer(const Page* page, size_t slot) {
    void* dest = getWriteBuffer(slot);
    std::memcpy(dest, page, mPageSize);
    return dest;
  }

  uint8_t* getWriteBuffer(size_t slot) {
    return &mWriteBuffer.Get()[slot * mPageSize];
  }
};

} // namespace leanstore::storage
