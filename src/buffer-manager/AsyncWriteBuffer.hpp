#pragma once

#include "buffer-manager/BufferFrame.hpp"
#include "leanstore/Units.hpp"
#include "utils/Misc.hpp"

#include <gflags/gflags.h>

#include <cstdint>
#include <functional>
#include <vector>

#include <libaio.h>

namespace leanstore::storage {

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
  std::vector<iocb[1]> mIocbsNew;
  std::vector<iocb*> mIocbPtrs;
  std::vector<io_event> mIoEvents;

public:
  AsyncWriteBuffer(int fd, uint64_t pageSize, uint64_t maxBatchSize);

  bool IsFull();

  void AddToIOBatch(BufferFrame& bf, PID pageId);

  uint64_t GetPendingRequests() {
    return mPendingRequests;
  }

  uint64_t SubmitIORequest();

  uint64_t WaitIORequestToComplete();

  void IterateFlushedBfs(
      std::function<void(BufferFrame& flushedBf, uint64_t flushedGsn)> callback,
      uint64_t numFlushedBfs);

private:
  uint8_t* getWriteBuffer(uint64_t slot) {
    return &mWriteBuffer.Get()[slot * mPageSize];
  }
};

} // namespace leanstore::storage
