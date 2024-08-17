#pragma once

#include "leanstore/LeanStore.hpp"
#include "leanstore/buffer-manager/AsyncWriteBuffer.hpp"
#include "leanstore/buffer-manager/BMPlainGuard.hpp"
#include "leanstore/buffer-manager/BufferFrame.hpp"
#include "leanstore/buffer-manager/FreeList.hpp"
#include "leanstore/buffer-manager/Partition.hpp"
#include "leanstore/buffer-manager/Swip.hpp"
#include "leanstore/utils/UserThread.hpp"

#include <cstdint>

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

namespace leanstore::storage {

class FreeBfList {
private:
  BufferFrame* mFirst = nullptr;

  BufferFrame* mLast = nullptr;

  uint64_t mSize = 0;

public:
  void Reset() {
    mFirst = nullptr;
    mLast = nullptr;
    mSize = 0;
  }

  void PopTo(Partition& partition) {
    partition.mFreeBfList.PushFront(mFirst, mLast, mSize);
    Reset();
  }

  uint64_t Size() {
    return mSize;
  }

  void PushFront(BufferFrame& bf) {
    bf.mHeader.mNextFreeBf = mFirst;
    mFirst = &bf;
    mSize++;
    if (mLast == nullptr) {
      mLast = &bf;
    }
  }
};

//! Evicts in-memory pages, provides free BufferFrames for partitions.
class PageEvictor : public utils::UserThread {
public:
  leanstore::LeanStore* mStore;
  const uint64_t mNumBfs;
  uint8_t* mBufferPool;

  const uint64_t mNumPartitions;
  const uint64_t mPartitionsMask;
  std::vector<std::unique_ptr<Partition>>& mPartitions;

  const int mFD;

  std::vector<BufferFrame*> mCoolCandidateBfs;  // input of phase 1
  std::vector<BufferFrame*> mEvictCandidateBfs; // output of phase 1
  AsyncWriteBuffer mAsyncWriteBuffer;           // output of phase 2
  FreeBfList mFreeBfList;                       // output of phase 3

public:
  PageEvictor(leanstore::LeanStore* store, const std::string& threadName, uint64_t runningCPU,
              uint64_t numBfs, uint8_t* bfs, uint64_t numPartitions, uint64_t partitionMask,
              std::vector<std::unique_ptr<Partition>>& partitions)
      : utils::UserThread(store, threadName, runningCPU),
        mStore(store),
        mNumBfs(numBfs),
        mBufferPool(bfs),
        mNumPartitions(numPartitions),
        mPartitionsMask(partitionMask),
        mPartitions(partitions),
        mFD(store->mPageFd),
        mCoolCandidateBfs(),
        mEvictCandidateBfs(),
        mAsyncWriteBuffer(store->mPageFd, store->mStoreOption->mPageSize,
                          mStore->mStoreOption->mBufferWriteBatchSize),
        mFreeBfList() {
    mCoolCandidateBfs.reserve(mStore->mStoreOption->mBufferFrameRecycleBatchSize);
    mEvictCandidateBfs.reserve(mStore->mStoreOption->mBufferFrameRecycleBatchSize);
  }

  // no copy and assign
  PageEvictor(const PageEvictor&) = delete;
  PageEvictor& operator=(const PageEvictor&) = delete;

  // no move construct and assign
  PageEvictor(PageEvictor&& other) = delete;
  PageEvictor& operator=(PageEvictor&& other) = delete;

  ~PageEvictor() override {
    Stop();
  }

public:
  //! Randomly picks a batch of buffer frames from the whole memory, gather the
  //! cool buffer frames for the next round to evict, cools the hot buffer
  //! frames if all their children are evicted.
  //!
  //! NOTE:
  //! 1. Only buffer frames that are cool are added in the eviction batch and
  //!    being evicted in the next phase.
  //!
  //! 2. Only buffer frames that are hot and all the children are evicted
  //!    can be cooled at this phase. Buffer frames cooled at this phase won't
  //!    be evicted in the next phase directly, they will be added to the
  //!    eviction batch in the future round of PickBufferFramesToCool() if they
  //!    stay cool at that time.
  //!
  //! @param targetPartition the target partition which needs more buffer frames
  //! to load pages for worker threads.
  void PickBufferFramesToCool(Partition& targetPartition);

  //! Find cool candidates and cool them
  //!   - hot and all the children are evicted: cool it
  //!   - hot but one of the children is cool: choose the child and restart
  //!   - cool: evict it
  void PrepareAsyncWriteBuffer(Partition& targetPartition);

  //! Writes all picked pages, push free BufferFrames to target partition.
  void FlushAndRecycleBufferFrames(Partition& targetPartition);

protected:
  void runImpl() override;

private:
  void randomBufferFramesToCoolOrEvict();

  void evictFlushedBf(BufferFrame& cooledBf, BMOptimisticGuard& optimisticGuard,
                      Partition& targetPartition);
};

} // namespace leanstore::storage