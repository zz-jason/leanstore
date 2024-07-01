#pragma once

#include "leanstore/Exceptions.hpp"
#include "leanstore/Units.hpp"
#include "leanstore/buffer-manager/BufferFrame.hpp"
#include "leanstore/buffer-manager/PageEvictor.hpp"
#include "leanstore/buffer-manager/Partition.hpp"
#include "leanstore/buffer-manager/Swip.hpp"
#include "leanstore/profiling/tables/BMTable.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/Result.hpp"

#include <expected>

#include <libaio.h>
#include <sys/mman.h>

namespace leanstore {
class LeanStore;
} // namespace leanstore

namespace leanstore::profiling {
class BMTable;
} // namespace leanstore::profiling

namespace leanstore::storage {
template <typename T>
class GuardedBufferFrame;
} // namespace leanstore::storage

namespace leanstore::storage {

//! Synchronization in Buffer Manager, terminology:
//!  - PET: Page Evictor Thread
//!  - WT: Worker Thread
//!  - P: Parent
//!  - C: Child
//!  - M: Cooling stage mutex
//!
//! Latching order for all page evictor operations (unswizzle, evict):
//!   M -> P -> C
//!
//! Latching order for all worker thread operations:
//!   - swizzle: [unlock P ->] M->P->C
//!   - coolPage: P->C->M, coolPage conflict with this order which could lead to
//!     a deadlock which we can mitigate by jumping instead of blocking in
//!     BMPlainGuard [WIP]
class BufferManager {
public:
  //! The LeanStore instance.
  leanstore::LeanStore* mStore;

  //! All the managed buffer frames in the memory.
  uint8_t* mBufferPool;

  //! Free Pages, reserved to to prevent segmentfaults.
  const uint8_t mNumSaftyBfs = 10;

  // total number of dram buffer frames
  uint64_t mNumBfs;

  //! For cooling and inflight io
  uint64_t mNumPartitions;

  uint64_t mPartitionsMask;

  std::vector<std::unique_ptr<Partition>> mPartitions;

  //! All the page evictor threads.
  std::vector<std::unique_ptr<PageEvictor>> mPageEvictors;

  BufferManager(leanstore::LeanStore* store);

  ~BufferManager();

  //! Get the partition ID of the page.
  uint64_t GetPartitionID(PID pageId) {
    return pageId & mPartitionsMask;
  }

  //! Get the partition of the page.
  Partition& GetPartition(PID pageId) {
    const uint64_t partitionId = GetPartitionID(pageId);
    return *mPartitions[partitionId];
  }

  //! Randomly pick a partition.
  Partition& RandomPartition() {
    auto partId = utils::RandomGenerator::Rand<uint64_t>(0, mNumPartitions);
    return GetPartition(partId);
  }

  //! Randomly pick a buffer frame.
  BufferFrame& RandomBufferFrame() {
    auto bfId = utils::RandomGenerator::Rand<uint64_t>(0, mNumBfs);
    auto* bfAddr = &mBufferPool[bfId * mStore->mStoreOption.mBufferFrameSize];
    return *reinterpret_cast<BufferFrame*>(bfAddr);
  }

  //! Get a buffer frame from a random partition for the new page. The buffer
  //! frame is initialized with an unused page ID, and is exclusively locked.
  BufferFrame& AllocNewPageMayJump(TREEID treeId);

  //! Resolve the swip to get the underlying buffer frame. Target page is read
  //! from disk if the swip is evicted. Called by worker threads.
  BufferFrame* ResolveSwipMayJump(HybridGuard& nodeGuard, Swip& swipInNode);

  // Pre: bf is exclusively locked
  // ATTENTION: this function unlocks it !!
  void ReclaimPage(BufferFrame& bf);

  //! Reads the page at pageId to the destination buffer. All the pages are
  //! stored in one file (mPageFd), page id (pageId) determines the offset of
  //! the pageId-th page in the underlying file:
  //!   - offset of pageId-th page: pageId * pageSize
  void ReadPageSync(PID pageId, void* destination);

  //! Reads the page at pageId, returns the buffer frame containing that page.
  //! Usually called by recovery.
  BufferFrame& ReadPageSync(PID pageId);

  //! Write page to disk.
  //! usually called by recovery.
  [[nodiscard]] Result<void> WritePageSync(BufferFrame&);

  //! Sync all the data written to disk, harden all the writes on mPageFd
  void SyncAllPageWrites() {
    fdatasync(mStore->mPageFd);
  }

  void StartPageEvictors();

  void StopPageEvictors();

  //! Checkpoints a buffer frame to disk. The buffer frame content is copied to
  //! a tmp memory buffer, swips in the tmp memory buffer are changed to page
  //! IDs, then the tmp memory buffer is written to the disk.
  [[nodiscard]] Result<void> CheckpointBufferFrame(BufferFrame& bf);

  //! Checkpoints all the buffer frames.
  [[nodiscard]] Result<void> CheckpointAllBufferFrames();

  void RecoverFromDisk();

  StringMap Serialize();

  void Deserialize(StringMap map);

  uint64_t ConsumedPages();

  //! Do something on all the buffer frames which satisify the condition
  void DoWithBufferFrameIf(std::function<bool(BufferFrame& bf)> condition,
                           std::function<void(BufferFrame& bf)> action);

private:
  Result<void> writePage(PID pageId, void* buffer) {
    utils::AsyncIo aio(1);
    const auto pageSize = mStore->mStoreOption.mPageSize;
    DEBUG_BLOCK() {
      auto* page [[maybe_unused]] = reinterpret_cast<Page*>(buffer);
      LS_DLOG("page write, pageId={}, btreeId={}", pageId, page->mBTreeId);
    }
    aio.PrepareWrite(mStore->mPageFd, buffer, pageSize, pageId * pageSize);
    if (auto res = aio.SubmitAll(); !res) {
      return std::unexpected(std::move(res.error()));
    }
    if (auto res = aio.WaitAll(); !res) {
      return std::unexpected(std::move(res.error()));
    }
    return {};
  }

  friend class leanstore::LeanStore;
  friend class leanstore::profiling::BMTable;
};

} // namespace leanstore::storage
