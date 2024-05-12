#pragma once

#include "buffer-manager/BufferFrame.hpp"
#include "buffer-manager/BufferFrameProvider.hpp"
#include "buffer-manager/Partition.hpp"
#include "buffer-manager/Swip.hpp"
#include "leanstore/Exceptions.hpp"
#include "leanstore/Units.hpp"
#include "profiling/tables/BMTable.hpp"
#include "utils/Result.hpp"

#include <expected>

#include <libaio.h>
#include <sys/mman.h>

namespace leanstore {

// Forward declaration
class LeanStore;

namespace profiling {

// Forward declaration
class BMTable;

} // namespace profiling

namespace storage {

template <typename T> class GuardedBufferFrame;

/// Notes on Synchronization in Buffer Manager.
/// Terminology:
///   PPT: Page Provider Thread
///   WT: Worker Thread
///   P: Parent
///   C: Child
///   M: Cooling stage mutex
///
/// Latching order for all PPT operations (unswizzle, evict):
///   M -> P -> C
/// Latching order for all WT operations:
///   swizzle: [unlock P ->] M -> P ->C
///   coolPage: P -> C -> M, coolPage conflict with this order which could lead
///   to a deadlock which we can mitigate by jumping instead of blocking in
///   BMPlainGuard [WIP]
///
/// TODO: revisit the comments after switching to clock replacement strategy
class BufferManager {
public:
  /// The LeanStore instance.
  leanstore::LeanStore* mStore;

  /// All the managed buffer frames in the memory.
  uint8_t* mBufferPool;

  /// Free Pages, reserved to to prevent segmentfaults.
  const uint8_t mNumSaftyBfs = 10;

  // total number of dram buffer frames
  uint64_t mNumBfs;

  /// For cooling and inflight io
  uint64_t mNumPartitions;

  uint64_t mPartitionsMask;

  std::vector<std::unique_ptr<Partition>> mPartitions;

  /// All the buffer frame provider threads.
  std::vector<std::unique_ptr<BufferFrameProvider>> mBfProviders;

  BufferManager(leanstore::LeanStore* store);

  ~BufferManager();

  /// Get the partition ID of the page.
  uint64_t GetPartitionID(PID);

  /// Get the partition of the page.
  Partition& GetPartition(PID);

  /// Randomly pick a partition.
  Partition& RandomPartition();

  /// Randomly pick a buffer frame.
  BufferFrame& RandomBufferFrame();

  /// Get a buffer frame from a random partition for the new page. The buffer
  /// frame is initialized with an unused page ID, and is exclusively locked.
  BufferFrame& AllocNewPageMayJump(TREEID treeId);

  BufferFrame* ResolveSwipMayJump(HybridGuard& nodeGuard, Swip& swipInNode);

  void ReclaimPage(BufferFrame& bf);

  /// Reads the page at pageId to the destination buffer. All the pages are
  /// stored in one file (mPageFd), page id (pageId) determines the offset of
  /// the pageId-th page in the underlying file:
  ///   - offset of pageId-th page: pageId * pageSize
  void ReadPageSync(PID pageId, void* destination);

  /// Reads the page at pageId, returns the buffer frame containing that page.
  /// Usually called by recovery.
  BufferFrame& ReadPageSync(PID pageId);

  /// Write page to disk.
  /// usually called by recovery.
  [[nodiscard]] Result<void> WritePageSync(BufferFrame&);

  /// Sync all the data written to disk, harden all the writes on mPageFd
  void SyncAllPageWrites();

  void StartBufferFrameProviders();

  void StopBufferFrameProviders();

  /// Checkpoints a buffer frame to disk. The buffer frame content is copied to
  /// a tmp memory buffer, swips in the tmp memory buffer are changed to page
  /// IDs, then the tmp memory buffer is written to the disk.
  [[nodiscard]] Result<void> CheckpointBufferFrame(BufferFrame& bf);

  /// Checkpoints all the buffer frames.
  void CheckpointAllBufferFrames();

  void RecoverFromDisk();

  StringMap Serialize();

  void Deserialize(StringMap map);

  uint64_t ConsumedPages();

  /// Do something on all the buffer frames which satisify the condition
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

} // namespace storage
} // namespace leanstore
