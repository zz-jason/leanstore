#pragma once

#include "BufferFrameProvider.hpp"
#include "Partition.hpp"
#include "Swip.hpp"
#include "profiling/tables/BMTable.hpp"
#include "shared-headers/Units.hpp"
#include "utils/Error.hpp"

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
private:
  friend class leanstore::LeanStore;
  friend class leanstore::profiling::BMTable;

public:
  /// The LeanStore instance.
  leanstore::LeanStore* mStore;

  /// All the managed buffer frames in the memory.
  u8* mBufferPool;

  /// Free Pages, reserved to to prevent segmentfaults.
  const u8 mNumSaftyBfs = 10;

  // total number of dram buffer frames
  u64 mNumBfs;

  /// For cooling and inflight io
  u64 mNumPartitions;

  u64 mPartitionsMask;

  std::vector<std::unique_ptr<Partition>> mPartitions;

  /// All the buffer frame provider threads.
  std::vector<std::unique_ptr<BufferFrameProvider>> mBfProviders;

public:
  BufferManager(leanstore::LeanStore* store);

  ~BufferManager();

public:
  /// Get the partition ID of the page.
  u64 GetPartitionID(PID);

  /// Get the partition of the page.
  Partition& GetPartition(PID);

  /// Randomly pick a partition.
  Partition& RandomPartition();

  /// Randomly pick a buffer frame.
  BufferFrame& RandomBufferFrame();

  /// Get a buffer frame from a random partition for new page.
  ///
  /// NOTE: The buffer frame is initialized with an unused page ID, and is
  /// exclusively locked.
  BufferFrame& AllocNewPage(TREEID treeId);

  /// Resolves the buffer frame pointed by the swipValue.
  ///
  /// @param swipGuard The latch guard on the owner of the swip. Usually a swip
  /// is owned by a btree node, and the node should be latched before resolve
  /// the swips of child nodes.
  ///
  /// @param swipValue The swip value from which to resolve the buffer frame.
  /// Usually a swip represents a btree node.
  ///
  /// @return The buffer frame regarding to the swip.
  inline BufferFrame* TryFastResolveSwip(HybridGuard& swipGuard,
                                         Swip<BufferFrame>& swipValue) {
    if (swipValue.isHOT()) {
      BufferFrame& bf = swipValue.AsBufferFrame();
      swipGuard.JumpIfModifiedByOthers();
      return &bf;
    }
    return ResolveSwipMayJump(swipGuard, swipValue);
  }

  BufferFrame* ResolveSwipMayJump(HybridGuard& swipGuard,
                                  Swip<BufferFrame>& swipValue);

  void ReclaimPage(BufferFrame& bf);

  /// Reads the page at pageId to the destination buffer. All the pages are
  /// stored in one file (mPageFd), page id (pageId) determines the offset of
  /// the pageId-th page in the underlying file:
  ///   1. offset of pageId-th page: pageId * FLAGS_page_size
  ///   2. size of each page: FLAGS_page_size
  void ReadPageSync(PID pageId, void* destination);

  /// Reads the page at pageId, returns the buffer frame containing that page.
  /// Usually called by recovery.
  BufferFrame& ReadPageSync(PID pageId);

  /// Write page to disk.
  /// usually called by recovery.
  void WritePageSync(BufferFrame&);

  /// Sync all the data written to disk, harden all the writes on mPageFd
  void SyncAllPageWrites();

  void StartBufferFrameProviders();

  void StopBufferFrameProviders();

  /// Checkpoints a buffer frame to disk. The buffer frame content is copied to
  /// a tmp memory buffer, swips in the tmp memory buffer are changed to page
  /// IDs, then the tmp memory buffer is written to the disk.
  [[nodiscard]] auto CheckpointBufferFrame(BufferFrame& bf)
      -> std::expected<void, utils::Error>;

  /// Checkpoints all the buffer frames.
  void CheckpointAllBufferFrames();

  void RecoverFromDisk();

  StringMap Serialize();

  void Deserialize(StringMap map);

  u64 ConsumedPages();

  /// Do something on all the buffer frames which satisify the condition
  void DoWithBufferFrameIf(std::function<bool(BufferFrame& bf)> condition,
                           std::function<void(BufferFrame& bf)> action);
};

} // namespace storage
} // namespace leanstore
