#pragma once

#include "BMPlainGuard.hpp"
#include "BufferFrame.hpp"
#include "BufferFrameProvider.hpp"
#include "FreeList.hpp"
#include "Partition.hpp"
#include "PerfEvent.hpp"
#include "Swip.hpp"
#include "TreeRegistry.hpp"
#include "Units.hpp"

#include <cstring>
#include <libaio.h>
#include <list>
#include <mutex>
#include <queue>
#include <sys/mman.h>
#include <thread>
#include <unordered_map>

namespace leanstore {

// Forward declaration
class LeanStore;

namespace profiling {

// Forward declaration
class BMTable;
} // namespace profiling

namespace storage {

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
  /// All the managed buffer frames in the memory.
  BufferFrame* mBfs;

  /// FD for disk files storing pages.
  const int mPageFd;

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
  BufferManager(s32 pageFd);

  ~BufferManager();

public:
  /// Get the partition ID of the page.
  u64 getPartitionID(PID);

  /// Get the partition of the page.
  Partition& getPartition(PID);

  /// Randomly pick a partition.
  Partition& randomPartition();

  /// Randomly pick a buffer frame.
  BufferFrame& randomBufferFrame();

  /// Get a buffer frame from a random partition for new page.
  ///
  /// NOTE: The buffer frame is initialized with an unused page ID, and is
  /// exclusively locked.
  BufferFrame& AllocNewPage();

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
  inline BufferFrame* tryFastResolveSwip(HybridGuard& swipGuard,
                                         Swip<BufferFrame>& swipValue) {
    if (swipValue.isHOT()) {
      BufferFrame& bf = swipValue.AsBufferFrame();
      swipGuard.JumpIfModifiedByOthers();
      return &bf;
    } else {
      return ResolveSwipMayJump(swipGuard, swipValue);
    }
  }

  BufferFrame* ResolveSwipMayJump(HybridGuard& swipGuard,
                                  Swip<BufferFrame>& swipValue);

  void reclaimPage(BufferFrame& bf);

  /// Reads the page at pageId to the destination buffer. All the pages are
  /// stored in one file (mPageFd), page id (pageId) determines the offset of
  /// the pageId-th page in the underlying file:
  ///   1. offset of pageId-th page: pageId * PAGE_SIZE
  ///   2. size of each page: PAGE_SIZE
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
  void CheckpointBufferFrame(BufferFrame& bf);

  /// Checkpoints all the buffer frames.
  void CheckpointAllBufferFrames();

  void RecoveryFromDisk();

  StringMap serialize();

  void deserialize(StringMap map);

  u64 consumedPages();

  /// Do something on all the buffer frames which satisify the condition
  void DoWithBufferFrameIf(std::function<bool(BufferFrame& bf)> condition,
                           std::function<void(BufferFrame& bf)> action);

public:
  /// Global buffer manager singleton instance, lazily initialized.
  static std::unique_ptr<BufferManager> sInstance;

  /// Temporary hack: let workers evict the last page they used
  static thread_local BufferFrame* sTlsLastReadBf;
};

} // namespace storage
} // namespace leanstore