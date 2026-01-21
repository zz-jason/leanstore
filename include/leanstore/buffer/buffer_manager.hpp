#pragma once

#include "leanstore/base/result.hpp"
#include "leanstore/buffer/buffer_frame.hpp"
#include "leanstore/buffer/page_evictor.hpp"
#include "leanstore/buffer/partition.hpp"
#include "leanstore/buffer/swip.hpp"
#include "leanstore/c/types.h"
#include "leanstore/exceptions.hpp"
#include "leanstore/utils/random_generator.hpp"
#include "leanstore/utils/json.hpp"

#include <expected>

#include <libaio.h>
#include <sys/mman.h>

namespace leanstore {

/// Forward declarations
class LeanStore;
template <typename T>
class GuardedBufferFrame;

/// Synchronization in Buffer Manager, terminology:
///  - PET: page evictor thread
///  - WT: worker thread
///  - P: parent
///  - C: child
///  - M: cooling stage mutex
///
/// Latching order for all page evictor operations (unswizzle, evict):
///   M -> P -> C
///
/// Latching order for all worker thread operations:
///   - swizzle: [unlock P ->] M->P->C
///   - coolPage: P->C->M, coolPage conflict with this order which could lead to
///     a deadlock which we can mitigate by jumping instead of blocking in
///     BMPlainGuard [WIP]
class BufferManager {
public:
  /// The LeanStore instance.
  LeanStore* store_;

  /// All the managed buffer frames in the memory.
  uint8_t* buffer_pool_;

  /// Free Pages, reserved to to prevent segmentfaults.
  const uint8_t num_safty_bfs_ = 10;

  // total number of dram buffer frames
  uint64_t num_bfs_;

  /// For cooling and inflight io
  uint64_t num_partitions_;

  uint64_t partitions_mask_;

  std::vector<std::unique_ptr<Partition>> partitions_;

  /// All the page evictor threads.
  std::vector<std::unique_ptr<PageEvictor>> page_evictors_;

  explicit BufferManager(LeanStore* store);

  ~BufferManager();

  /// Get the partition ID of the page.
  uint64_t GetPartitionID(lean_pid_t page_id) {
    return page_id & partitions_mask_;
  }

  /// Get the partition of the page.
  Partition& GetPartition(lean_pid_t page_id) {
    const uint64_t partition_id = GetPartitionID(page_id);
    return *partitions_[partition_id];
  }

  /// Randomly pick a partition.
  Partition& RandomPartition() {
    auto part_id = utils::RandomGenerator::Rand<uint64_t>(0, num_partitions_);
    return GetPartition(part_id);
  }

  /// Randomly pick a buffer frame.
  BufferFrame& RandomBufferFrame() {
    auto bf_id = utils::RandomGenerator::Rand<uint64_t>(0, num_bfs_);
    auto* bf_addr = &buffer_pool_[bf_id * store_->store_option_->buffer_frame_size_];
    return *reinterpret_cast<BufferFrame*>(bf_addr);
  }

  /// Get a buffer frame from a random partition for the new page. The buffer
  /// frame is initialized with an unused page ID, and is exclusively locked.
  BufferFrame& AllocNewPageMayJump(lean_treeid_t tree_id);

  BufferFrame& AllocNewPage(lean_treeid_t tree_id);

  /// Resolve the swip to get the underlying buffer frame. Target page is read
  /// from disk if the swip is evicted. Called by worker threads.
  BufferFrame* ResolveSwipMayJump(HybridGuard& node_guard, Swip& swip_in_node);

  // Pre: bf is exclusively locked
  // ATTENTION: this function unlocks it !!
  void ReclaimPage(BufferFrame& bf);

  /// Reads the page at pageId to the destination buffer. All the pages are
  /// stored in one file (page_fd_), page id (pageId) determines the offset of
  /// the pageId-th page in the underlying file:
  ///   - offset of pageId-th page: pageId * pageSize
  void ReadPageSync(lean_pid_t page_id, void* destination);

  /// Reads the page at pageId, returns the buffer frame containing that page.
  /// Usually called by recovery.
  BufferFrame& ReadPageSync(lean_pid_t page_id);

  /// Write page to disk.
  /// usually called by recovery.
  Result<void> WritePageSync(BufferFrame&);

  /// Sync all the data written to disk, harden all the writes on page_fd_
  void SyncAllPageWrites() {
    fdatasync(store_->page_fd_);
  }

  void InitFreeBfLists();

  void StartPageEvictors();

  void StopPageEvictors();

  /// Checkpoints a buffer frame to disk. The buffer frame content is copied to
  /// a tmp memory buffer, swips in the tmp memory buffer are changed to page
  /// IDs, then the tmp memory buffer is written to the disk.
  Result<void> CheckpointBufferFrame(BufferFrame& bf);

  /// Checkpoints all the buffer frames.
  Result<void> CheckpointAllBufferFrames();

  void RecoverFromDisk();

  utils::JsonObj Serialize();

  void Deserialize(const utils::JsonObj& json_obj);

  uint64_t ConsumedPages();

  /// Do something on all the buffer frames which satisify the condition
  void DoWithBufferFrameIf(std::function<bool(BufferFrame& bf)> condition,
                           std::function<void(BufferFrame& bf)> action);

private:
  Result<void> WritePageSync(lean_pid_t page_id, void* buffer) {
    utils::AsyncIo aio(1);
    const auto page_size = store_->store_option_->page_size_;
    LEAN_DEXEC() {
      auto* page [[maybe_unused]] = reinterpret_cast<Page*>(buffer);
      LEAN_DLOG("page write, pageId={}, btreeId={}", page_id, page->btree_id_);
    }
    aio.PrepareWrite(store_->page_fd_, buffer, page_size, page_id * page_size);
    if (auto res = aio.SubmitAll(); !res) {
      return std::move(res.error());
    }
    if (auto res = aio.WaitAll(); !res) {
      return std::move(res.error());
    }
    return {};
  }

  friend class LeanStore;
};

} // namespace leanstore
