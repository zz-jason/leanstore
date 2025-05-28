#pragma once

#include "leanstore/buffer-manager/async_write_buffer.hpp"
#include "leanstore/buffer-manager/bm_plain_guard.hpp"
#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/buffer-manager/free_list.hpp"
#include "leanstore/buffer-manager/partition.hpp"
#include "leanstore/buffer-manager/swip.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/user_thread.hpp"

#include <cstdint>

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

namespace leanstore::storage {

class FreeBfList {
private:
  BufferFrame* first_ = nullptr;

  BufferFrame* last_ = nullptr;

  uint64_t size_ = 0;

public:
  void Reset() {
    first_ = nullptr;
    last_ = nullptr;
    size_ = 0;
  }

  void PopTo(Partition& partition) {
    partition.free_bf_list_.PushFront(first_, last_, size_);
    Reset();
  }

  uint64_t Size() {
    return size_;
  }

  void PushFront(BufferFrame& bf) {
    bf.header_.next_free_bf_ = first_;
    first_ = &bf;
    size_++;
    if (last_ == nullptr) {
      last_ = &bf;
    }
  }
};

/// Evicts in-memory pages, provides free BufferFrames for partitions.
class PageEvictor : public utils::UserThread {
public:
  leanstore::LeanStore* store_;
  const uint64_t num_bfs_;
  uint8_t* buffer_pool_;

  const uint64_t num_partitions_;
  const uint64_t partitions_mask_;
  std::vector<std::unique_ptr<Partition>>& partitions_;

  const int fd_;

  std::vector<BufferFrame*> cool_candidate_bfs_;  // input of phase 1
  std::vector<BufferFrame*> evict_candidate_bfs_; // output of phase 1
  AsyncWriteBuffer async_write_buffer_;           // output of phase 2
  FreeBfList free_bf_list_;                       // output of phase 3

public:
  PageEvictor(leanstore::LeanStore* store, const std::string& thread_name, uint64_t running_cpu,
              uint64_t num_bfs, uint8_t* bfs, uint64_t num_partitions, uint64_t partition_mask,
              std::vector<std::unique_ptr<Partition>>& partitions)
      : utils::UserThread(store, thread_name, running_cpu),
        store_(store),
        num_bfs_(num_bfs),
        buffer_pool_(bfs),
        num_partitions_(num_partitions),
        partitions_mask_(partition_mask),
        partitions_(partitions),
        fd_(store->page_fd_),
        cool_candidate_bfs_(),
        evict_candidate_bfs_(),
        async_write_buffer_(store->page_fd_, store->store_option_->page_size_,
                            store_->store_option_->buffer_write_batch_size_),
        free_bf_list_() {
    cool_candidate_bfs_.reserve(store_->store_option_->buffer_frame_recycle_batch_size_);
    evict_candidate_bfs_.reserve(store_->store_option_->buffer_frame_recycle_batch_size_);
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
  /// Randomly picks a batch of buffer frames from the whole memory, gather the
  /// cool buffer frames for the next round to evict, cools the hot buffer
  /// frames if all their children are evicted.
  ///
  /// NOTE:
  /// 1. Only buffer frames that are cool are added in the eviction batch and
  ///    being evicted in the next phase.
  ///
  /// 2. Only buffer frames that are hot and all the children are evicted
  ///    can be cooled at this phase. Buffer frames cooled at this phase won't
  ///    be evicted in the next phase directly, they will be added to the
  ///    eviction batch in the future round of PickBufferFramesToCool() if they
  ///    stay cool at that time.
  ///
  /// @param targetPartition the target partition which needs more buffer frames
  /// to load pages for worker threads.
  void PickBufferFramesToCool(Partition& target_partition);

  /// Find cool candidates and cool them
  ///   - hot and all the children are evicted: cool it
  ///   - hot but one of the children is cool: choose the child and restart
  ///   - cool: evict it
  void PrepareAsyncWriteBuffer(Partition& target_partition);

  /// Writes all picked pages, push free BufferFrames to target partition.
  void FlushAndRecycleBufferFrames(Partition& target_partition);

protected:
  void run_impl() override;

private:
  void random_buffer_frames_to_cool_or_evict();

  void evict_flushed_bf(BufferFrame& cooled_bf, BMOptimisticGuard& optimistic_guard,
                        Partition& target_partition);
};

} // namespace leanstore::storage