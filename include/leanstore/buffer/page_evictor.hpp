#pragma once

#include "leanstore/buffer/async_write_buffer.hpp"
#include "leanstore/buffer/bm_plain_guard.hpp"
#include "leanstore/buffer/buffer_frame.hpp"
#include "leanstore/buffer/partition.hpp"
#include "leanstore/buffer/swip.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/managed_thread.hpp"
#include "leanstore/utils/scoped_timer.hpp"

#include <cstdint>
#include <vector>

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

namespace leanstore {

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
    partition.AddFreeBfs(first_, last_, size_);
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
class PageEvictor : public utils::ManagedThread {
public:
  PageEvictor(LeanStore* store, const std::string& thread_name, uint64_t running_cpu,
              uint64_t num_bfs, uint64_t num_partitions,
              std::vector<std::unique_ptr<Partition>>& partitions)
      : utils::ManagedThread(store, thread_name, running_cpu),
        store_(store),
        num_bfs_(num_bfs),
        num_partitions_(num_partitions),
        partitions_(partitions),
        cool_candidate_bfs_(),
        evict_candidate_bfs_(),
        async_write_buffer_(store->page_fd_, store->store_option_->page_size_,
                            store_->store_option_->buffer_write_batch_size_),
        free_bf_list_() {
    ScopedTimer timer([this](double elapsed_ms) {
      Log::Info("PageEvictor created, num_bfs={}, page_size={}, buffer_write_batch_size={}, "
                "buffer_frame_recycle_batch_size={}, num_partitions={}, elapsed={}ms",
                num_bfs_, store_->store_option_->page_size_,
                store_->store_option_->buffer_write_batch_size_,
                store_->store_option_->buffer_frame_recycle_batch_size_, num_partitions_,
                elapsed_ms);
    });

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

  void Run4Partitions(const std::unordered_set<uint64_t>& partition_ids) {
    for (const auto& partition_id : partition_ids) {
      auto& target_partition = *partitions_[partition_id];
      if (!target_partition.NeedMoreFreeBfs()) {
        continue;
      }
      PickBufferFramesToCool(target_partition);
      PrepareAsyncWriteBuffer(target_partition);
      FlushAndRecycleBufferFrames(target_partition);
    }
  }

  auto& GetPartitions() {
    return partitions_;
  }

protected:
  void RunImpl() override;

  void RandomBufferFrames2CoolOrEvict();

  void EvictFlushedBufferFrame(BufferFrame& cooled_bf, BMOptimisticGuard& optimistic_guard,
                               Partition& target_partition);

private:
  LeanStore* store_;
  const uint64_t num_bfs_;

  const uint64_t num_partitions_;
  std::vector<std::unique_ptr<Partition>>& partitions_;

  std::vector<BufferFrame*> cool_candidate_bfs_;  // input of phase 1
  std::vector<BufferFrame*> evict_candidate_bfs_; // output of phase 1
  AsyncWriteBuffer async_write_buffer_;           // output of phase 2
  FreeBfList free_bf_list_;                       // output of phase 3
};

} // namespace leanstore