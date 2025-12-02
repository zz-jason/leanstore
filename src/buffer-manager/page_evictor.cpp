#include "leanstore/buffer-manager/page_evictor.hpp"

#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/buffer-manager/tree_registry.hpp"
#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/log.hpp"

namespace leanstore {

using Time = decltype(std::chrono::high_resolution_clock::now());

void PageEvictor::RunImpl() {
  while (keep_running_) {
    auto& target_partition = store_->buffer_manager_->RandomPartition();
    if (!target_partition.NeedMoreFreeBfs()) {
      continue;
    }

    // Phase 1
    PickBufferFramesToCool(target_partition);

    // Phase 2
    PrepareAsyncWriteBuffer(target_partition);

    // Phase 3
    FlushAndRecycleBufferFrames(target_partition);
  }
}

void PageEvictor::PickBufferFramesToCool(Partition& target_partition) {
  LEAN_DLOG("Phase1: PickBufferFramesToCool begins");
  SCOPED_DEFER(LEAN_DLOG("Phase1: PickBufferFramesToCool ended, evict_candidate_bfs_.size={}",
                         evict_candidate_bfs_.size()));

  // [corner cases]: prevent starving when free list is empty and cooling to
  // the required level can not be achieved
  uint64_t failed_attempts = 0;
  if (target_partition.NeedMoreFreeBfs() && failed_attempts < 10) {
    RandomBufferFrames2CoolOrEvict();
    while (cool_candidate_bfs_.size() > 0) {
      auto* cool_candidate = cool_candidate_bfs_.back();
      cool_candidate_bfs_.pop_back();
      JUMPMU_TRY() {
        BMOptimisticGuard read_guard(cool_candidate->header_.latch_);
        if (cool_candidate->ShouldRemainInMem()) {
          failed_attempts = failed_attempts + 1;
          LEAN_DLOG("Cool candidate discarded, should remain in memory, pageId={}",
                    cool_candidate->header_.page_id_);
          JUMPMU_CONTINUE;
        }
        read_guard.JumpIfModifiedByOthers();

        if (cool_candidate->header_.state_ == State::kCool) {
          evict_candidate_bfs_.push_back(cool_candidate);
          LEAN_DLOG("Find a cool buffer frame, added to evict_candidate_bfs_, "
                    "pageId={}",
                    cool_candidate->header_.page_id_);
          // TODO: maybe without failedAttempts?
          failed_attempts = failed_attempts + 1;
          LEAN_DLOG("Cool candidate discarded, it's already cool, pageId={}",
                    cool_candidate->header_.page_id_);
          JUMPMU_CONTINUE;
        }

        if (cool_candidate->header_.state_ != State::kHot) {
          failed_attempts = failed_attempts + 1;
          LEAN_DLOG("Cool candidate discarded, it's not hot, pageId={}",
                    cool_candidate->header_.page_id_);
          JUMPMU_CONTINUE;
        }
        read_guard.JumpIfModifiedByOthers();

        // Iterate all the child pages to check whether all the children are
        // evicted, otherwise pick the fist met unevicted child as the next
        // cool page candidate.
        bool all_children_evicted(true);
        bool picked_a_child(false);
        [[maybe_unused]] Time iterate_children_begin;
        [[maybe_unused]] Time iterate_children_end;

        store_->tree_registry_->IterateChildSwips(
            cool_candidate->page_.btree_id_, *cool_candidate, [&](Swip& child_swip) {
              // Ignore when it has a child in the cooling stage
              all_children_evicted = all_children_evicted && child_swip.IsEvicted();
              if (child_swip.IsHot()) {
                BufferFrame* child_bf = &child_swip.AsBufferFrame();
                read_guard.JumpIfModifiedByOthers();
                picked_a_child = true;
                cool_candidate_bfs_.push_back(child_bf);
                LEAN_DLOG("Cool candidate discarded, one of its child is hot, "
                          "pageId={}, hotChildPageId={}, the hot child is "
                          "picked as the next cool candidate",
                          cool_candidate->header_.page_id_, child_bf->header_.page_id_);
                return false;
              }
              read_guard.JumpIfModifiedByOthers();
              return true;
            });

        if (!all_children_evicted || picked_a_child) {
          LEAN_DLOG("Cool candidate discarded, not all the children are "
                    "evicted, pageId={}, allChildrenEvicted={}, pickedAChild={}",
                    cool_candidate->header_.page_id_, all_children_evicted, picked_a_child);
          failed_attempts = failed_attempts + 1;
          JUMPMU_CONTINUE;
        }

        [[maybe_unused]] Time find_parent_begin;
        [[maybe_unused]] Time find_parent_end;
        lean_treeid_t btree_id = cool_candidate->page_.btree_id_;
        read_guard.JumpIfModifiedByOthers();
        auto parent_handler = store_->tree_registry_->FindParent(btree_id, *cool_candidate);

        LEAN_DCHECK(parent_handler.parent_guard_.state_ == GuardState::kSharedOptimistic);
        LEAN_DCHECK(parent_handler.parent_guard_.latch_ != reinterpret_cast<HybridMutex*>(0x99));
        read_guard.JumpIfModifiedByOthers();
        auto check_result = store_->tree_registry_->CheckSpaceUtilization(
            cool_candidate->page_.btree_id_, *cool_candidate);
        if (check_result == SpaceCheckResult::kRestartSameBf ||
            check_result == SpaceCheckResult::kPickAnotherBf) {
          LEAN_DLOG("Cool candidate discarded, space check failed, "
                    "pageId={}, checkResult={}",
                    cool_candidate->header_.page_id_, (uint64_t)check_result);
          JUMPMU_CONTINUE;
        }
        read_guard.JumpIfModifiedByOthers();

        // Suitable page founds, lets cool
        const lean_pid_t page_id [[maybe_unused]] = cool_candidate->header_.page_id_;
        {
          // writeGuard can only be acquired and released while the partition
          // mutex is locked
          BMExclusiveUpgradeIfNeeded parent_write_guard(parent_handler.parent_guard_);
          BMExclusiveGuard write_guard(read_guard);

          LEAN_DCHECK(cool_candidate->header_.page_id_ == page_id);
          LEAN_DCHECK(cool_candidate->header_.state_ == State::kHot);
          LEAN_DCHECK(cool_candidate->header_.is_being_written_back_ == false);
          LEAN_DCHECK(parent_handler.parent_guard_.version_ ==
                      parent_handler.parent_guard_.latch_->GetVersion());
          LEAN_DCHECK(parent_handler.child_swip_.bf_ == cool_candidate);

          // mark the buffer frame in cool state
          cool_candidate->header_.state_ = State::kCool;
          // mark the swip to the buffer frame to cool state
          parent_handler.child_swip_.Cool();
          LEAN_DLOG("Cool candidate find, state changed to cool, pageId={}",
                    cool_candidate->header_.page_id_);
        }

        failed_attempts = 0;
      }
      JUMPMU_CATCH() {
        LEAN_DLOG("Cool candidate discarded, optimistic latch failed, "
                  "someone has modified the buffer frame during cool "
                  "validation, pageId={}",
                  cool_candidate->header_.page_id_);
      }
    }
  }
}

void PageEvictor::RandomBufferFrames2CoolOrEvict() {
  cool_candidate_bfs_.clear();
  for (auto i = 0u; i < store_->store_option_->buffer_frame_recycle_batch_size_; i++) {
    auto* random_bf = &store_->buffer_manager_->RandomBufferFrame();
    DoNotOptimize(random_bf->header_.state_);
    cool_candidate_bfs_.push_back(random_bf);
  }
}

void PageEvictor::PrepareAsyncWriteBuffer(Partition& target_partition) {
  LEAN_DLOG("Phase2: PrepareAsyncWriteBuffer begins");
  SCOPED_DEFER(LEAN_DLOG("Phase2: PrepareAsyncWriteBuffer ended, "
                         "async_write_buffer_.PendingRequests={}",
                         async_write_buffer_.GetPendingRequests()));

  free_bf_list_.Reset();
  for (auto* cooled_bf : evict_candidate_bfs_) {
    JUMPMU_TRY() {
      BMOptimisticGuard optimistic_guard(cooled_bf->header_.latch_);
      // Check if the BF got swizzled in or unswizzle another time in another
      // partition
      if (cooled_bf->header_.state_ != State::kCool || cooled_bf->header_.is_being_written_back_) {
        LEAN_DLOG("COOLed buffer frame discarded, pageId={}, IsCool={}, "
                  "isBeingWrittenBack={}",
                  cooled_bf->header_.page_id_, cooled_bf->header_.state_ == State::kCool,
                  cooled_bf->header_.is_being_written_back_.load());
        JUMPMU_CONTINUE;
      }

      // Prevent evicting a page that already has an IO Frame with (possibly)
      // threads working on it.
      {
        lean_pid_t cooled_page_id = cooled_bf->header_.page_id_;
        auto partition_id = store_->buffer_manager_->GetPartitionID(cooled_page_id);
        Partition& partition = *partitions_[partition_id];
        if (partition.IsBeingReadBack(cooled_page_id)) {
          LEAN_DLOG("COOLed buffer frame discarded, is being read back, pageId={}, partitionId={}",
                    cooled_page_id, partition_id);
          JUMPMU_CONTINUE;
        }
      }

      // Evict clean pages. They can be safely cleared in memory without
      // writing any bytes back to the underlying disk.
      if (!cooled_bf->IsDirty()) {
        EvictFlushedBufferFrame(*cooled_bf, optimistic_guard, target_partition);
        LEAN_DLOG("COOLed buffer frame is not dirty, reclaim directly, pageId={}",
                  cooled_bf->header_.page_id_);
        JUMPMU_CONTINUE;
      }

      // Async write dirty pages back. They should keep in memory and stay in
      // cooling stage until all the contents are written back to the
      // underlying disk.
      if (async_write_buffer_.IsFull()) {
        LEAN_DLOG("Async write buffer is full, bufferSize={}",
                  async_write_buffer_.GetPendingRequests());
        JUMPMU_BREAK;
      }

      BMExclusiveGuard exclusive_guard(optimistic_guard);
      LEAN_DCHECK(!cooled_bf->header_.is_being_written_back_);
      cooled_bf->header_.is_being_written_back_.store(true, std::memory_order_release);

      // performs crc check if necessary
      if (store_->store_option_->enable_buffer_crc_check_) {
        cooled_bf->header_.crc_ = cooled_bf->page_.CRC();
      }

      // TODO: preEviction callback according to lean_treeid_t
      async_write_buffer_.Add(*cooled_bf);
      LEAN_DLOG("COOLed buffer frame is added to async write buffer, "
                "pageId={}, bufferSize={}",
                cooled_bf->header_.page_id_, async_write_buffer_.GetPendingRequests());
    }
    JUMPMU_CATCH() {
      LEAN_DLOG("COOLed buffer frame discarded, optimistic latch failed, "
                "someone has modified the buffer frame during cool validation, "
                "pageId={}",
                cooled_bf->header_.page_id_);
    }
  }

  evict_candidate_bfs_.clear();
}

void PageEvictor::FlushAndRecycleBufferFrames(Partition& target_partition) {
  LEAN_DLOG("Phase3: FlushAndRecycleBufferFrames begins");
  SCOPED_DEFER(LEAN_DLOG("Phase3: FlushAndRecycleBufferFrames ended"));

  auto result = async_write_buffer_.SubmitAll();
  if (!result) {
    Log::Error("Failed to submit IO, error={}", result.error().ToString());
    return;
  }

  result = async_write_buffer_.WaitAll();
  if (!result) {
    Log::Error("Failed to wait IO request to complete, error={}", result.error().ToString());
    return;
  }

  auto num_flushed_bfs = result.value();
  async_write_buffer_.IterateFlushedBfs(
      [&](BufferFrame& written_bf, uint64_t flushed_psn) {
        JUMPMU_TRY() {
          // When the written back page is being exclusively locked, we
          // should rather waste the write and move on to another page
          // Instead of waiting on its latch because of the likelihood that
          // a data structure implementation keeps holding a parent latch
          // while trying to acquire a new page
          BMOptimisticGuard optimistic_guard(written_bf.header_.latch_);
          BMExclusiveGuard exclusive_guard(optimistic_guard);
          LEAN_DCHECK(written_bf.header_.is_being_written_back_);
          LEAN_DCHECK(written_bf.header_.flushed_page_version_ < flushed_psn);

          // For recovery, so much has to be done here...
          written_bf.header_.flushed_page_version_ = flushed_psn;
          written_bf.header_.is_being_written_back_ = false;
        }
        JUMPMU_CATCH() {
          written_bf.header_.crc_ = 0;
          written_bf.header_.is_being_written_back_.store(false, std::memory_order_release);
        }

        JUMPMU_TRY() {
          BMOptimisticGuard optimistic_guard(written_bf.header_.latch_);
          if (written_bf.header_.state_ == State::kCool &&
              !written_bf.header_.is_being_written_back_ && !written_bf.IsDirty()) {
            EvictFlushedBufferFrame(written_bf, optimistic_guard, target_partition);
          }
        }
        JUMPMU_CATCH() {
        }
      },
      num_flushed_bfs);

  if (free_bf_list_.Size()) {
    free_bf_list_.PopTo(target_partition);
  }
}

void PageEvictor::EvictFlushedBufferFrame(BufferFrame& cooled_bf,
                                          BMOptimisticGuard& optimistic_guard,
                                          Partition& target_partition) {
  lean_treeid_t btree_id = cooled_bf.page_.btree_id_;
  optimistic_guard.JumpIfModifiedByOthers();
  ParentSwipHandler parent_handler = store_->tree_registry_->FindParent(btree_id, cooled_bf);

  LEAN_DCHECK(parent_handler.parent_guard_.state_ == GuardState::kSharedOptimistic);
  BMExclusiveUpgradeIfNeeded parent_write_guard(parent_handler.parent_guard_);
  optimistic_guard.guard_.ToExclusiveMayJump();

  if (store_->store_option_->enable_buffer_crc_check_ && cooled_bf.header_.crc_) {
    LEAN_DCHECK(cooled_bf.page_.CRC() == cooled_bf.header_.crc_);
  }
  LEAN_DCHECK(!cooled_bf.IsDirty());
  LEAN_DCHECK(!cooled_bf.header_.is_being_written_back_);
  LEAN_DCHECK(cooled_bf.header_.state_ == State::kCool);
  LEAN_DCHECK(parent_handler.child_swip_.IsCool());

  parent_handler.child_swip_.Evict(cooled_bf.header_.page_id_);

  // Reclaim buffer frame
  cooled_bf.Reset();
  cooled_bf.header_.latch_.UnlockExclusively();

  free_bf_list_.PushFront(cooled_bf);
  if (free_bf_list_.Size() <= std::min<uint64_t>(store_->store_option_->worker_threads_, 128)) {
    free_bf_list_.PopTo(target_partition);
  }
};

} // namespace leanstore