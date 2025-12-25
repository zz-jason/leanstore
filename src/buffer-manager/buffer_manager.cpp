#include "leanstore/buffer-manager/buffer_manager.hpp"

#include "coroutine/coro_executor.hpp"
#include "coroutine/lean_mutex.hpp"
#include "failpoint/failpoint.hpp"
#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/buffer-manager/tree_registry.hpp"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/concurrency/group_committer.hpp"
#include "leanstore/concurrency/recovery.hpp"
#include "leanstore/cpp/base/error.hpp"
#include "leanstore/cpp/base/jump_mu.hpp"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/cpp/config/store_paths.hpp"
#include "leanstore/cpp/io/async_io.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/sync/hybrid_mutex.hpp"
#include "leanstore/sync/scoped_hybrid_guard.hpp"
#include "leanstore/utils/managed_thread.hpp"
#ifndef LEAN_ENABLE_CORO
#include "leanstore/utils/parallelize.hpp"
#endif
#include "leanstore/cpp/base/small_vector.hpp"
#include "utils/scoped_timer.hpp"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <format>
#include <utility>
#include <vector>

#include <sys/mman.h>
#include <unistd.h>

namespace leanstore {

BufferManager::BufferManager(LeanStore* store) : store_(store) {
  auto bp_size = store_->store_option_->buffer_pool_size_;
  auto bf_size = store_->store_option_->buffer_frame_size_;
  num_bfs_ = bp_size / bf_size;
  const uint64_t total_mem_size = bf_size * (num_bfs_ + num_safty_bfs_);

  // Init buffer pool with zero-initialized buffer frames. Use mmap with flags
  // MAP_PRIVATE and MAP_ANONYMOUS, no underlying file descriptor to allocate
  // totalmemSize buffer pool with zero-initialized contents.
  void* underlying_buf =
      mmap(nullptr, total_mem_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (underlying_buf == MAP_FAILED) {
    Log::Fatal("Failed to allocate memory for the buffer pool, bufferPoolSize={}, totalMemSize={}",
               store_->store_option_->buffer_pool_size_, total_mem_size);
  }

  buffer_pool_ = reinterpret_cast<uint8_t*>(underlying_buf);
  madvise(buffer_pool_, total_mem_size, MADV_HUGEPAGE);
  madvise(buffer_pool_, total_mem_size, MADV_DONTFORK);

  // Initialize partitions_
  num_partitions_ = store_->store_option_->num_partitions_;
  partitions_mask_ = num_partitions_ - 1;
  const uint64_t free_bfs_limit_per_partition =
      std::ceil((store_->store_option_->free_pct_ * 1.0 * num_bfs_ / 100.0) / num_partitions_);
  for (uint64_t i = 0; i < num_partitions_; i++) {
    partitions_.push_back(
        std::make_unique<Partition>(i, num_partitions_, free_bfs_limit_per_partition));
  }
  Log::Info("Init buffer manager, IO partitions={}, freeBfsLimitPerPartition={}", num_partitions_,
            free_bfs_limit_per_partition);
}

void BufferManager::InitFreeBfLists() {
  auto spread_free_bfs = [&](uint64_t begin, uint64_t end) {
    uint64_t partition_id = 0;
    for (uint64_t i = begin; i < end; i++) {
      auto& partition = GetPartition(partition_id);
      auto* bf_addr = &buffer_pool_[i * store_->store_option_->buffer_frame_size_];
      partition.AddFreeBf(*new (bf_addr) BufferFrame());
      partition_id = (partition_id + 1) % num_partitions_;
    }
  };

#ifdef LEAN_ENABLE_CORO
  store_->ParallelRange(num_bfs_, std::move(spread_free_bfs));
#else
  utils::Parallelize::ParallelRange(num_bfs_, std::move(spread_free_bfs));
#endif
}

void BufferManager::StartPageEvictors() {
  auto num_buffer_providers = store_->store_option_->num_buffer_providers_;
  // make it optional for pure in-memory experiments
  if (num_buffer_providers <= 0) {
    return;
  }

  LEAN_DCHECK(num_buffer_providers <= num_partitions_);
  page_evictors_.reserve(num_buffer_providers);
  for (auto i = 0U; i < num_buffer_providers; ++i) {
    std::string thread_name = "PageEvictor";
    if (num_buffer_providers > 1) {
      thread_name += std::to_string(i);
    }

    auto& store_option = store_->store_option_;
    auto running_cpu = store_option->worker_threads_ + store_option->enable_wal_ + i;
    page_evictors_.push_back(std::make_unique<PageEvictor>(store_, thread_name, running_cpu,
                                                           num_bfs_, num_partitions_, partitions_));
  }

  for (auto i = 0U; i < page_evictors_.size(); ++i) {
    page_evictors_[i]->Start();
  }
}

namespace {
constexpr auto kMaxPageId = "max_pid";
}; // namespace

utils::JsonObj BufferManager::Serialize() {
  // TODO: correctly serialize ranges of used pages
  lean_pid_t max_page_id = 0;
  for (uint64_t i = 0; i < num_partitions_; i++) {
    max_page_id = std::max<lean_pid_t>(GetPartition(i).GetNextPageId(), max_page_id);
  }

  utils::JsonObj json_obj;
  json_obj.AddUint64(kMaxPageId, max_page_id);
  return json_obj;
}

void BufferManager::Deserialize(const utils::JsonObj& json_obj) {
  lean_pid_t max_page_id = *json_obj.GetUint64(kMaxPageId);
  max_page_id = (max_page_id + (num_partitions_ - 1)) & ~(num_partitions_ - 1);
  for (uint64_t i = 0; i < num_partitions_; i++) {
    GetPartition(i).SetNextPageId(max_page_id + i);
  }
}

Result<void> BufferManager::CheckpointAllBufferFrames() {
  ScopedTimer timer([](double elapsed_ms) {
    Log::Info("CheckpointAllBufferFrames finished, timeElasped={:.6f}ms", elapsed_ms);
  });
  Log::Info("CheckpointAllBufferFrames, num_bfs_={}", num_bfs_);

  LEAN_FAIL_POINT(FailPoint::kSkipCheckpointAll, return Error::General("Checkpoint skipped"););

  auto checkpoint_func = [&](uint64_t begin, uint64_t end) {
    const auto buffer_frame_size = store_->store_option_->buffer_frame_size_;
    const auto page_size = store_->store_option_->page_size_;

    // the underlying batch for aio
    const auto batch_capacity = store_->store_option_->buffer_write_batch_size_;
    auto small_buffer = SmallBuffer512Aligned<4096 * 1024>(page_size * batch_capacity);
    auto* buffer = small_buffer.Data();
    auto batch_size = 0U;

    // the aio itself
    utils::AsyncIo aio(batch_capacity);

    for (uint64_t i = begin; i < end;) {
      // collect a batch of pages for async write
      for (; batch_size < batch_capacity && i < end; i++) {
        auto& bf = *reinterpret_cast<BufferFrame*>(&buffer_pool_[i * buffer_frame_size]);
        if (!bf.IsFree()) {
          auto* tmp_buffer = buffer + batch_size * page_size;
          auto page_offset = bf.header_.page_id_ * page_size;
          store_->tree_registry_->Checkpoint(bf.page_.btree_id_, bf, tmp_buffer);
          aio.PrepareWrite(store_->page_fd_, tmp_buffer, page_size, page_offset);
          bf.header_.flushed_page_version_ = bf.page_.page_version_;
          batch_size++;
        }
      }

      // write the batch of pages
      if (auto res = aio.SubmitAll(); !res) {
        Log::Fatal("Failed to submit aio, error={}", res.error().ToString());
      }
      if (auto res = aio.WaitAll(); !res) {
        Log::Fatal("Failed to wait aio, error={}", res.error().ToString());
      }

      // reset batch size
      batch_size = 0;
    }
  };

#ifdef LEAN_ENABLE_CORO
  store_->ParallelRange(num_bfs_, std::move(checkpoint_func));
#else
  StopPageEvictors();
  utils::Parallelize::ParallelRange(num_bfs_, std::move(checkpoint_func));
#endif

  return {};
}

Result<void> BufferManager::CheckpointBufferFrame(BufferFrame& bf) {
  auto small_buffer = SmallBuffer512Aligned<4096>(store_->store_option_->page_size_);
  auto* buffer = small_buffer.Data();

  bf.header_.latch_.LockExclusively();
  if (!bf.IsFree()) {
    store_->tree_registry_->Checkpoint(bf.page_.btree_id_, bf, buffer);
    auto res = WritePageSync(bf.header_.page_id_, buffer);
    if (!res) {
      return std::move(res.error());
    }
    bf.header_.flushed_page_version_ = bf.page_.page_version_;
  }
  bf.header_.latch_.UnlockExclusively();
  return {};
}

void BufferManager::RecoverFromDisk() {
  auto recovery =
      std::make_unique<Recovery>(store_, 0, store_->crmanager_->group_committer_->wal_size_);
  recovery->Run();
}

uint64_t BufferManager::ConsumedPages() {
  uint64_t total_used_bfs = 0;
  uint64_t total_free_bfs = 0;
  for (uint64_t i = 0; i < num_partitions_; i++) {
    total_free_bfs += GetPartition(i).NumReclaimedPages();
    total_used_bfs += GetPartition(i).NumAllocatedPages();
  }
  return total_used_bfs - total_free_bfs;
}

BufferFrame& BufferManager::AllocNewPageMayJump(lean_treeid_t tree_id) {
  Partition& partition = RandomPartition();
  auto* free_bf = partition.GetFreeBfMayJump();

  // BufferFrame& free_bf = *bf;
  memset((void*)free_bf, 0, store_->store_option_->buffer_frame_size_);
  new (free_bf) BufferFrame();
  free_bf->Init(partition.NextPageId());

  free_bf->page_.btree_id_ = tree_id;
  free_bf->page_.page_version_++; // mark the page as dirty
  LEAN_DLOG("Alloc new page, pageId={}, btreeId={}", free_bf->header_.page_id_,
            free_bf->page_.btree_id_);
  return *free_bf;
}

BufferFrame& BufferManager::AllocNewPage(lean_treeid_t tree_id) {
  while (true) {
    JUMPMU_TRY() {
      auto& res = AllocNewPageMayJump(tree_id);
      JUMPMU_RETURN res;
    }
    JUMPMU_CATCH() {
    }
  }
}

void BufferManager::ReclaimPage(BufferFrame& bf) {
  Partition& partition = GetPartition(bf.header_.page_id_);
  if (store_->store_option_->enable_reclaim_page_ids_) {
    partition.ReclaimPageId(bf.header_.page_id_);
  }

  if (bf.header_.is_being_written_back_) {
    // Do nothing ! we have a garbage collector ;-)
    bf.header_.latch_.UnlockExclusively();
  } else {
    bf.Reset();
    bf.header_.latch_.UnlockExclusively();
    partition.AddFreeBf(bf);
  }
}

BufferFrame* BufferManager::ResolveSwipMayJump(HybridGuard& node_guard, Swip& swip_in_node) {
  LEAN_DCHECK(node_guard.state_ == GuardState::kSharedOptimistic);
  if (swip_in_node.IsHot()) {
    // Resolve swip from hot state
    auto* bf = &swip_in_node.AsBufferFrame();
    node_guard.JumpIfModifiedByOthers();
    return bf;
  }

  if (swip_in_node.IsCool()) {
    // Resolve swip from cool state
    auto* bf = &swip_in_node.AsBufferFrameMasked();
    node_guard.JumpIfModifiedByOthers();
    BMOptimisticGuard bf_guard(bf->header_.latch_);
    BMExclusiveUpgradeIfNeeded swip_x_guard(node_guard); // parent
    BMExclusiveGuard bf_x_guard(bf_guard);               // child
    bf->header_.state_ = State::kHot;
    swip_in_node.SetToHot();
    return bf;
  }

  // Resolve swip from evicted state
  //
  // 1. Allocate buffer frame from memory
  // 2. Read page content from disk and fill the buffer frame
  //

  // unlock the current node firstly to avoid deadlock: P->G, G->P
  node_guard.Unlock();

  const lean_pid_t page_id = swip_in_node.AsPageId();
  Partition& partition = GetPartition(page_id);

  JumpScoped<LeanUniqueLock<LeanSharedMutex>> inflight_io_guard(partition.inflight_ios_mutex_);
  node_guard.JumpIfModifiedByOthers();

  auto frame_handler = partition.inflight_ios_.Lookup(page_id);

  // Create an IO frame to read page from disk.
  if (!frame_handler) {
    // 1. Randomly get a buffer frame from partitions
    auto* free_bf = partition.GetFreeBfMayJump();
    BufferFrame& bf = *free_bf;

    LEAN_DCHECK(!bf.header_.latch_.IsLockedExclusively());
    LEAN_DCHECK(bf.header_.state_ == State::kFree);

    // 2. Create an IO frame in the current partition
    IOFrame& io_frame = partition.inflight_ios_.Insert(page_id);
    io_frame.state_ = IOFrame::State::kReading;
    io_frame.num_readers_ = 1;
    JumpScoped<LeanUniqueLock<LeanMutex>> io_frame_guard(io_frame.mutex_);
    inflight_io_guard->unlock();

    // 3. Read page at pageId to the target buffer frame
    ReadPageSync(page_id, &bf.page_);
    LEAN_DLOG("Read page from disk, pageId={}, btreeId={}", page_id, bf.page_.btree_id_);

    // 4. Intialize the buffer frame header
    LEAN_DCHECK(!bf.header_.is_being_written_back_);
    bf.header_.flushed_page_version_ = bf.page_.page_version_;
    bf.header_.state_ = State::kLoaded;
    bf.header_.page_id_ = page_id;
    if (store_->store_option_->enable_buffer_crc_check_) {
      bf.header_.crc_ = bf.page_.CRC();
    }

    // 5. Publish the buffer frame
    JUMPMU_TRY() {
      node_guard.JumpIfModifiedByOthers();
      io_frame_guard->unlock();
      JumpScoped<LeanUniqueLock<LeanSharedMutex>> inflight_io_guard(partition.inflight_ios_mutex_);
      BMExclusiveUpgradeIfNeeded swip_x_guard(node_guard);

      swip_in_node.FromBufferFrame(&bf);
      bf.header_.state_ = State::kHot;

      if (io_frame.num_readers_.fetch_add(-1) == 1) {
        partition.inflight_ios_.Remove(page_id);
      }

      JUMPMU_RETURN & bf;
    }
    JUMPMU_CATCH() {
      // Change state to ready if contention is encountered
      inflight_io_guard->lock();
      io_frame.bf_ = &bf;
      io_frame.state_ = IOFrame::State::kReady;
      inflight_io_guard->unlock();
      if (io_frame_guard->owns_lock()) {
        io_frame_guard->unlock();
      }
      JumpContext::Jump();
    }
  }

  IOFrame& io_frame = frame_handler.Frame();
  switch (io_frame.state_) {
  case IOFrame::State::kReading: {
    io_frame.num_readers_++; // incremented while holding partition lock
    inflight_io_guard->unlock();

    // wait untile the reading is finished
    JumpScoped<LeanUniqueLock<LeanMutex>> io_frame_guard(io_frame.mutex_);
    io_frame_guard->unlock(); // no need to hold the mutex anymore
    if (io_frame.num_readers_.fetch_add(-1) == 1) {
      inflight_io_guard->lock();
      if (io_frame.num_readers_ == 0) {
        partition.inflight_ios_.Remove(page_id);
      }
      inflight_io_guard->unlock();
    }
    JumpContext::Jump(); // why jump?
    break;
  }
  case IOFrame::State::kReady: {
    BufferFrame* bf = io_frame.bf_;
    {
      // We have to exclusively lock the bf because the page evictor thread will
      // try to evict them when its IO is done
      LEAN_DCHECK(!bf->header_.latch_.IsLockedExclusively());
      LEAN_DCHECK(bf->header_.state_ == State::kLoaded);
      BMOptimisticGuard bf_guard(bf->header_.latch_);
      BMExclusiveUpgradeIfNeeded swip_x_guard(node_guard);
      BMExclusiveGuard bf_x_guard(bf_guard);
      io_frame.bf_ = nullptr;
      swip_in_node.FromBufferFrame(bf);
      LEAN_DCHECK(bf->header_.page_id_ == page_id);
      LEAN_DCHECK(swip_in_node.IsHot());
      LEAN_DCHECK(bf->header_.state_ == State::kLoaded);
      bf->header_.state_ = State::kHot;

      if (io_frame.num_readers_.fetch_add(-1) == 1) {
        partition.inflight_ios_.Remove(page_id);
      } else {
        io_frame.state_ = IOFrame::State::kToDelete;
      }
      inflight_io_guard->unlock();
      return bf;
    }
  }
  case IOFrame::State::kToDelete: {
    if (io_frame.num_readers_ == 0) {
      partition.inflight_ios_.Remove(page_id);
    }
    inflight_io_guard->unlock();
    JumpContext::Jump();
    break;
  }
  default: {
    LEAN_DCHECK(false);
  }
  }
  assert(false);
  return nullptr;
}

void BufferManager::ReadPageSync(lean_pid_t page_id, void* page_buffer) {
  LEAN_DCHECK(uint64_t(page_buffer) % 512 == 0);
  int64_t bytes_left = store_->store_option_->page_size_;
  while (bytes_left > 0) {
    auto total_read = store_->store_option_->page_size_ - bytes_left;
    auto cur_offset = page_id * store_->store_option_->page_size_ + total_read;
    auto* cur_buffer = reinterpret_cast<uint8_t*>(page_buffer) + total_read;
    auto bytes_read = pread(store_->page_fd_, cur_buffer, bytes_left, cur_offset);

    // read error, return a zero-initialized pageBuffer frame
    if (bytes_read <= 0) {
      memset(page_buffer, 0, store_->store_option_->page_size_);
      auto* page = new (page_buffer) BufferFrame();
      page->Init(page_id);
      if (bytes_read == 0) {
        Log::Warn("Read empty page, pageId={}, fd={}, bytesRead={}, bytesLeft={}, file={}", page_id,
                  store_->page_fd_, bytes_read, bytes_left,
                  StorePaths::PagesFilePath(store_->store_option_->store_dir_));
      } else {
        Log::Error("Failed to read page, errno={}, error={}, pageId={}, fd={}, bytesRead={}, "
                   "bytesLeft={}, file={}",
                   errno, strerror(errno), page_id, store_->page_fd_, bytes_read, bytes_left,
                   StorePaths::PagesFilePath(store_->store_option_->store_dir_));
      }
      return;
    }

    bytes_left -= bytes_read;
  }
}

BufferFrame& BufferManager::ReadPageSync(lean_pid_t page_id) {
  HybridMutex dummy_parent_latch;
  HybridGuard dummy_parent_guard(&dummy_parent_latch);
  dummy_parent_guard.ToOptimisticSpin();

  Swip swip;
  swip.FromPageId(page_id);

  while (true) {
    JUMPMU_TRY() {
      auto* bf = ResolveSwipMayJump(dummy_parent_guard, swip);
      swip.FromBufferFrame(bf);
      JUMPMU_RETURN swip.AsBufferFrame();
    }
    JUMPMU_CATCH() {
      CoroExecutor::CurrentCoro()->Yield(CoroState::kRunning);
    }
  }
}

Result<void> BufferManager::WritePageSync(BufferFrame& bf) {
  ScopedHybridGuard guard(bf.header_.latch_, LatchMode::kExclusivePessimistic);
  auto page_id = bf.header_.page_id_;
  auto& partition = GetPartition(page_id);

  auto res = WritePageSync(page_id, &bf.page_);
  if (!res) {
    return std::move(res.error());
  }

  bf.Reset();
  guard.Unlock();
  partition.AddFreeBf(bf);
  return {};
}

void BufferManager::StopPageEvictors() {
  for (auto& page_evictor : page_evictors_) {
    page_evictor->Stop();
  }
  page_evictors_.clear();
}

BufferManager::~BufferManager() {
  StopPageEvictors();
  uint64_t total_mem_size = store_->store_option_->buffer_frame_size_ * (num_bfs_ + num_safty_bfs_);
  munmap(buffer_pool_, total_mem_size);
}

void BufferManager::DoWithBufferFrameIf(std::function<bool(BufferFrame& bf)> condition,
                                        std::function<void(BufferFrame& bf)> action) {
  auto work = [&](uint64_t begin, uint64_t end) {
    LEAN_DCHECK(condition != nullptr);
    LEAN_DCHECK(action != nullptr);
    for (uint64_t i = begin; i < end; i++) {
      auto* bf_addr = &buffer_pool_[i * store_->store_option_->buffer_frame_size_];
      auto& bf = *reinterpret_cast<BufferFrame*>(bf_addr);
      bf.header_.latch_.LockExclusively();
      if (condition(bf)) {
        action(bf);
      }
      bf.header_.latch_.UnlockExclusively();
    }
  };

#ifdef LEAN_ENABLE_CORO
  store_->ParallelRange(num_bfs_, std::move(work));
#else
  utils::Parallelize::ParallelRange(num_bfs_, std::move(work));
#endif
}

} // namespace leanstore
