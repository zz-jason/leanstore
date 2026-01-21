#include "leanstore/checkpoint/checkpoint_processor.hpp"

#include "leanstore/base/error.hpp"
#include "leanstore/base/log.hpp"
#include "leanstore/base/range_splits.hpp"
#include "leanstore/base/small_vector.hpp"
#include "leanstore/buffer/buffer_frame.hpp"
#include "leanstore/buffer/tree_registry.hpp"
#include "leanstore/c/types.h"
#include "leanstore/coro/coro_env.hpp"
#include "leanstore/coro/coro_io.hpp"
#include "leanstore/lean_store.hpp"

#include <vector>

namespace leanstore {

CheckpointProcessor::CheckpointProcessor(LeanStore& store, const lean_store_option& store_option)
    : store_(store),
      store_option_(store_option),
      min_flushed_gsn_(std::numeric_limits<lean_lid_t>::max()) {
}

Result<void> CheckpointProcessor::CheckpointAll(uint8_t* buffer_pool, size_t num_bfs) {
  ScopedTimer timer([](double elapsed_ms) {
    Log::Info("Checkpoint all buffer frames finished, timeElasped={:.6f}ms", elapsed_ms);
  });
  Log::Info("Checkpointing all buffer frames ...");

  auto& coro_scheduler = CoroEnv::CurStore().GetCoroScheduler();
  auto parallelism = CoroEnv::CurStore().store_option_->worker_threads_;
  std::vector<std::shared_ptr<CoroFuture<void>>> futures;
  std::vector<CoroSession*> reserved_sessions;
  auto ranges = RangeSplits<size_t>(num_bfs, parallelism);

  for (auto i = 0u; i < ranges.size(); ++i) {
    reserved_sessions.push_back(coro_scheduler.TryReserveCoroSession(i));
    LEAN_DCHECK(reserved_sessions.back() != nullptr &&
                "Failed to reserve a CoroSession for parallel range execution");
    auto range = ranges[i];
    futures.emplace_back(
        coro_scheduler.Submit(reserved_sessions.back(), [this, buffer_pool, range]() {
          auto res = CheckpointRange(buffer_pool, range.begin(), range.end());
          if (!res) {
            Log::Error("Failed to checkpoint buffer frames in range [{}, {}), error={}",
                       range.begin(), range.end(), res.error().ToString());
          }
        }));
  }

  for (const auto& future : futures) {
    future->Wait();
  }
  for (auto* session : reserved_sessions) {
    coro_scheduler.ReleaseCoroSession(session);
  }

  return {};
}

Result<void> CheckpointProcessor::CheckpointRange(uint8_t* buffer_pool, uint64_t start_idx,
                                                  uint64_t end_idx) {
  const auto buffer_frame_size = store_option_.buffer_frame_size_;
  for (uint64_t i = start_idx; i < end_idx; ++i) {
    auto& bf = *reinterpret_cast<BufferFrame*>(&buffer_pool[i * buffer_frame_size]);
    if (auto res = CheckpointOne(bf); !res) {
      Log::Error("Failed to checkpoint buffer frame, pageId={}, error={}", bf.header_.page_id_,
                 res.error().ToString());
      return res;
    }
  }

  return {};
}

// TODO: replace with async coroutine version
Result<void> CheckpointProcessor::CheckpointOne(BufferFrame& bf) {
  auto page_size = store_option_.page_size_;
  auto page_buffer_handle = SmallBuffer512Aligned<4096>(page_size);
  auto* page_buffer = page_buffer_handle.Data();

  bf.header_.latch_.LockExclusively();
  if (!bf.IsFree()) {
    Log::Info("Checkpointing page_id={} btree_id={} page_version={}", bf.header_.page_id_,
              bf.page_.btree_id_, bf.page_.page_version_);
    store_.tree_registry_->Checkpoint(bf.page_.btree_id_, bf, page_buffer);

    auto page_offset = bf.header_.page_id_ * page_size;
    CoroWrite(store_.page_fd_, page_buffer, page_size, page_offset);

    bf.header_.flushed_page_version_ = bf.page_.page_version_;
    min_flushed_gsn_ = std::min(min_flushed_gsn_, bf.page_.page_version_);
  }
  bf.header_.latch_.UnlockExclusively();
  return {};
}

} // namespace leanstore