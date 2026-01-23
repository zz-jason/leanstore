#pragma once

#include "leanstore/base/result.hpp"
#include "leanstore/c/types.h"

namespace leanstore {

class LeanStore;
class BufferFrame;

class CheckpointProcessor {
public:
  CheckpointProcessor(LeanStore& store, const lean_store_option& store_option);
  ~CheckpointProcessor() = default;

  // no copy and assign
  CheckpointProcessor& operator=(const CheckpointProcessor&) = delete;
  CheckpointProcessor(const CheckpointProcessor&) = delete;

  /// Checkpoint all the buffer frames in the buffer pool.
  Result<void> CheckpointAll(uint8_t* buffer_pool, size_t num_bfs);

  /// Checkpoint a range of buffer frames [lower_include, upper_exclude), where
  /// the range is defined by the buffer frame indices in the buffer pool.
  Result<void> CheckpointRange(uint8_t* buffer_pool, uint64_t start_idx, uint64_t end_idx);

  /// Checkpoint a single buffer frame.
  Result<void> CheckpointOne(BufferFrame& bf);

private:
  Result<void> WritePageSync(lean_pid_t page_id, void* buffer);

  LeanStore& store_;
  const lean_store_option& store_option_;
  lean_lid_t min_flushed_gsn_;
};

} // namespace leanstore