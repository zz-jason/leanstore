#pragma once

#include "leanstore/common/types.h"
#include "leanstore/cpp/base/error.hpp"
#include "leanstore/cpp/base/optional.hpp"

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
  Optional<Error> CheckpointAll(uint8_t* buffer_pool, size_t num_bfs);

  /// Checkpoint a range of buffer frames [lower_include, upper_exclude), where
  /// the range is defined by the buffer frame indices in the buffer pool.
  Optional<Error> CheckpointRange(uint8_t* buffer_pool, uint64_t start_idx, uint64_t end_idx);

  /// Checkpoint a single buffer frame.
  Optional<Error> CheckpointOne(BufferFrame& bf);

private:
  Optional<Error> WritePageSync(lean_pid_t page_id, void* buffer);

  LeanStore& store_;
  const lean_store_option& store_option_;
  lean_lid_t min_flushed_gsn_;
};

} // namespace leanstore