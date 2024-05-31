#pragma once

#include "utils/EnumerableThreadLocal.hpp"

#include <atomic>

namespace leanstore {

// NOLINTBEGIN

class PPCounters {
public:
  PPCounters() = default;

public:
  // ATTENTION: These counters should be only used by page evictor threads or
  // slow path worker code
  std::atomic<int64_t> mPhase1MS = 0;

  std::atomic<int64_t> mPhase2MS = 0;

  std::atomic<int64_t> mPhase3MS = 0;

  // Phase 1 detailed
  std::atomic<uint64_t> mFindParentMS = 0;

  std::atomic<uint64_t> mIterateChildrenMS = 0;

  // Phase 3 detailed
  std::atomic<uint64_t> async_wb_ms = 0;

  std::atomic<uint64_t> submit_ms = 0;

  std::atomic<uint64_t> phase_1_counter = 0;

  std::atomic<uint64_t> phase_2_counter = 0;

  std::atomic<uint64_t> phase_3_counter = 0;

  std::atomic<uint64_t> evicted_pages = 0;

  std::atomic<uint64_t> pp_thread_rounds = 0;

  std::atomic<uint64_t> touched_bfs_counter = 0;

  std::atomic<uint64_t> flushed_pages_counter = 0;

  std::atomic<uint64_t> unswizzled_pages_counter = 0;

  static utils::EnumerableThreadLocal<PPCounters> sCounters;

  static PPCounters& MyCounters() {
    return *sCounters.Local();
  }
};

// NOLINTEND

} // namespace leanstore
