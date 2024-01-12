#pragma once

#include "shared-headers/Units.hpp"
#include "utils/EnumerableThreadLocal.hpp"

namespace leanstore {

class PPCounters {
public:
  PPCounters() = default;

public:
  // ATTENTION: These counters should be only used by page provider threads or
  // slow path worker code
  atomic<s64> mPhase1MS = 0;

  atomic<s64> mPhase2MS = 0;

  atomic<s64> mPhase3MS = 0;

  // Phase 1 detailed
  atomic<u64> mFindParentMS = 0;

  atomic<u64> mIterateChildrenMS = 0;

  // Phase 3 detailed
  atomic<u64> async_wb_ms = 0;

  atomic<u64> submit_ms = 0;

  atomic<u64> phase_1_counter = 0;

  atomic<u64> phase_2_counter = 0;

  atomic<u64> phase_3_counter = 0;

  atomic<u64> evicted_pages = 0;

  atomic<u64> pp_thread_rounds = 0;

  atomic<u64> touched_bfs_counter = 0;

  atomic<u64> flushed_pages_counter = 0;

  atomic<u64> unswizzled_pages_counter = 0;

  static utils::EnumerableThreadLocal<PPCounters> sCounters;

  static PPCounters& myCounters() {
    return *sCounters.Local();
  }
};

} // namespace leanstore
