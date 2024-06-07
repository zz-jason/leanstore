#include "BMTable.hpp"

#include "buffer-manager/BufferManager.hpp"
#include "profiling/counters/PPCounters.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "utils/EnumerableThreadLocal.hpp"
#include "utils/UserThread.hpp"

namespace leanstore {
namespace profiling {

BMTable::BMTable(storage::BufferManager& bm) : ProfilingTable(), bm(bm) {
}

std::string BMTable::getName() {
  return "bm";
}

void BMTable::open() {
  columns.emplace("key", [](Column& col) { col << 0; });

  columns.emplace("space_usage_gib", [&](Column& col) {
    const double gib = bm.ConsumedPages() * 1.0 * utils::tlsStore->mStoreOption.mPageSize / 1024.0 /
                       1024.0 / 1024.0;
    col << gib;
  });

  columns.emplace("space_usage_kib", [&](Column& col) {
    const double kib = bm.ConsumedPages() * 1.0 * utils::tlsStore->mStoreOption.mPageSize / 1024.0;
    col << kib;
  });

  columns.emplace("consumed_pages", [&](Column& col) { col << bm.ConsumedPages(); });
  columns.emplace("p1_pct", [&](Column& col) { col << (local_phase_1_ms * 100.0 / total); });
  columns.emplace("p2_pct", [&](Column& col) { col << (local_phase_2_ms * 100.0 / total); });
  columns.emplace("p3_pct", [&](Column& col) { col << (local_phase_3_ms * 100.0 / total); });
  columns.emplace("poll_pct", [&](Column& col) { col << ((local_poll_ms * 100.0 / total)); });
  columns.emplace("find_parent_pct", [&](Column& col) {
    auto res = Sum(PPCounters::sCounters, &PPCounters::mFindParentMS);
    col << (res * 100.0 / total);
  });
  columns.emplace("iterate_children_pct", [&](Column& col) {
    auto res = Sum(PPCounters::sCounters, &PPCounters::mIterateChildrenMS);
    col << (res * 100.0 / total);
  });
  columns.emplace(
      "pc1", [&](Column& col) { col << Sum(PPCounters::sCounters, &PPCounters::phase_1_counter); });
  columns.emplace(
      "pc2", [&](Column& col) { col << Sum(PPCounters::sCounters, &PPCounters::phase_2_counter); });
  columns.emplace(
      "pc3", [&](Column& col) { col << Sum(PPCounters::sCounters, &PPCounters::phase_3_counter); });
  columns.emplace("free_pct", [&](Column& col) { col << (local_total_free * 100.0 / bm.mNumBfs); });
  columns.emplace("evicted_mib", [&](Column& col) {
    auto res = Sum(PPCounters::sCounters, &PPCounters::evicted_pages);
    col << (res * utils::tlsStore->mStoreOption.mPageSize / 1024.0 / 1024.0);
  });
  columns.emplace("rounds", [&](Column& col) {
    col << (Sum(PPCounters::sCounters, &PPCounters::pp_thread_rounds));
  });
  columns.emplace("touches", [&](Column& col) {
    col << (Sum(PPCounters::sCounters, &PPCounters::touched_bfs_counter));
  });
  columns.emplace("unswizzled", [&](Column& col) {
    col << (Sum(PPCounters::sCounters, &PPCounters::unswizzled_pages_counter));
  });
  columns.emplace("submit_ms", [&](Column& col) {
    auto res = Sum(PPCounters::sCounters, &PPCounters::submit_ms);
    col << (res * 100.0 / total);
  });
  columns.emplace("async_mb_ws", [&](Column& col) {
    col << (Sum(PPCounters::sCounters, &PPCounters::async_wb_ms));
  });
  columns.emplace("w_mib", [&](Column& col) {
    auto res = Sum(PPCounters::sCounters, &PPCounters::flushed_pages_counter);
    col << (res * utils::tlsStore->mStoreOption.mPageSize / 1024.0 / 1024.0);
  });

  columns.emplace("allocate_ops", [&](Column& col) {
    col << (Sum(WorkerCounters::sCounters, &WorkerCounters::allocate_operations_counter));
  });
  columns.emplace("r_mib", [&](Column& col) {
    auto res = Sum(WorkerCounters::sCounters, &WorkerCounters::read_operations_counter);
    col << (res * utils::tlsStore->mStoreOption.mPageSize / 1024.0 / 1024.0);
  });
}

void BMTable::next() {
  clear();
  local_phase_1_ms = Sum(PPCounters::sCounters, &PPCounters::mPhase1MS);
  local_phase_2_ms = Sum(PPCounters::sCounters, &PPCounters::mPhase2MS);
  local_phase_3_ms = Sum(PPCounters::sCounters, &PPCounters::mPhase3MS);

  local_total_free = 0;
  for (uint64_t i = 0; i < bm.mNumPartitions; i++) {
    local_total_free += bm.GetPartition(i).mFreeBfList.mSize.load();
  }
  total = local_phase_1_ms + local_phase_2_ms + local_phase_3_ms;
  for (auto& c : columns) {
    c.second.generator(c.second);
  }
}

} // namespace profiling
} // namespace leanstore
