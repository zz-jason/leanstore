#pragma once

#include <atomic>
#include <cstdint>

namespace leanstore::ycsb {

/// Benchmark statistics for YCSB.
class YcsbBenchStats {
public:
  YcsbBenchStats() = default;

  void IncCommitted() {
    committed_++;
  }

  void IncAborted() {
    aborted_++;
  }

  /// Merge local stats into the provided counters and reset local stats.
  void MergeInto(uint64_t& committed, uint64_t& aborted) {
    committed += committed_.exchange(0, std::memory_order_relaxed);
    aborted += aborted_.exchange(0, std::memory_order_relaxed);
  }

private:
  std::atomic<uint64_t> committed_ = 0;
  std::atomic<uint64_t> aborted_ = 0;
};

} // namespace leanstore::ycsb