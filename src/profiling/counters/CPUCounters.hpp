#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>

class PerfEvent;

namespace leanstore {

// NOLINTBEGIN

struct CPUCounters {
  std::unique_ptr<PerfEvent> e;

  std::string name;

  static uint64_t id;

  static std::unordered_map<uint64_t, CPUCounters> threads;

  static std::mutex mutex;

  static uint64_t registerThread(std::string name, bool perf_inherit = false);

  static void removeThread(uint64_t id);
};

// NOLINTEND

} // namespace leanstore
