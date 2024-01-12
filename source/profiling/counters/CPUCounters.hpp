#pragma once

#include "shared-headers/PerfEvent.hpp"
#include "shared-headers/Units.hpp"

#include <memory>
#include <mutex>
#include <unordered_map>

namespace leanstore {

struct CPUCounters {
  std::unique_ptr<PerfEvent> e;

  string name;

  static u64 id;

  static std::unordered_map<u64, CPUCounters> threads;

  static std::mutex mutex;

  static u64 registerThread(string name, bool perf_inherit = false);

  static void removeThread(u64 id);
};

} // namespace leanstore
