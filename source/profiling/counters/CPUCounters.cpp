#include "CPUCounters.hpp"

namespace leanstore {

std::mutex CPUCounters::mutex;
uint64_t CPUCounters::id = 0;
std::unordered_map<uint64_t, CPUCounters> CPUCounters::threads;

uint64_t CPUCounters::registerThread(std::string name, bool perfInherit) {
  std::unique_lock guard(mutex);
  threads[id] = {.e = std::make_unique<PerfEvent>(perfInherit), .name = name};
  return id++;
}

void CPUCounters::removeThread(uint64_t id) {
  std::unique_lock guard(mutex);
  threads.erase(id);
}

} // namespace leanstore
