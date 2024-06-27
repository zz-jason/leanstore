#include "leanstore/profiling/counters/WorkerCounters.hpp"

#include "leanstore/utils/EnumerableThreadLocal.hpp"

namespace leanstore {

std::atomic<uint64_t> WorkerCounters::sNumWorkers = 0;

utils::EnumerableThreadLocal<WorkerCounters> WorkerCounters::sCounters;

} // namespace leanstore
