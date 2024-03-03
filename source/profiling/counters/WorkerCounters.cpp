#include "WorkerCounters.hpp"

#include "utils/EnumerableThreadLocal.hpp"

namespace leanstore {

std::atomic<uint64_t> WorkerCounters::sNumWorkers = 0;

utils::EnumerableThreadLocal<WorkerCounters> WorkerCounters::sCounters;

} // namespace leanstore
