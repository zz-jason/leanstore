#include "WorkerCounters.hpp"

#include "utils/EnumerableThreadLocal.hpp"

namespace leanstore {

atomic<u64> WorkerCounters::sNumWorkers = 0;

utils::EnumerableThreadLocal<WorkerCounters> WorkerCounters::sCounters;

} // namespace leanstore
