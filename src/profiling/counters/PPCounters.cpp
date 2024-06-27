#include "leanstore/profiling/counters/PPCounters.hpp"

#include "leanstore/utils/EnumerableThreadLocal.hpp"

namespace leanstore {

utils::EnumerableThreadLocal<PPCounters> PPCounters::sCounters;

} // namespace leanstore