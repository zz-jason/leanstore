#include "leanstore/profiling/counters/CRCounters.hpp"

namespace leanstore {

utils::EnumerableThreadLocal<CRCounters> CRCounters::sCounters;

} // namespace leanstore
