#include "CRCounters.hpp"

namespace leanstore {

tbb::enumerable_thread_specific<CRCounters> CRCounters::cr_counters;

} // namespace leanstore
