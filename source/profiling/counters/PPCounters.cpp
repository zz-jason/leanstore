#include "PPCounters.hpp"

namespace leanstore {

tbb::enumerable_thread_specific<PPCounters> PPCounters::pp_counters;

}