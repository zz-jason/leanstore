#include "PPCounters.hpp"

#include "utils/EnumerableThreadLocal.hpp"

namespace leanstore {

utils::EnumerableThreadLocal<PPCounters> PPCounters::sCounters;

} // namespace leanstore