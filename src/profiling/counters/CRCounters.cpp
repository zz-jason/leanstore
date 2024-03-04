#include "CRCounters.hpp"

namespace leanstore {

utils::EnumerableThreadLocal<CRCounters> CRCounters::sCounters;

} // namespace leanstore
