#include "MetricsManager.hpp"

namespace leanstore {

auto MetricsManager::sInstance =
    std::make_unique<MetricsManager>("127.0.0.1:10080");

} // namespace leanstore