#include "MetricsManager.hpp"

namespace leanstore {

MetricsManager* MetricsManager::Get() {
  static std::unique_ptr<MetricsManager> sInstance =
      std::make_unique<MetricsManager>("127.0.0.1:10080");
  return sInstance.get();
}

} // namespace leanstore