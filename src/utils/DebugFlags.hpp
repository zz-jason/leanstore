#pragma once

#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_set>

namespace leanstore {
namespace utils {

#ifdef DEBUG
#define LS_DEBUG_EXECUTE(store, name, action)                                                      \
  if (store != nullptr && store->mDebugFlagsRegistry.IsExists(name)) {                             \
    action;                                                                                        \
  }
#define LS_DEBUG_ENABLE(store, name)                                                               \
  if (store != nullptr) {                                                                          \
    store->mDebugFlagsRegistry.Insert(name);                                                       \
  }

#define LS_DEBUG_DISABLE(store, name)                                                              \
  if (store != nullptr) {                                                                          \
    store->mDebugFlagsRegistry.Erase(name);                                                        \
  }
#else
#define LS_DEBUG_EXECUTE(store, name, action)
#define LS_DEBUG_ENABLE(store, name)
#define LS_DEBUG_DISABLE(store, name)
#endif

class DebugFlagsRegistry {
public:
  std::shared_mutex mMutex;
  std::unordered_set<std::string> mFlags;

  DebugFlagsRegistry() = default;
  ~DebugFlagsRegistry() = default;

public:
  void Insert(const std::string& name) {
    std::unique_lock uniqueGuard(mMutex);
    mFlags.insert(name);
  }

  void Erase(const std::string& name) {
    std::unique_lock uniqueGuard(mMutex);
    mFlags.erase(name);
  }

  bool IsExists(const std::string& name) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mFlags.find(name);
    return it != mFlags.end();
  }
};

} // namespace utils
} // namespace leanstore
