#pragma once

#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_set>

namespace leanstore {
namespace utils {

// determine whether the code is running in debug mode
#ifndef NDEBUG
#define LS_DEBUG
constexpr bool kIsDebug = true;
#else
constexpr bool kIsDebug = false;
#endif

#ifdef LS_DEBUG
#define LS_DEBUG_EXECUTE(store, name, action)                                  \
  if (store->mDebugFlagsRegistry.IsExists(name)) {                             \
    action;                                                                    \
  }
#define LS_DEBUG_ENABLE(store, name)                                           \
  do {                                                                         \
    store->mDebugFlagsRegistry.Insert(name);                                   \
  } while (false);

#define LS_DEBUG_DISABLE(store, name)                                          \
  do {                                                                         \
    store->mDebugFlagsRegistry.Erase(name);                                    \
  } while (false);
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
