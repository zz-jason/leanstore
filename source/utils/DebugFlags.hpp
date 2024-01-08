#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_set>

namespace leanstore {
namespace utils {

#ifdef NDEBUG
#define LS_DEBUG_EXECUTE(name, action)
#define LS_DEBUG_ENABLE(name)
#define LS_DEBUG_DISABLE(name)
#else
#define LS_DEBUG_EXECUTE(name, action)                                         \
  if (leanstore::utils::DebugFlagsRegistry::sInstance->IsExists(name)) {       \
    action;                                                                    \
  }
#define LS_DEBUG_ENABLE(name)                                                  \
  do {                                                                         \
    leanstore::utils::DebugFlagsRegistry::sInstance->Insert(name);             \
  } while (false);

#define LS_DEBUG_DISABLE(name)                                                 \
  do {                                                                         \
    leanstore::utils::DebugFlagsRegistry::sInstance->Erase(name);              \
  } while (false);
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

public:
  static std::unique_ptr<DebugFlagsRegistry> sInstance;
};

} // namespace utils
} // namespace leanstore
