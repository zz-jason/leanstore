#pragma once

#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_set>

namespace leanstore {
namespace utils {

#ifdef DEBUG
#define LS_DEBUG_EXECUTE(store, name, action)                                                      \
  if (store != nullptr && store->debug_flags_registry_.IsExists(name)) {                           \
    action;                                                                                        \
  }
#define LS_DEBUG_ENABLE(store, name)                                                               \
  if (store != nullptr) {                                                                          \
    store->debug_flags_registry_.Insert(name);                                                     \
  }

#define LS_DEBUG_DISABLE(store, name)                                                              \
  if (store != nullptr) {                                                                          \
    store->debug_flags_registry_.Erase(name);                                                      \
  }
#else
#define LS_DEBUG_EXECUTE(store, name, action)
#define LS_DEBUG_ENABLE(store, name)
#define LS_DEBUG_DISABLE(store, name)
#endif

class DebugFlagsRegistry {
public:
  std::shared_mutex mutex_;
  std::unordered_set<std::string> flags_;

  DebugFlagsRegistry() = default;
  ~DebugFlagsRegistry() = default;

public:
  void Insert(const std::string& name) {
    std::unique_lock unique_guard(mutex_);
    flags_.insert(name);
  }

  void Erase(const std::string& name) {
    std::unique_lock unique_guard(mutex_);
    flags_.erase(name);
  }

  bool IsExists(const std::string& name) {
    std::shared_lock shared_guard(mutex_);
    auto it = flags_.find(name);
    return it != flags_.end();
  }
};

} // namespace utils
} // namespace leanstore
