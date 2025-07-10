#pragma once

#include "utils/coroutine/lean_mutex.hpp"

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
private:
  LeanSharedMutex mutex_;
  std::unordered_set<std::string> flags_;

public:
  DebugFlagsRegistry() = default;
  ~DebugFlagsRegistry() = default;

  void Insert(const std::string& name) {
    LEAN_UNIQUE_LOCK(mutex_);
    flags_.insert(name);
  }

  void Erase(const std::string& name) {
    LEAN_UNIQUE_LOCK(mutex_);
    flags_.erase(name);
  }

  bool IsExists(const std::string& name) {
    LEAN_SHARED_LOCK(mutex_);
    auto it = flags_.find(name);
    return it != flags_.end();
  }
};

} // namespace utils
} // namespace leanstore
