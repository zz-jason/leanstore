#pragma once

#include "coroutine/lean_mutex.hpp"

#include <string>
#include <unordered_set>

namespace leanstore::utils {

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

} // namespace leanstore::utils
