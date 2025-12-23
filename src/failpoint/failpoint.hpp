#pragma once

#include "coroutine/lean_mutex.hpp"
#include "leanstore/cpp/base/log.hpp"

#include <string>
#include <string_view>
#include <unordered_set>

namespace leanstore {

class FailPoint {
public:
  static void Enable(std::string_view name) {
    LEAN_UNIQUE_LOCK(s_mutex);
    s_enabled_fps.insert(std::string(name));
    Log::Info("Failpoint enabled: {}", name);
  }

  static void Disable(std::string_view name) {
    LEAN_UNIQUE_LOCK(s_mutex);
    s_enabled_fps.erase(std::string(name));
    Log::Info("Failpoint disabled: {}", name);
  }

  static bool IsEnabled(std::string_view name) {
    LEAN_SHARED_LOCK(s_mutex);
    return s_enabled_fps.find(std::string(name)) != s_enabled_fps.end();
  }

  static constexpr auto kSkipCheckpointAll = "skip_checkpoint_all";

private:
  inline static LeanSharedMutex s_mutex{};
  inline static std::unordered_set<std::string> s_enabled_fps{};
};

#ifdef DEBUG
#define LEAN_FAIL_POINT(name, code)                                                                \
  if (leanstore::FailPoint::IsEnabled(name)) {                                                     \
    Log::Info("FailPoint triggered: {}", name);                                                    \
    code;                                                                                          \
  }
#else
#define LEAN_FAIL_POINT(name, code) (void)0;
#endif

} // namespace leanstore