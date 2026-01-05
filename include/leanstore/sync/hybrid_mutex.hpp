#pragma once

#include "coroutine/lean_mutex.hpp"
#include "leanstore/cpp/base/log.hpp"

#include <atomic>

#include <unistd.h>

namespace leanstore {

class ScopedHybridGuard;
class HybridGuard;

enum class LatchMode : uint8_t {
  kOptimisticOrJump = 0,
  kOptimisticSpin = 1,
  kSharedPessimistic = 2,
  kExclusivePessimistic = 3,
};

static constexpr uint64_t kLatchExclusiveBit = 1ull;

inline bool HasExclusiveMark(uint64_t version) {
  return (version & kLatchExclusiveBit) == kLatchExclusiveBit;
}

/// An alternative to std::mutex and std::shared_mutex. A hybrid latch can be
/// latched optimistically, pessimistically in shared or exclusive mode:
/// - optimistic shared: in shared mode, for low-contention scenarios. At this
///   mode, the version number is used to detech latch contention.
/// - pessimistic shared: in shared mode, for high-contention scenarios.
/// - pessimistic exclusive: in exclusive mode, for high-contention scenarios.
class HybridMutex {
private:
  /// The optimistic version.
  std::atomic<uint64_t> version_ = 0;

  /// The pessimistic shared mutex.
  LeanSharedMutex shared_mutex_;

  friend class HybridGuard;
  friend class ScopedHybridGuard;

public:
  HybridMutex() = default;

  void LockExclusively() {
    shared_mutex_.lock();
    version_.fetch_add(kLatchExclusiveBit, std::memory_order_release);
    LEAN_DCHECK(IsLockedExclusively());
  }

  void UnlockExclusively() {
    LEAN_DCHECK(IsLockedExclusively());
    version_.fetch_add(kLatchExclusiveBit, std::memory_order_release);
    shared_mutex_.unlock();
  }

  uint64_t GetVersion() {
    return version_.load();
  }

  bool IsLockedExclusively() {
    return HasExclusiveMark(version_.load());
  }
};

} // namespace leanstore