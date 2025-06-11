#pragma once

#include "leanstore/utils/log.hpp"

#include <atomic>
#include <shared_mutex>

#include <unistd.h>

namespace leanstore::storage {

class ScopedHybridGuard;
class HybridGuard;

enum class LatchMode : uint8_t {
  kOptimisticOrJump = 0,
  kOptimisticSpin = 1,
  kSharedPessimistic = 2,
  kExclusivePessimistic = 3,
};

constexpr static uint64_t kLatchExclusiveBit = 1ull;

inline bool HasExclusiveMark(uint64_t version) {
  return (version & kLatchExclusiveBit) == kLatchExclusiveBit;
}

/// An alternative to std::mutex and std::shared_mutex. A hybrid latch can be
/// latched optimistically, pessimistically in shared or exclusive mode:
/// - optimistic shared: in shared mode, for low-contention scenarios. At this
///   mode, the version number is used to detech latch contention.
/// - pessimistic shared: in shared mode, for high-contention scenarios.
/// - pessimistic exclusive: in exclusive mode, for high-contention scenarios.
class alignas(64) HybridLatch {
private:
  /// The optimistic version.
  std::atomic<uint64_t> version_ = 0;

  /// The pessimistic shared mutex.
  std::shared_mutex mutex_;

  friend class HybridGuard;
  friend class ScopedHybridGuard;

public:
  HybridLatch(uint64_t version = 0) : version_(version) {
  }

  void LockExclusively() {
    mutex_.lock();
    version_.fetch_add(kLatchExclusiveBit, std::memory_order_release);
    LS_DCHECK(IsLockedExclusively());
  }

  void UnlockExclusively() {
    LS_DCHECK(IsLockedExclusively());
    version_.fetch_add(kLatchExclusiveBit, std::memory_order_release);
    mutex_.unlock();
  }

  uint64_t GetVersion() {
    return version_.load();
  }

  bool IsLockedExclusively() {
    return HasExclusiveMark(version_.load());
  }
};

static_assert(sizeof(HybridLatch) == 64, "");

} // namespace leanstore::storage