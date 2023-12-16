#pragma once

#include "Units.hpp"

#include <glog/logging.h>

#include <atomic>
#include <shared_mutex>

#include <unistd.h>

namespace leanstore {
namespace storage {

constexpr static u64 LATCH_EXCLUSIVE_BIT = 1ull;
constexpr static u64 LATCH_VERSION_MASK = ~(0ull);

inline bool HasExclusiveMark(u64 version) {
  return (version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT;
}

class HybridGuard;

/// An alternative to std::mutex and std::shared_mutex. A hybrid latch can be
/// latched optimistically, pessimistically in shared or exclusive mode:
///   - latch optimistically: for low-contention scenarios. At this mode, the
///     version number is used to detech latch contention.
///   - latch pessimistically in shared mode: for high-contention scenarios.
///   - latch pessimistically in exclusive mode: for high-contention scenarios.
class alignas(64) HybridLatch {
private:
  atomic<u64> mVersion = 0;

  std::shared_mutex mMutex;

public:
  template <typename... Args>
  HybridLatch(Args&&... args) : mVersion(std::forward<Args>(args)...) {
  }

public:
  void LockExclusively() {
    DCHECK(!IsLockedExclusively());
    mMutex.lock();
    mVersion.fetch_add(LATCH_EXCLUSIVE_BIT);
  }

  void UnlockExclusively() {
    DCHECK(IsLockedExclusively());
    mVersion.fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
    mMutex.unlock();
  }

  u64 GetOptimisticVersion() {
    return mVersion.load();
  }

  bool IsLockedExclusively() {
    return HasExclusiveMark(mVersion.load());
  }

private:
  friend class HybridGuard;
};

static_assert(sizeof(HybridLatch) == 64, "");

} // namespace storage
} // namespace leanstore