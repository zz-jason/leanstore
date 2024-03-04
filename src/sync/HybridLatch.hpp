#pragma once

#include <glog/logging.h>

#include <atomic>
#include <shared_mutex>

#include <unistd.h>

namespace leanstore {
namespace storage {

enum class LatchMode : uint8_t {
  kOptimisticOrJump = 0,
  kOptimisticSpin = 1,
  kPessimisticShared = 2,
  kPessimisticExclusive = 3,
};

constexpr static uint64_t kLatchExclusiveBit = 1ull;

inline bool HasExclusiveMark(uint64_t version) {
  return (version & kLatchExclusiveBit) == kLatchExclusiveBit;
}

class ScopedHybridGuard;
class HybridGuard;

/// An alternative to std::mutex and std::shared_mutex. A hybrid latch can be
/// latched optimistically, pessimistically in shared or exclusive mode:
///   - latch optimistically: for low-contention scenarios. At this mode, the
///     version number is used to detech latch contention.
///   - latch pessimistically in shared mode: for high-contention scenarios.
///   - latch pessimistically in exclusive mode: for high-contention scenarios.
class alignas(64) HybridLatch {
private:
  std::atomic<uint64_t> mVersion = 0;

  std::shared_mutex mMutex;

public:
  HybridLatch(uint64_t version = 0) : mVersion(version) {
  }

public:
  void LockExclusively() {
    DCHECK(!IsLockedExclusively());
    mMutex.lock();
    mVersion.fetch_add(kLatchExclusiveBit, std::memory_order_release);
  }

  void UnlockExclusively() {
    DCHECK(IsLockedExclusively());
    mVersion.fetch_add(kLatchExclusiveBit, std::memory_order_release);
    mMutex.unlock();
  }

  uint64_t GetOptimisticVersion() {
    return mVersion.load();
  }

  bool IsLockedExclusively() {
    return HasExclusiveMark(mVersion.load());
  }

private:
  friend class HybridGuard;
  friend class ScopedHybridGuard;
};

static_assert(sizeof(HybridLatch) == 64, "");

} // namespace storage
} // namespace leanstore