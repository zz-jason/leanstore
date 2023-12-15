#pragma once

#include "Config.hpp"
#include "Units.hpp"
#include "utils/JumpMU.hpp"
#include "utils/RandomGenerator.hpp"

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

class Guard;

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
  friend class Guard;
};

static_assert(sizeof(HybridLatch) == 64, "");

enum class GUARD_STATE { UNINITIALIZED, OPTIMISTIC, SHARED, EXCLUSIVE, MOVED };

enum class LATCH_FALLBACK_MODE : u8 {
  SHARED = 0,
  EXCLUSIVE = 1,
  JUMP = 2,
  SPIN = 3,
  SHOULD_NOT_HAPPEN = 4
};

/// Like std::unique_lock, std::shared_lock, std::lock_guard, this Guard is used
/// together with HybridLatch to provide various lock mode.
///
/// TODO(jian.z): should we unlock the guard when it's destroied?
class Guard {
public:
  HybridLatch* mLatch = nullptr;

  GUARD_STATE state = GUARD_STATE::UNINITIALIZED;

  u64 version;

  bool mEncounteredContention = false;

public:
  Guard(HybridLatch* latch) : mLatch(latch) {
  }

  // Manually construct a guard from a snapshot. Use with caution!
  Guard(HybridLatch& latch, const u64 last_seen_version)
      : mLatch(&latch), state(GUARD_STATE::OPTIMISTIC),
        version(last_seen_version), mEncounteredContention(false) {
  }

  Guard(HybridLatch& latch, GUARD_STATE state)
      : mLatch(&latch), state(state), version(latch.mVersion.load()) {
  }

  // Move constructor
  Guard(Guard&& other)
      : mLatch(other.mLatch), state(other.state), version(other.version),
        mEncounteredContention(other.mEncounteredContention) {
    other.state = GUARD_STATE::MOVED;
  }

  // Move assignment
  Guard& operator=(Guard&& other) {
    unlock();

    mLatch = other.mLatch;
    state = other.state;
    version = other.version;
    mEncounteredContention = other.mEncounteredContention;
    other.state = GUARD_STATE::MOVED;
    return *this;
  }

public:
  void JumpIfModifiedByOthers() {
    DCHECK(state == GUARD_STATE::OPTIMISTIC ||
           version == mLatch->mVersion.load());
    if (state == GUARD_STATE::OPTIMISTIC && version != mLatch->mVersion.load()) {
      jumpmu::jump();
    }
  }

  inline void unlock() {
    if (state == GUARD_STATE::EXCLUSIVE) {
      version += LATCH_EXCLUSIVE_BIT;
      mLatch->mVersion.store(version, std::memory_order_release);
      mLatch->mMutex.unlock();
      state = GUARD_STATE::OPTIMISTIC;
    } else if (state == GUARD_STATE::SHARED) {
      mLatch->mMutex.unlock_shared();
      state = GUARD_STATE::OPTIMISTIC;
    }
  }

  inline void toOptimisticSpin() {
    DCHECK(state == GUARD_STATE::UNINITIALIZED && mLatch != nullptr);
    version = mLatch->mVersion.load();
    while (HasExclusiveMark(version)) {
      mEncounteredContention = true;
      version = mLatch->mVersion.load();
    }
    state = GUARD_STATE::OPTIMISTIC;
  }

  inline void toOptimisticOrJump() {
    DCHECK(state == GUARD_STATE::UNINITIALIZED && mLatch != nullptr);
    version = mLatch->mVersion.load();
    if (HasExclusiveMark(version)) {
      mEncounteredContention = true;
      jumpmu::jump();
    }
    state = GUARD_STATE::OPTIMISTIC;
  }

  inline void toOptimisticOrShared() {
    DCHECK(state == GUARD_STATE::UNINITIALIZED && mLatch != nullptr);
    version = mLatch->mVersion.load();
    if (HasExclusiveMark(version)) {
      mLatch->mMutex.lock_shared();
      version = mLatch->mVersion.load();
      state = GUARD_STATE::SHARED;
      mEncounteredContention = true;
    } else {
      state = GUARD_STATE::OPTIMISTIC;
    }
  }

  inline void toOptimisticOrExclusive() {
    DCHECK(state == GUARD_STATE::UNINITIALIZED && mLatch != nullptr);
    version = mLatch->mVersion.load();
    if (HasExclusiveMark(version)) {
      mLatch->mMutex.lock();
      version = mLatch->mVersion.load() + LATCH_EXCLUSIVE_BIT;
      mLatch->mVersion.store(version, std::memory_order_release);
      state = GUARD_STATE::EXCLUSIVE;
      mEncounteredContention = true;
    } else {
      state = GUARD_STATE::OPTIMISTIC;
    }
  }

  inline void ToExclusiveMayJump() {
    DCHECK(state != GUARD_STATE::SHARED);
    if (state == GUARD_STATE::EXCLUSIVE) {
      return;
    }
    if (state == GUARD_STATE::OPTIMISTIC) {
      const u64 newVersion = version + LATCH_EXCLUSIVE_BIT;
      u64 expected = version;
      // changed from try_lock because of possible retries b/c lots of readers
      mLatch->mMutex.lock();
      if (!mLatch->mVersion.compare_exchange_strong(expected, newVersion)) {
        mLatch->mMutex.unlock();
        jumpmu::jump();
      }
      version = newVersion;
      state = GUARD_STATE::EXCLUSIVE;
    } else {
      mLatch->mMutex.lock();
      version = mLatch->mVersion.load() + LATCH_EXCLUSIVE_BIT;
      mLatch->mVersion.store(version, std::memory_order_release);
      state = GUARD_STATE::EXCLUSIVE;
    }
  }

  inline void ToSharedMayJump() {
    DCHECK(state == GUARD_STATE::OPTIMISTIC || state == GUARD_STATE::SHARED);
    if (state == GUARD_STATE::SHARED) {
      return;
    }
    if (state == GUARD_STATE::OPTIMISTIC) {
      mLatch->mMutex.lock_shared();
      if (mLatch->mVersion.load() != version) {
        mLatch->mMutex.unlock_shared();
        jumpmu::jump();
      }
      state = GUARD_STATE::SHARED;
    } else {
      UNREACHABLE();
    }
  }

  // For buffer management
  inline void TryToExclusiveMayJump() {
    DCHECK(state == GUARD_STATE::OPTIMISTIC);
    const u64 newVersion = version + LATCH_EXCLUSIVE_BIT;
    u64 expected = version;

    if (!mLatch->mMutex.try_lock()) {
      jumpmu::jump();
    }

    if (!mLatch->mVersion.compare_exchange_strong(expected, newVersion)) {
      mLatch->mMutex.unlock();
      jumpmu::jump();
    }

    version = newVersion;
    state = GUARD_STATE::EXCLUSIVE;
  }

  inline void TryToSharedMayJump() {
    DCHECK(state == GUARD_STATE::OPTIMISTIC);
    if (!mLatch->mMutex.try_lock_shared()) {
      jumpmu::jump();
    }
    if (mLatch->mVersion.load() != version) {
      mLatch->mMutex.unlock_shared();
      jumpmu::jump();
    }
    state = GUARD_STATE::SHARED;
  }
};

} // namespace storage
} // namespace leanstore
