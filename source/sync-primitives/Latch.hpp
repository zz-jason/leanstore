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

  GUARD_STATE mState = GUARD_STATE::UNINITIALIZED;

  u64 mVersion;

  bool mEncounteredContention = false;

public:
  Guard(HybridLatch* latch) : mLatch(latch) {
  }

  // Manually construct a guard from a snapshot. Use with caution!
  Guard(HybridLatch& latch, const u64 last_seen_version)
      : mLatch(&latch), mState(GUARD_STATE::OPTIMISTIC),
        mVersion(last_seen_version), mEncounteredContention(false) {
  }

  Guard(HybridLatch& latch, GUARD_STATE state)
      : mLatch(&latch), mState(state), mVersion(latch.mVersion.load()) {
  }

  // Move constructor
  Guard(Guard&& other)
      : mLatch(other.mLatch), mState(other.mState), mVersion(other.mVersion),
        mEncounteredContention(other.mEncounteredContention) {
    other.mState = GUARD_STATE::MOVED;
  }

  // Move assignment
  Guard& operator=(Guard&& other) {
    unlock();

    mLatch = other.mLatch;
    mState = other.mState;
    mVersion = other.mVersion;
    mEncounteredContention = other.mEncounteredContention;
    other.mState = GUARD_STATE::MOVED;
    return *this;
  }

public:
  void JumpIfModifiedByOthers() {
    DCHECK(mState == GUARD_STATE::OPTIMISTIC ||
           mVersion == mLatch->mVersion.load());
    if (mState == GUARD_STATE::OPTIMISTIC &&
        mVersion != mLatch->mVersion.load()) {
      jumpmu::jump();
    }
  }

  inline void unlock() {
    if (mState == GUARD_STATE::EXCLUSIVE) {
      mVersion += LATCH_EXCLUSIVE_BIT;
      mLatch->mVersion.store(mVersion, std::memory_order_release);
      mLatch->mMutex.unlock();
      mState = GUARD_STATE::OPTIMISTIC;
    } else if (mState == GUARD_STATE::SHARED) {
      mLatch->mMutex.unlock_shared();
      mState = GUARD_STATE::OPTIMISTIC;
    }
  }

  inline void toOptimisticSpin() {
    DCHECK(mState == GUARD_STATE::UNINITIALIZED && mLatch != nullptr);
    mVersion = mLatch->mVersion.load();
    while (HasExclusiveMark(mVersion)) {
      mEncounteredContention = true;
      mVersion = mLatch->mVersion.load();
    }
    mState = GUARD_STATE::OPTIMISTIC;
  }

  inline void toOptimisticOrJump() {
    DCHECK(mState == GUARD_STATE::UNINITIALIZED && mLatch != nullptr);
    mVersion = mLatch->mVersion.load();
    if (HasExclusiveMark(mVersion)) {
      mEncounteredContention = true;
      jumpmu::jump();
    }
    mState = GUARD_STATE::OPTIMISTIC;
  }

  inline void toOptimisticOrShared() {
    DCHECK(mState == GUARD_STATE::UNINITIALIZED && mLatch != nullptr);
    mVersion = mLatch->mVersion.load();
    if (HasExclusiveMark(mVersion)) {
      mLatch->mMutex.lock_shared();
      mVersion = mLatch->mVersion.load();
      mState = GUARD_STATE::SHARED;
      mEncounteredContention = true;
    } else {
      mState = GUARD_STATE::OPTIMISTIC;
    }
  }

  inline void toOptimisticOrExclusive() {
    DCHECK(mState == GUARD_STATE::UNINITIALIZED && mLatch != nullptr);
    mVersion = mLatch->mVersion.load();
    if (HasExclusiveMark(mVersion)) {
      mLatch->mMutex.lock();
      mVersion = mLatch->mVersion.load() + LATCH_EXCLUSIVE_BIT;
      mLatch->mVersion.store(mVersion, std::memory_order_release);
      mState = GUARD_STATE::EXCLUSIVE;
      mEncounteredContention = true;
    } else {
      mState = GUARD_STATE::OPTIMISTIC;
    }
  }

  inline void ToExclusiveMayJump() {
    DCHECK(mState != GUARD_STATE::SHARED);
    if (mState == GUARD_STATE::EXCLUSIVE) {
      return;
    }
    if (mState == GUARD_STATE::OPTIMISTIC) {
      const u64 newVersion = mVersion + LATCH_EXCLUSIVE_BIT;
      u64 expected = mVersion;
      // changed from try_lock because of possible retries b/c lots of readers
      mLatch->mMutex.lock();
      if (!mLatch->mVersion.compare_exchange_strong(expected, newVersion)) {
        mLatch->mMutex.unlock();
        jumpmu::jump();
      }
      mVersion = newVersion;
      mState = GUARD_STATE::EXCLUSIVE;
    } else {
      mLatch->mMutex.lock();
      mVersion = mLatch->mVersion.load() + LATCH_EXCLUSIVE_BIT;
      mLatch->mVersion.store(mVersion, std::memory_order_release);
      mState = GUARD_STATE::EXCLUSIVE;
    }
  }

  inline void ToSharedMayJump() {
    DCHECK(mState == GUARD_STATE::OPTIMISTIC || mState == GUARD_STATE::SHARED);
    if (mState == GUARD_STATE::SHARED) {
      return;
    }
    if (mState == GUARD_STATE::OPTIMISTIC) {
      mLatch->mMutex.lock_shared();
      if (mLatch->mVersion.load() != mVersion) {
        mLatch->mMutex.unlock_shared();
        jumpmu::jump();
      }
      mState = GUARD_STATE::SHARED;
    } else {
      UNREACHABLE();
    }
  }

  // For buffer management
  inline void TryToExclusiveMayJump() {
    DCHECK(mState == GUARD_STATE::OPTIMISTIC);
    const u64 newVersion = mVersion + LATCH_EXCLUSIVE_BIT;
    u64 expected = mVersion;

    if (!mLatch->mMutex.try_lock()) {
      jumpmu::jump();
    }

    if (!mLatch->mVersion.compare_exchange_strong(expected, newVersion)) {
      mLatch->mMutex.unlock();
      jumpmu::jump();
    }

    mVersion = newVersion;
    mState = GUARD_STATE::EXCLUSIVE;
  }

  inline void TryToSharedMayJump() {
    DCHECK(mState == GUARD_STATE::OPTIMISTIC);
    if (!mLatch->mMutex.try_lock_shared()) {
      jumpmu::jump();
    }
    if (mLatch->mVersion.load() != mVersion) {
      mLatch->mMutex.unlock_shared();
      jumpmu::jump();
    }
    mState = GUARD_STATE::SHARED;
  }
};

} // namespace storage
} // namespace leanstore
