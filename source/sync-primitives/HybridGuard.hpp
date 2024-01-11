#pragma once

#include "Exceptions.hpp"
#include "HybridLatch.hpp"
#include "Units.hpp"
#include "utils/JumpMU.hpp"

#include <glog/logging.h>

#include <atomic>
#include <shared_mutex>

#include <unistd.h>

namespace leanstore {
namespace storage {

enum class GUARD_STATE { UNINITIALIZED, OPTIMISTIC, SHARED, EXCLUSIVE, MOVED };

enum class LATCH_FALLBACK_MODE : u8 {
  SHARED = 0,
  EXCLUSIVE = 1,
  JUMP = 2,
  SPIN = 3,
  SHOULD_NOT_HAPPEN = 4
};

/// Like std::unique_lock, std::shared_lock, std::lock_guard, this HybridGuard
/// is used together with HybridLatch to provide various lock mode.
///
/// TODO(jian.z): should we unlock the guard when it's destroied?
class HybridGuard {
public:
  HybridLatch* mLatch = nullptr;

  GUARD_STATE mState = GUARD_STATE::UNINITIALIZED;

  u64 mVersion;

  bool mEncounteredContention = false;

public:
  HybridGuard(HybridLatch* latch) : mLatch(latch) {
  }

  // Manually construct a guard from a snapshot. Use with caution!
  HybridGuard(HybridLatch& latch, const u64 lastSeenVersion)
      : mLatch(&latch), mState(GUARD_STATE::OPTIMISTIC),
        mVersion(lastSeenVersion), mEncounteredContention(false) {
  }

  HybridGuard(HybridLatch& latch, GUARD_STATE state)
      : mLatch(&latch), mState(state), mVersion(latch.mVersion.load()) {
  }

  // Move constructor
  HybridGuard(HybridGuard&& other)
      : mLatch(other.mLatch), mState(other.mState), mVersion(other.mVersion),
        mEncounteredContention(other.mEncounteredContention) {
    other.mState = GUARD_STATE::MOVED;
  }

  // Move assignment
  HybridGuard& operator=(HybridGuard&& other) {
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
