#pragma once

#include "HybridLatch.hpp"
#include "shared-headers/Exceptions.hpp"
#include "shared-headers/Units.hpp"
#include "utils/JumpMU.hpp"

#include <glog/logging.h>

#include <atomic>
#include <shared_mutex>

#include <unistd.h>

namespace leanstore {
namespace storage {

enum class GuardState : u8 {
  kUninitialized = 0,
  kOptimistic = 1,
  kShared = 2,
  kExclusive = 3,
  kMoved = 4,
};

/// Like std::unique_lock, std::shared_lock, std::lock_guard, this HybridGuard
/// is used together with HybridLatch to provide various lock mode.
///
/// TODO(jian.z): should we unlock the guard when it's destroied?
class HybridGuard {
public:
  HybridLatch* mLatch = nullptr;

  GuardState mState = GuardState::kUninitialized;

  u64 mVersion;

  bool mEncounteredContention = false;

public:
  HybridGuard(HybridLatch* latch) : mLatch(latch) {
  }

  // Manually construct a guard from a snapshot. Use with caution!
  HybridGuard(HybridLatch& latch, const u64 lastSeenVersion)
      : mLatch(&latch),
        mState(GuardState::kOptimistic),
        mVersion(lastSeenVersion),
        mEncounteredContention(false) {
  }

  HybridGuard(HybridLatch& latch, GuardState state)
      : mLatch(&latch),
        mState(state),
        mVersion(latch.mVersion.load()) {
  }

  // Move constructor
  HybridGuard(HybridGuard&& other) {
    *this = std::move(other);
  }

  // Move assignment
  HybridGuard& operator=(HybridGuard&& other) {
    unlock();

    mLatch = other.mLatch;
    mState = other.mState;
    mVersion = other.mVersion;
    mEncounteredContention = other.mEncounteredContention;
    other.mState = GuardState::kMoved;
    return *this;
  }

public:
  void JumpIfModifiedByOthers() {
    DCHECK(mState == GuardState::kOptimistic ||
           mVersion == mLatch->mVersion.load());
    if (mState == GuardState::kOptimistic &&
        mVersion != mLatch->mVersion.load()) {
      DLOG(INFO) << "Jump because of contention"
                 << ", mVersion(expected)=" << mVersion
                 << ", mLatch->mVersion(actual)=" << mLatch->mVersion.load();
      jumpmu::Jump();
    }
  }

  inline void unlock() {
    if (mState == GuardState::kExclusive) {
      unlockExclusive();
      DCHECK(!HasExclusiveMark(mLatch->mVersion));
    } else if (mState == GuardState::kShared) {
      unlockShared();
      DCHECK(!HasExclusiveMark(mLatch->mVersion));
    }
  }

  inline void ToOptimisticSpin() {
    DCHECK(mState == GuardState::kUninitialized && mLatch != nullptr);
    mVersion = mLatch->mVersion.load();
    while (HasExclusiveMark(mVersion)) {
      mEncounteredContention = true;
      mVersion = mLatch->mVersion.load();
    }
    mState = GuardState::kOptimistic;
  }

  inline void ToOptimisticOrJump() {
    DCHECK(mState == GuardState::kUninitialized && mLatch != nullptr);
    mVersion = mLatch->mVersion.load();
    if (HasExclusiveMark(mVersion)) {
      mEncounteredContention = true;
      jumpmu::Jump();
    }
    mState = GuardState::kOptimistic;
  }

  inline void ToOptimisticOrShared() {
    DCHECK(mState == GuardState::kUninitialized && mLatch != nullptr);
    mVersion = mLatch->mVersion.load();
    if (HasExclusiveMark(mVersion)) {
      lockShared();
      mEncounteredContention = true;
    } else {
      mState = GuardState::kOptimistic;
    }
  }

  inline void ToOptimisticOrExclusive() {
    DCHECK(mState == GuardState::kUninitialized && mLatch != nullptr);
    mVersion = mLatch->mVersion.load();
    if (HasExclusiveMark(mVersion)) {
      lockExclusive();
      mEncounteredContention = true;
    } else {
      mState = GuardState::kOptimistic;
    }
  }

  inline void ToExclusiveMayJump() {
    DCHECK(mState != GuardState::kShared);
    if (mState == GuardState::kExclusive) {
      return;
    }

    if (mState == GuardState::kOptimistic) {
      const u64 newVersion = mVersion + kLatchExclusiveBit;
      u64 expected = mVersion;
      // changed from try_lock because of possible retries b/c lots of readers
      mLatch->mMutex.lock();
      if (!mLatch->mVersion.compare_exchange_strong(expected, newVersion)) {
        mLatch->mMutex.unlock();
        jumpmu::Jump();
      }
      mVersion = newVersion;
      mState = GuardState::kExclusive;
    } else {
      lockExclusive();
    }
  }

  inline void ToSharedMayJump() {
    DCHECK(mState == GuardState::kOptimistic || mState == GuardState::kShared);
    if (mState == GuardState::kShared) {
      return;
    }
    if (mState == GuardState::kOptimistic) {
      mLatch->mMutex.lock_shared();
      if (mLatch->mVersion.load() != mVersion) {
        mLatch->mMutex.unlock_shared();
        jumpmu::Jump();
      }
      mState = GuardState::kShared;
    } else {
      UNREACHABLE();
    }
  }

  // For buffer management
  inline void TryToExclusiveMayJump() {
    DCHECK(mState == GuardState::kOptimistic);
    const u64 newVersion = mVersion + kLatchExclusiveBit;
    u64 expected = mVersion;

    if (!mLatch->mMutex.try_lock()) {
      jumpmu::Jump();
    }

    if (!mLatch->mVersion.compare_exchange_strong(expected, newVersion)) {
      mLatch->mMutex.unlock();
      jumpmu::Jump();
    }

    mVersion = newVersion;
    mState = GuardState::kExclusive;
  }

  inline void TryToSharedMayJump() {
    DCHECK(mState == GuardState::kOptimistic);
    if (!mLatch->mMutex.try_lock_shared()) {
      jumpmu::Jump();
    }
    if (mLatch->mVersion.load() != mVersion) {
      mLatch->mMutex.unlock_shared();
      jumpmu::Jump();
    }
    mState = GuardState::kShared;
  }

private:
  inline void lockExclusive() {
    mLatch->mMutex.lock();
    DCHECK(!HasExclusiveMark(mLatch->mVersion));
    mVersion = mLatch->mVersion.load() + kLatchExclusiveBit;
    mLatch->mVersion.store(mVersion, std::memory_order_release);
    mState = GuardState::kExclusive;
  }

  inline void unlockExclusive() {
    DCHECK(HasExclusiveMark(mLatch->mVersion));
    mVersion += kLatchExclusiveBit;
    mLatch->mVersion.store(mVersion, std::memory_order_release);
    DCHECK(!HasExclusiveMark(mLatch->mVersion));
    mLatch->mMutex.unlock();
    mState = GuardState::kOptimistic;
  }

  inline void lockShared() {
    mLatch->mMutex.lock_shared();
    DCHECK(!HasExclusiveMark(mLatch->mVersion));
    mVersion = mLatch->mVersion.load();
    mState = GuardState::kShared;
  }

  inline void unlockShared() {
    DCHECK(!HasExclusiveMark(mLatch->mVersion));
    mLatch->mMutex.unlock_shared();
    mState = GuardState::kOptimistic;
  }
};

} // namespace storage
} // namespace leanstore
