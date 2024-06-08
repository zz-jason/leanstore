#pragma once

#include "HybridLatch.hpp"
#include "leanstore/Exceptions.hpp"
#include "utils/JumpMU.hpp"
#include "utils/Log.hpp"

#include <atomic>
#include <shared_mutex>

#include <unistd.h>

namespace leanstore::storage {

enum class GuardState : uint8_t {
  kUninitialized = 0,
  kOptimisticShared = 1,
  kPessimisticShared = 2,
  kPessimisticExclusive = 3,
  kMoved = 4,
};

//! Like std::unique_lock, std::shared_lock, std::lock_guard, this HybridGuard
//! is used together with HybridLatch to provide various lock mode.
///
//! TODO(jian.z): should we unlock the guard when it's destroied?
class HybridGuard {
public:
  HybridLatch* mLatch = nullptr;

  GuardState mState = GuardState::kUninitialized;

  uint64_t mVersion;

  bool mEncounteredContention = false;

public:
  HybridGuard(HybridLatch* latch) : mLatch(latch), mState(GuardState::kUninitialized), mVersion(0) {
  }

  // Manually construct a guard from a snapshot. Use with caution!
  HybridGuard(HybridLatch& latch, const uint64_t lastSeenVersion)
      : mLatch(&latch),
        mState(GuardState::kOptimisticShared),
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
    Unlock();

    mLatch = other.mLatch;
    mState = other.mState;
    mVersion = other.mVersion;
    mEncounteredContention = other.mEncounteredContention;
    other.mState = GuardState::kMoved;
    return *this;
  }

public:
  void JumpIfModifiedByOthers() {
    LS_DCHECK(mState == GuardState::kOptimisticShared || mVersion == mLatch->mVersion.load());
    if (mState == GuardState::kOptimisticShared && mVersion != mLatch->mVersion.load()) {
      LS_DLOG("JumpIfModifiedByOthers, mVersion(expected)={}, "
              "mLatch->mVersion(actual)={}",
              mVersion, mLatch->mVersion.load());
      jumpmu::Jump();
    }
  }

  inline void Unlock() {
    switch (mState) {
    case GuardState::kPessimisticExclusive: {
      unlockExclusive();
      break;
    }
    case GuardState::kPessimisticShared: {
      unlockShared();
      break;
    }
    default: {
      break;
    }
    }
    LS_DCHECK(mState == GuardState::kMoved || mState == GuardState::kUninitialized ||
              mState == GuardState::kOptimisticShared);
  }

  inline void ToOptimisticSpin() {
    LS_DCHECK(mState == GuardState::kUninitialized && mLatch != nullptr);
    mVersion = mLatch->mVersion.load();
    while (HasExclusiveMark(mVersion)) {
      mEncounteredContention = true;
      mVersion = mLatch->mVersion.load();
    }
    mState = GuardState::kOptimisticShared;
  }

  inline void ToOptimisticOrJump() {
    LS_DCHECK(mState == GuardState::kUninitialized && mLatch != nullptr);
    mVersion = mLatch->mVersion.load();
    if (HasExclusiveMark(mVersion)) {
      mEncounteredContention = true;
      jumpmu::Jump();
    }
    mState = GuardState::kOptimisticShared;
  }

  inline void ToOptimisticOrShared() {
    if (mState == GuardState::kOptimisticShared || mState == GuardState::kPessimisticShared) {
      return;
    }
    LS_DCHECK(mState == GuardState::kUninitialized || mLatch != nullptr);
    mVersion = mLatch->mVersion.load();
    if (HasExclusiveMark(mVersion)) {
      lockShared();
      mEncounteredContention = true;
    } else {
      mState = GuardState::kOptimisticShared;
    }
  }

  inline void ToOptimisticOrExclusive() {
    LS_DCHECK(mState == GuardState::kUninitialized && mLatch != nullptr);
    mVersion = mLatch->mVersion.load();
    if (HasExclusiveMark(mVersion)) {
      lockExclusive();
      mEncounteredContention = true;
    } else {
      mState = GuardState::kOptimisticShared;
    }
  }

  inline void ToExclusiveMayJump() {
    LS_DCHECK(mState != GuardState::kPessimisticShared);
    if (mState == GuardState::kPessimisticExclusive) {
      return;
    }

    if (mState == GuardState::kOptimisticShared) {
      const uint64_t newVersion = mVersion + kLatchExclusiveBit;
      uint64_t expected = mVersion;
      // changed from try_lock because of possible retries b/c lots of readers
      mLatch->mMutex.lock();
      if (!mLatch->mVersion.compare_exchange_strong(expected, newVersion)) {
        mLatch->mMutex.unlock();
        jumpmu::Jump();
      }
      mVersion = newVersion;
      mState = GuardState::kPessimisticExclusive;
    } else {
      lockExclusive();
    }
  }

  inline void ToSharedMayJump() {
    LS_DCHECK(mState == GuardState::kOptimisticShared || mState == GuardState::kPessimisticShared);
    if (mState == GuardState::kPessimisticShared) {
      return;
    }
    if (mState == GuardState::kOptimisticShared) {
      mLatch->mMutex.lock_shared();
      if (mLatch->mVersion.load() != mVersion) {
        mLatch->mMutex.unlock_shared();
        jumpmu::Jump();
      }
      mState = GuardState::kPessimisticShared;
    } else {
      UNREACHABLE();
    }
  }

  // For buffer management
  inline void TryToExclusiveMayJump() {
    LS_DCHECK(mState == GuardState::kOptimisticShared);
    const uint64_t newVersion = mVersion + kLatchExclusiveBit;
    uint64_t expected = mVersion;

    if (!mLatch->mMutex.try_lock()) {
      jumpmu::Jump();
    }
    if (!mLatch->mVersion.compare_exchange_strong(expected, newVersion)) {
      mLatch->mMutex.unlock();
      jumpmu::Jump();
    }

    mVersion = newVersion;
    mState = GuardState::kPessimisticExclusive;
  }

  inline void TryToSharedMayJump() {
    LS_DCHECK(mState == GuardState::kOptimisticShared);
    if (!mLatch->mMutex.try_lock_shared()) {
      jumpmu::Jump();
    }
    if (mLatch->mVersion.load() != mVersion) {
      mLatch->mMutex.unlock_shared();
      jumpmu::Jump();
    }
    mState = GuardState::kPessimisticShared;
  }

private:
  inline void lockExclusive() {
    mLatch->mMutex.lock();
    LS_DCHECK(!HasExclusiveMark(mLatch->mVersion));
    mVersion = mLatch->mVersion.load() + kLatchExclusiveBit;
    mLatch->mVersion.store(mVersion, std::memory_order_release);
    mState = GuardState::kPessimisticExclusive;
  }

  inline void unlockExclusive() {
    LS_DCHECK(HasExclusiveMark(mLatch->mVersion));
    mVersion += kLatchExclusiveBit;
    mLatch->mVersion.store(mVersion, std::memory_order_release);
    LS_DCHECK(!HasExclusiveMark(mLatch->mVersion));
    mLatch->mMutex.unlock();
    mState = GuardState::kOptimisticShared;
  }

  inline void lockShared() {
    mLatch->mMutex.lock_shared();
    LS_DCHECK(!HasExclusiveMark(mLatch->mVersion));
    mVersion = mLatch->mVersion.load();
    mState = GuardState::kPessimisticShared;
  }

  inline void unlockShared() {
    LS_DCHECK(!HasExclusiveMark(mLatch->mVersion));
    mLatch->mMutex.unlock_shared();
    mState = GuardState::kOptimisticShared;
  }
};

} // namespace leanstore::storage
