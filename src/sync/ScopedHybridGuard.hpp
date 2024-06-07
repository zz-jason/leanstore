#pragma once

#include "sync/HybridLatch.hpp"
#include "utils/JumpMU.hpp"
#include "utils/Log.hpp"

#include <atomic>
#include <functional>
#include <shared_mutex>

#include <sys/mman.h>
#include <unistd.h>

namespace leanstore::test {

class ScopedHybridGuardTest;

} // namespace leanstore::test

namespace leanstore {
namespace storage {

//! A scoped guard for the hybrid latch. It locks the latch in the specified
//! mode when constructed, and unlocks the latch when destructed.
//! The guard is movable but not copyable.
class ScopedHybridGuard {
private:
  //! The latch to guard.
  HybridLatch* mLatch;

  //! The latch mode.
  LatchMode mLatchMode;

  //! The version of the latch when it was optimistically locked.
  uint64_t mVersionOnLock;

  //! Whether the guard has encountered contention, checked when the latch is
  //! optimistically locked.
  bool mEncounteredContention;

  //! Whether the guard has locked the latch.
  bool mLocked;

public:
  //! Construct a guard for the latch, lock it immediately in the specified
  //! latch mode. It may jump if latchMode is kOptimisticOrJump and the latch is
  //! exclusive locked by others.
  ScopedHybridGuard(HybridLatch& latch, LatchMode latchMode)
      : mLatch(&latch),
        mLatchMode(latchMode),
        mVersionOnLock(0),
        mEncounteredContention(false),
        mLocked(false) {
    Lock();
  }

  //! Construct a guard for the latch, lock it in kOptimisticOrJump mode with
  //! the specified version. It may jump if the optimistic version does not
  //! match the current version.
  ScopedHybridGuard(HybridLatch& latch, uint64_t version)
      : mLatch(&latch),
        mLatchMode(LatchMode::kOptimisticOrJump),
        mVersionOnLock(version),
        mEncounteredContention(false),
        mLocked(false) {
    // jump if the optimistic lock is invalid
    jumpIfModifiedByOthers();
    mLocked = true;
  }

  //! Destruct the guard, unlock the latch if it is locked.
  ~ScopedHybridGuard() {
    Unlock();
  }

  //! No copy construct
  ScopedHybridGuard(const ScopedHybridGuard&) = delete;

  //! No copy assign
  ScopedHybridGuard& operator=(const ScopedHybridGuard& other) = delete;

  //! Move construct
  ScopedHybridGuard(ScopedHybridGuard&& other) {
    *this = std::move(other);
  }

  //! Move assign
  ScopedHybridGuard& operator=(ScopedHybridGuard&& other) {
    Unlock();

    mLatch = other.mLatch;
    mLatchMode = other.mLatchMode;
    mVersionOnLock = other.mVersionOnLock;
    mEncounteredContention = other.mEncounteredContention;
    mLocked = other.mLocked;
    other.mLocked = false;
    return *this;
  }

  //! Lock the latch in the specified mode if it is not locked.
  void Lock();

  //! Unlock the latch if it is locked.
  void Unlock();

  static void GetOptimistic(HybridLatch& latch, LatchMode latchMode, std::function<void()> copier);

  static void Get(HybridLatch& latch, std::function<void()> copier);

private:
  //! Lock the latch in kOptimisticOrJump mode.
  void lockOptimisticOrJump();

  //! Lock the latch in kOptimisticSpin mode.
  void lockOptimisticSpin();

  //! Unlock the latch in kOptimisticOrJump or kOptimisticSpin mode.
  void unlockOptimisticOrJump();

  //! Jump if the latch has been modified by others.
  void jumpIfModifiedByOthers();

  //! Lock the latch in kPessimisticShared mode.
  void lockPessimisticShared();

  //! Unlock the latch in kPessimisticShared mode.
  void unlockPessimisticShared();

  //! Lock the latch in kPessimisticExclusive mode.
  void lockPessimisticExclusive();

  //! Unlock the latch in kPessimisticExclusive mode.
  void unlockPessimisticExclusive();

private:
  //! Allow the test class to access private members.
  friend class leanstore::test::ScopedHybridGuardTest;
};

inline void ScopedHybridGuard::GetOptimistic(HybridLatch& latch, LatchMode latchMode,
                                             std::function<void()> copier) {
  LS_DCHECK(latchMode == LatchMode::kOptimisticOrJump || latchMode == LatchMode::kOptimisticSpin);
  while (true) {
    JUMPMU_TRY() {
      auto guard = ScopedHybridGuard(latch, latchMode);
      copier();
    }
    JUMPMU_CATCH() {
      continue;
    }
    return;
  }
}

inline void ScopedHybridGuard::Get(HybridLatch& latch, std::function<void()> copier) {
  while (true) {
    JUMPMU_TRY() {
      auto guard = ScopedHybridGuard(latch, LatchMode::kOptimisticOrJump);
      copier();
      guard.Unlock();
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
      auto guard = ScopedHybridGuard(latch, LatchMode::kPessimisticShared);
      copier();
      guard.Unlock();
      return;
    }
  }
}

inline void ScopedHybridGuard::Lock() {
  if (mLocked) {
    return;
  }

  switch (mLatchMode) {
  case LatchMode::kOptimisticOrJump: {
    lockOptimisticOrJump();
    break;
  }
  case LatchMode::kOptimisticSpin: {
    lockOptimisticSpin();
    break;
  }
  case LatchMode::kPessimisticShared: {
    lockPessimisticShared();
    break;
  }
  case LatchMode::kPessimisticExclusive: {
    lockPessimisticExclusive();
    break;
  }
  default: {
    Log::Error("Unsupported latch mode: {}", (uint64_t)mLatchMode);
  }
  }
  mLocked = true;
}

inline void ScopedHybridGuard::Unlock() {
  if (!mLocked) {
    return;
  }

  switch (mLatchMode) {
  case LatchMode::kOptimisticOrJump: {
    unlockOptimisticOrJump();
    break;
  }
  case LatchMode::kOptimisticSpin: {
    unlockOptimisticOrJump();
    break;
  }
  case LatchMode::kPessimisticShared: {
    unlockPessimisticShared();
    break;
  }
  case LatchMode::kPessimisticExclusive: {
    unlockPessimisticExclusive();
    break;
  }
  default: {
    Log::Error("Unsupported latch mode: {}", (uint64_t)mLatchMode);
  }
  }

  mLocked = false;
}

inline void ScopedHybridGuard::lockOptimisticOrJump() {
  LS_DCHECK(mLatchMode == LatchMode::kOptimisticOrJump && mLatch != nullptr);
  mVersionOnLock = mLatch->mVersion.load();
  if (HasExclusiveMark(mVersionOnLock)) {
    mEncounteredContention = true;
    LS_DLOG("lockOptimisticOrJump() failed, target latch, latch={}, version={}", (void*)&mLatch,
            mVersionOnLock);
    jumpmu::Jump();
  }
}

inline void ScopedHybridGuard::lockOptimisticSpin() {
  LS_DCHECK(mLatchMode == LatchMode::kOptimisticSpin && mLatch != nullptr);
  mVersionOnLock = mLatch->mVersion.load();
  while (HasExclusiveMark(mVersionOnLock)) {
    mEncounteredContention = true;
    mVersionOnLock = mLatch->mVersion.load();
  }
}

inline void ScopedHybridGuard::unlockOptimisticOrJump() {
  LS_DCHECK(
      (mLatchMode == LatchMode::kOptimisticOrJump || mLatchMode == LatchMode::kOptimisticSpin) &&
      mLatch != nullptr);
  jumpIfModifiedByOthers();
}

inline void ScopedHybridGuard::jumpIfModifiedByOthers() {
  auto curVersion = mLatch->mVersion.load();
  if (mVersionOnLock != curVersion) {
    mEncounteredContention = true;
    LS_DLOG("jumpIfModifiedByOthers() failed, target latch, latch={}, "
            "version(expected)={}, version(actual)={}",
            (void*)&mLatch, mVersionOnLock, curVersion);
    jumpmu::Jump();
  }
}

inline void ScopedHybridGuard::lockPessimisticShared() {
  LS_DCHECK(mLatchMode == LatchMode::kPessimisticShared && mLatch != nullptr);
  mLatch->mMutex.lock_shared();
}

inline void ScopedHybridGuard::unlockPessimisticShared() {
  LS_DCHECK(mLatchMode == LatchMode::kPessimisticShared && mLatch != nullptr);
  mLatch->mMutex.unlock_shared();
}

inline void ScopedHybridGuard::lockPessimisticExclusive() {
  LS_DCHECK(mLatchMode == LatchMode::kPessimisticExclusive && mLatch != nullptr);
  mLatch->LockExclusively();
}

inline void ScopedHybridGuard::unlockPessimisticExclusive() {
  LS_DCHECK(mLatchMode == LatchMode::kPessimisticExclusive && mLatch != nullptr);
  mLatch->UnlockExclusively();
}

} // namespace storage
} // namespace leanstore