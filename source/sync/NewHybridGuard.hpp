#pragma once

#include "sync/HybridLatch.hpp"
#include "utils/JumpMU.hpp"

#include <glog/logging.h>

#include <atomic>
#include <shared_mutex>

#include <sys/mman.h>
#include <unistd.h>

namespace leanstore {
namespace storage {

class ScopedHybridGuard {
private:
  HybridLatch* mLatch;

  LatchMode mLatchMode;

  uint64_t mVersionOnLock;

  bool mEncounteredContention;

  bool mLocked;

public:
  ScopedHybridGuard(HybridLatch& latch, LatchMode latchMode)
      : mLatch(&latch),
        mLatchMode(latchMode),
        mVersionOnLock(0),
        mEncounteredContention(false),
        mLocked(false) {
    Lock();
  }

  ScopedHybridGuard(HybridLatch& latch, uint64_t version)
      : mLatch(&latch),
        mLatchMode(LatchMode::kOptimisticSpin),
        mVersionOnLock(version),
        mEncounteredContention(false),
        mLocked(true) {
    // jump if the optimistic lock is invalid
    jumpIfModifiedByOthers();
  }

  ~ScopedHybridGuard() {
    Unlock();
  }

  /// no copy construct
  ScopedHybridGuard(const ScopedHybridGuard&) = delete;

  /// no copy assign
  ScopedHybridGuard& operator=(const ScopedHybridGuard& other) = delete;

  /// move construct
  ScopedHybridGuard(ScopedHybridGuard&& other) {
    *this = std::move(other);
  }

  /// move assign
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

  void Lock();

  void Unlock();

private:
  void lockOptimisticOrJump();
  void lockOptimisticSpin();
  void unlockOptimisticOrJump();
  void jumpIfModifiedByOthers();

  void lockPessimisticShared();
  void unlockPessimisticShared();

  void lockPessimisticExclusive();
  void unlockPessimisticExclusive();
};

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
    LOG(ERROR) << "Unsupported latch mode: " << (uint64_t)mLatchMode;
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
    LOG(ERROR) << "Unsupported latch mode: " << (uint64_t)mLatchMode;
  }
  }

  mLocked = false;
}

inline void ScopedHybridGuard::lockOptimisticOrJump() {
  DCHECK(mLatchMode == LatchMode::kOptimisticOrJump && mLatch != nullptr);
  mVersionOnLock = mLatch->mVersion.load();
  if (HasExclusiveMark(mVersionOnLock)) {
    mEncounteredContention = true;
    DLOG(INFO) << "lockOptimisticOrJump() failed, target latch"
               << " (" << (void*)&mLatch << ")"
               << " is exclusive locked by others, jump";
    jumpmu::Jump();
  }
}

inline void ScopedHybridGuard::lockOptimisticSpin() {
  DCHECK(mLatchMode == LatchMode::kOptimisticOrJump && mLatch != nullptr);
  mVersionOnLock = mLatch->mVersion.load();
  while (HasExclusiveMark(mVersionOnLock)) {
    mEncounteredContention = true;
    mVersionOnLock = mLatch->mVersion.load();
  }
}

inline void ScopedHybridGuard::unlockOptimisticOrJump() {
  DCHECK((mLatchMode == LatchMode::kOptimisticOrJump ||
          mLatchMode == LatchMode::kOptimisticSpin) &&
         mLatch != nullptr);
  jumpIfModifiedByOthers();
}

inline void ScopedHybridGuard::jumpIfModifiedByOthers() {
  auto curVersion = mLatch->mVersion.load();
  if (mVersionOnLock != curVersion) {
    mEncounteredContention = true;
    DLOG(INFO)
        << "unlockOptimisticOrJump() failed, object protected by target latch"
        << " (" << (void*)&mLatch << ")"
        << " has been modified, jump";
    jumpmu::Jump();
  }
}

inline void ScopedHybridGuard::lockPessimisticShared() {
  DCHECK(mLatchMode == LatchMode::kPessimisticShared && mLatch != nullptr);
  mLatch->mMutex.lock_shared();
}

inline void ScopedHybridGuard::unlockPessimisticShared() {
  DCHECK(mLatchMode == LatchMode::kPessimisticShared && mLatch != nullptr);
  DCHECK(mVersionOnLock == mLatch->mVersion);
  mLatch->mMutex.unlock_shared();
}

inline void ScopedHybridGuard::lockPessimisticExclusive() {
  DCHECK(mLatchMode == LatchMode::kPessimisticExclusive && mLatch != nullptr);
  mLatch->LockExclusively();
}

inline void ScopedHybridGuard::unlockPessimisticExclusive() {
  DCHECK(mLatchMode == LatchMode::kPessimisticExclusive && mLatch != nullptr);
  mLatch->UnlockExclusively();
}

} // namespace storage
} // namespace leanstore