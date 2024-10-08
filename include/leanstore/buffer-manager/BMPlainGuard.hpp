#pragma once

#include "leanstore/sync/HybridGuard.hpp"
#include "leanstore/utils/JumpMU.hpp"

namespace leanstore {
namespace storage {

// The following guards are primarily designed for buffer management use cases
// This implies that the guards never block (sleep), they immediately jump
// instead.
class BMOptimisticGuard;
class BMExclusiveGuard;
template <typename T>
class GuardedBufferFrame;

class BMOptimisticGuard {
  friend class BMExclusiveGuard;
  template <typename T>
  friend class GuardedBufferFrame;
  template <typename T>
  friend class ExclusiveGuardedBufferFrame;

public:
  HybridGuard mGuard;

  BMOptimisticGuard(HybridLatch& lock) : mGuard(&lock) {
    mGuard.ToOptimisticOrJump();
  }

  BMOptimisticGuard() = delete;
  BMOptimisticGuard(BMOptimisticGuard& other) = delete; // copy constructor
  // move constructor
  BMOptimisticGuard(BMOptimisticGuard&& other) : mGuard(std::move(other.mGuard)) {
  }
  BMOptimisticGuard& operator=(BMOptimisticGuard& other) = delete;
  BMOptimisticGuard& operator=(BMOptimisticGuard&& other) {
    mGuard = std::move(other.mGuard);
    return *this;
  }

  inline void JumpIfModifiedByOthers() {
    mGuard.JumpIfModifiedByOthers();
  }
};

class BMExclusiveGuard {
private:
  BMOptimisticGuard& mOptimisticGuard; // our basis

public:
  BMExclusiveGuard(BMOptimisticGuard& optimisticGuard) : mOptimisticGuard(optimisticGuard) {
    mOptimisticGuard.mGuard.TryToExclusiveMayJump();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(BMExclusiveGuard)

  ~BMExclusiveGuard() {
    mOptimisticGuard.mGuard.Unlock();
    JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP();
  }
};

class BMExclusiveUpgradeIfNeeded {
private:
  HybridGuard& mGuard;

  const bool mWasExclusive;

public:
  BMExclusiveUpgradeIfNeeded(HybridGuard& guard)
      : mGuard(guard),
        mWasExclusive(guard.mState == GuardState::kPessimisticExclusive) {
    mGuard.TryToExclusiveMayJump();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(BMExclusiveUpgradeIfNeeded)

  ~BMExclusiveUpgradeIfNeeded() {
    if (!mWasExclusive) {
      mGuard.Unlock();
    }
    JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP()
  }
};

class BMSharedGuard {
private:
  BMOptimisticGuard& mOptimisticGuard; // our basis

public:
  BMSharedGuard(BMOptimisticGuard& optimisticGuard) : mOptimisticGuard(optimisticGuard) {
    mOptimisticGuard.mGuard.TryToSharedMayJump();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(BMSharedGuard)

  ~BMSharedGuard() {
    mOptimisticGuard.mGuard.Unlock();
    JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP()
  }
};

} // namespace storage
} // namespace leanstore
