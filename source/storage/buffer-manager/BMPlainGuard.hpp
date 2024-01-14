#pragma once

#include "sync-primitives/HybridGuard.hpp"
#include "utils/JumpMU.hpp"

namespace leanstore {
namespace storage {

// The following guards are primarily designed for buffer management use cases
// This implies that the guards never block (sleep), they immediately jump
// instead.
class BMOptimisticGuard;
class BMExclusiveGuard;
template <typename T> class GuardedBufferFrame;

class BMOptimisticGuard {
  friend class BMExclusiveGuard;
  template <typename T> friend class GuardedBufferFrame;
  template <typename T> friend class ExclusiveGuardedBufferFrame;

public:
  HybridGuard mGuard;

  BMOptimisticGuard(HybridLatch& lock) : mGuard(&lock) {
    mGuard.toOptimisticOrJump();
  }

  BMOptimisticGuard() = delete;
  BMOptimisticGuard(BMOptimisticGuard& other) = delete; // copy constructor
  // move constructor
  BMOptimisticGuard(BMOptimisticGuard&& other)
      : mGuard(std::move(other.mGuard)) {
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
  BMExclusiveGuard(BMOptimisticGuard& optimisiticGuard)
      : mOptimisticGuard(optimisiticGuard) {
    mOptimisticGuard.mGuard.TryToExclusiveMayJump();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(BMExclusiveGuard)

  ~BMExclusiveGuard() {
    mOptimisticGuard.mGuard.unlock();
    JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP();
  }
};

class BMExclusiveUpgradeIfNeeded {
private:
  HybridGuard& mGuard;
  const bool was_exclusive;

public:
  BMExclusiveUpgradeIfNeeded(HybridGuard& guard)
      : mGuard(guard), was_exclusive(guard.mState == GuardState::kExclusive) {
    mGuard.TryToExclusiveMayJump();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(BMExclusiveUpgradeIfNeeded)

  ~BMExclusiveUpgradeIfNeeded() {
    if (!was_exclusive) {
      mGuard.unlock();
    }
    JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP()
  }
};

class BMSharedGuard {
private:
  BMOptimisticGuard& mOptimisticGuard; // our basis

public:
  BMSharedGuard(BMOptimisticGuard& optimisiticGuard)
      : mOptimisticGuard(optimisiticGuard) {
    mOptimisticGuard.mGuard.TryToSharedMayJump();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(BMSharedGuard)

  ~BMSharedGuard() {
    mOptimisticGuard.mGuard.unlock();
    JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP()
  }
};

} // namespace storage
} // namespace leanstore
