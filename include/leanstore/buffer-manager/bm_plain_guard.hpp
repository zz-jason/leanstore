#pragma once

#include "leanstore/sync/hybrid_guard.hpp"
#include "leanstore/utils/jump_mu.hpp"

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
  HybridGuard guard_;

  BMOptimisticGuard(HybridLatch& lock) : guard_(&lock) {
    guard_.ToOptimisticOrJump();
  }

  BMOptimisticGuard() = delete;
  BMOptimisticGuard(BMOptimisticGuard& other) = delete; // copy constructor
  // move constructor
  BMOptimisticGuard(BMOptimisticGuard&& other) : guard_(std::move(other.guard_)) {
  }
  BMOptimisticGuard& operator=(BMOptimisticGuard& other) = delete;
  BMOptimisticGuard& operator=(BMOptimisticGuard&& other) {
    guard_ = std::move(other.guard_);
    return *this;
  }

  inline void JumpIfModifiedByOthers() {
    guard_.JumpIfModifiedByOthers();
  }
};

class BMExclusiveGuard {
private:
  BMOptimisticGuard& optimistic_guard_; // our basis

public:
  BMExclusiveGuard(BMOptimisticGuard& optimistic_guard) : optimistic_guard_(optimistic_guard) {
    optimistic_guard_.guard_.TryToExclusiveMayJump();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(BMExclusiveGuard)

  ~BMExclusiveGuard() {
    optimistic_guard_.guard_.Unlock();
    JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP();
  }
};

class BMExclusiveUpgradeIfNeeded {
private:
  HybridGuard& guard_;

  const bool was_exclusive_;

public:
  BMExclusiveUpgradeIfNeeded(HybridGuard& guard)
      : guard_(guard),
        was_exclusive_(guard.state_ == GuardState::kExclusivePessimistic) {
    guard_.TryToExclusiveMayJump();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(BMExclusiveUpgradeIfNeeded)

  ~BMExclusiveUpgradeIfNeeded() {
    if (!was_exclusive_) {
      guard_.Unlock();
    }
    JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP()
  }
};

class BMSharedGuard {
private:
  BMOptimisticGuard& optimistic_guard_; // our basis

public:
  BMSharedGuard(BMOptimisticGuard& optimistic_guard) : optimistic_guard_(optimistic_guard) {
    optimistic_guard_.guard_.TryToSharedMayJump();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(BMSharedGuard)

  ~BMSharedGuard() {
    optimistic_guard_.guard_.Unlock();
    JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP()
  }
};

} // namespace storage
} // namespace leanstore
