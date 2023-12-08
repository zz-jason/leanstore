#pragma once
#include "Units.hpp"
#include "Config.hpp"
#include "sync-primitives/Latch.hpp"
#include "utils/JumpMU.hpp"
#include "utils/RandomGenerator.hpp"

namespace leanstore {
namespace storage {

// The following guards are primarily designed for buffer management use cases
// This implies that the guards never block (sleep), they immediately jump
// instead.
class BMOptimisticGuard;
class BMExclusiveGuard;
template <typename T> class HybridPageGuard;

class BMOptimisticGuard {
  friend class BMExclusiveGuard;
  template <typename T> friend class HybridPageGuard;
  template <typename T> friend class ExclusivePageGuard;

public:
  Guard guard;

  BMOptimisticGuard(HybridLatch& lock) : guard(lock) {
    guard.toOptimisticOrJump();
  }

  BMOptimisticGuard() = delete;
  BMOptimisticGuard(BMOptimisticGuard& other) = delete; // copy constructor
  // move constructor
  BMOptimisticGuard(BMOptimisticGuard&& other) : guard(std::move(other.guard)) {
  }
  BMOptimisticGuard& operator=(BMOptimisticGuard& other) = delete;
  BMOptimisticGuard& operator=(BMOptimisticGuard&& other) {
    guard = std::move(other.guard);
    return *this;
  }

  inline void JumpIfModifiedByOthers() {
    guard.JumpIfModifiedByOthers();
  }
};

class BMExclusiveGuard {
private:
  BMOptimisticGuard& mOptimisticGuard; // our basis

public:
  BMExclusiveGuard(BMOptimisticGuard& optimisiticGuard)
      : mOptimisticGuard(optimisiticGuard) {
    mOptimisticGuard.guard.tryToExclusive();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(BMExclusiveGuard)

  ~BMExclusiveGuard() {
    mOptimisticGuard.guard.unlock();
    JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP();
  }
};

class BMExclusiveUpgradeIfNeeded {
private:
  Guard& guard;
  const bool was_exclusive;

public:
  BMExclusiveUpgradeIfNeeded(Guard& guard)
      : guard(guard), was_exclusive(guard.state == GUARD_STATE::EXCLUSIVE) {
    guard.tryToExclusive();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(BMExclusiveUpgradeIfNeeded)

  ~BMExclusiveUpgradeIfNeeded() {
    if (!was_exclusive) {
      guard.unlock();
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
    mOptimisticGuard.guard.tryToShared();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(BMSharedGuard)

  ~BMSharedGuard() {
    mOptimisticGuard.guard.unlock();
    JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP()
  }
};

} // namespace storage
} // namespace leanstore
