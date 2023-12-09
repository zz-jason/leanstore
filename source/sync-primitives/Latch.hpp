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

class alignas(64) HybridLatch {
public:
  atomic<u64> version;

  std::shared_mutex mutex;

public:
  template <typename... Args>
  HybridLatch(Args&&... args) : version(std::forward<Args>(args)...) {
  }

public:
  atomic<u64>* operator->() {
    return &version;
  }

  atomic<u64>* ptr() {
    return &version;
  }

  atomic<u64>& ref() {
    return version;
  }

  bool isExclusivelyLatched() {
    return (version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT;
  }
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

class Guard {
public:
  HybridLatch* latch = nullptr;

  GUARD_STATE state = GUARD_STATE::UNINITIALIZED;

  u64 version;

  bool mEncounteredContention = false;

public:
  Guard(HybridLatch& latch) : latch(&latch) {
  }

  Guard(HybridLatch* latch) : latch(latch) {
  }

  // Manually construct a guard from a snapshot. Use with caution!
  Guard(HybridLatch& latch, const u64 last_seen_version)
      : latch(&latch), state(GUARD_STATE::OPTIMISTIC),
        version(last_seen_version), mEncounteredContention(false) {
  }

  Guard(HybridLatch& latch, GUARD_STATE state)
      : latch(&latch), state(state), version(latch.ref().load()) {
  }

  // Move constructor
  Guard(Guard&& other)
      : latch(other.latch), state(other.state), version(other.version),
        mEncounteredContention(other.mEncounteredContention) {
    other.state = GUARD_STATE::MOVED;
  }

  // Move assignment
  Guard& operator=(Guard&& other) {
    unlock();

    latch = other.latch;
    state = other.state;
    version = other.version;
    mEncounteredContention = other.mEncounteredContention;
    other.state = GUARD_STATE::MOVED;
    return *this;
  }

public:
  void JumpIfModifiedByOthers() {
    assert(state == GUARD_STATE::OPTIMISTIC || version == latch->ref().load());
    if (state == GUARD_STATE::OPTIMISTIC && version != latch->ref().load()) {
      jumpmu::jump();
    }
  }

  inline void unlock() {
    if (state == GUARD_STATE::EXCLUSIVE) {
      version += LATCH_EXCLUSIVE_BIT;
      latch->ref().store(version, std::memory_order_release);
      latch->mutex.unlock();
      state = GUARD_STATE::OPTIMISTIC;
    } else if (state == GUARD_STATE::SHARED) {
      latch->mutex.unlock_shared();
      state = GUARD_STATE::OPTIMISTIC;
    }
  }

  inline void toOptimisticSpin() {
    assert(state == GUARD_STATE::UNINITIALIZED && latch != nullptr &&
           state != GUARD_STATE::MOVED);
    version = latch->ref().load();
    if ((version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {
      mEncounteredContention = true;
      do {
        version = latch->ref().load();
      } while ((version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT);
    }
    state = GUARD_STATE::OPTIMISTIC;
  }

  inline void toOptimisticOrJump() {
    assert(state == GUARD_STATE::UNINITIALIZED && latch != nullptr);
    version = latch->ref().load();
    if ((version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {
      jumpmu::jump();
    } else {
      state = GUARD_STATE::OPTIMISTIC;
    }
  }

  inline void toOptimisticOrShared() {
    assert(state == GUARD_STATE::UNINITIALIZED && latch != nullptr &&
           state != GUARD_STATE::MOVED);
    version = latch->ref().load();
    if ((version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {
      latch->mutex.lock_shared();
      version = latch->ref().load();
      state = GUARD_STATE::SHARED;
      mEncounteredContention = true;
    } else {
      state = GUARD_STATE::OPTIMISTIC;
    }
  }

  inline void toOptimisticOrExclusive() {
    assert(state == GUARD_STATE::UNINITIALIZED && latch != nullptr &&
           state != GUARD_STATE::MOVED);
    version = latch->ref().load();
    if ((version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {
      latch->mutex.lock();
      version = latch->ref().load() + LATCH_EXCLUSIVE_BIT;
      latch->ref().store(version, std::memory_order_release);
      state = GUARD_STATE::EXCLUSIVE;
      mEncounteredContention = true;
    } else {
      state = GUARD_STATE::OPTIMISTIC;
    }
  }

  inline void toExclusive() {
    assert(state != GUARD_STATE::SHARED);
    if (state == GUARD_STATE::EXCLUSIVE)
      return;
    if (state == GUARD_STATE::OPTIMISTIC) {
      const u64 new_version = version + LATCH_EXCLUSIVE_BIT;
      u64 expected = version;
      latch->mutex.lock(); // changed from try_lock because of possible retries
                           // b/c lots of readers
      if (!latch->ref().compare_exchange_strong(expected, new_version)) {
        latch->mutex.unlock();
        jumpmu::jump();
      }
      version = new_version;
      state = GUARD_STATE::EXCLUSIVE;
    } else {
      latch->mutex.lock();
      version = latch->ref().load() + LATCH_EXCLUSIVE_BIT;
      latch->ref().store(version, std::memory_order_release);
      state = GUARD_STATE::EXCLUSIVE;
    }
  }

  inline void toShared() {
    assert(state == GUARD_STATE::OPTIMISTIC || state == GUARD_STATE::SHARED);
    if (state == GUARD_STATE::SHARED)
      return;
    if (state == GUARD_STATE::OPTIMISTIC) {
      latch->mutex.lock_shared();
      if (latch->ref().load() != version) {
        latch->mutex.unlock_shared();
        jumpmu::jump();
      }
      state = GUARD_STATE::SHARED;
    } else {
      UNREACHABLE();
    }
  }

  // For buffer management
  inline void tryToExclusive() {
    assert(state == GUARD_STATE::OPTIMISTIC);
    const u64 new_version = version + LATCH_EXCLUSIVE_BIT;
    u64 expected = version;

    if (!latch->mutex.try_lock()) {
      jumpmu::jump();
    }

    if (!latch->ref().compare_exchange_strong(expected, new_version)) {
      latch->mutex.unlock();
      jumpmu::jump();
    }

    version = new_version;
    state = GUARD_STATE::EXCLUSIVE;
  }

  inline void tryToShared() {
    assert(state == GUARD_STATE::OPTIMISTIC);
    if (!latch->mutex.try_lock_shared()) {
      jumpmu::jump();
    }
    if (latch->ref().load() != version) {
      latch->mutex.unlock_shared();
      jumpmu::jump();
    }
    state = GUARD_STATE::SHARED;
  }
};

} // namespace storage
} // namespace leanstore
