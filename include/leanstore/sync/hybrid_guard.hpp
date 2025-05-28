#pragma once

#include "hybrid_latch.hpp"
#include "leanstore/exceptions.hpp"
#include "leanstore/utils/jump_mu.hpp"
#include "leanstore/utils/log.hpp"

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

/// Like std::unique_lock, std::shared_lock, std::lock_guard, this HybridGuard
/// is used together with HybridLatch to provide various lock mode.
///
/// TODO(jian.z): should we unlock the guard when it's destroied?
class HybridGuard {
public:
  HybridLatch* latch_ = nullptr;

  GuardState state_ = GuardState::kUninitialized;

  uint64_t version_;

  bool encountered_contention_ = false;

public:
  HybridGuard() = default;

  HybridGuard(HybridLatch* latch) : latch_(latch), state_(GuardState::kUninitialized), version_(0) {
  }

  // Manually construct a guard from a snapshot. Use with caution!
  HybridGuard(HybridLatch& latch, const uint64_t last_seen_version)
      : latch_(&latch),
        state_(GuardState::kOptimisticShared),
        version_(last_seen_version),
        encountered_contention_(false) {
  }

  HybridGuard(HybridLatch& latch, GuardState state)
      : latch_(&latch),
        state_(state),
        version_(latch.version_.load()) {
  }

  // Move constructor
  HybridGuard(HybridGuard&& other) {
    *this = std::move(other);
  }

  // Move assignment
  HybridGuard& operator=(HybridGuard&& other) {
    Unlock();

    latch_ = other.latch_;
    state_ = other.state_;
    version_ = other.version_;
    encountered_contention_ = other.encountered_contention_;
    other.state_ = GuardState::kMoved;
    return *this;
  }

public:
  void JumpIfModifiedByOthers() {
    LS_DCHECK(state_ == GuardState::kOptimisticShared || version_ == latch_->version_.load());
    if (state_ == GuardState::kOptimisticShared && version_ != latch_->version_.load()) {
      LS_DLOG("JumpIfModifiedByOthers, version_(expected)={}, "
              "latch_->version_(actual)={}",
              version_, latch_->version_.load());
      jumpmu::Jump();
    }
  }

  inline void Unlock() {
    switch (state_) {
    case GuardState::kPessimisticExclusive: {
      unlock_exclusive();
      break;
    }
    case GuardState::kPessimisticShared: {
      unlock_shared();
      break;
    }
    default: {
      break;
    }
    }
    LS_DCHECK(state_ == GuardState::kMoved || state_ == GuardState::kUninitialized ||
              state_ == GuardState::kOptimisticShared);
  }

  inline void ToOptimisticSpin() {
    LS_DCHECK(state_ == GuardState::kUninitialized && latch_ != nullptr);
    version_ = latch_->version_.load();
    while (HasExclusiveMark(version_)) {
      encountered_contention_ = true;
      version_ = latch_->version_.load();
    }
    state_ = GuardState::kOptimisticShared;
  }

  inline void ToOptimisticOrJump() {
    LS_DCHECK(state_ == GuardState::kUninitialized && latch_ != nullptr);
    version_ = latch_->version_.load();
    if (HasExclusiveMark(version_)) {
      encountered_contention_ = true;
      jumpmu::Jump();
    }
    state_ = GuardState::kOptimisticShared;
  }

  inline void ToOptimisticOrShared() {
    if (state_ == GuardState::kOptimisticShared || state_ == GuardState::kPessimisticShared) {
      return;
    }
    LS_DCHECK(state_ == GuardState::kUninitialized || latch_ != nullptr);
    version_ = latch_->version_.load();
    if (HasExclusiveMark(version_)) {
      lock_shared();
      encountered_contention_ = true;
    } else {
      state_ = GuardState::kOptimisticShared;
    }
  }

  inline void ToOptimisticOrExclusive() {
    LS_DCHECK(state_ == GuardState::kUninitialized && latch_ != nullptr);
    version_ = latch_->version_.load();
    if (HasExclusiveMark(version_)) {
      lock_exclusive();
      encountered_contention_ = true;
    } else {
      state_ = GuardState::kOptimisticShared;
    }
  }

  inline void ToExclusiveMayJump() {
    LS_DCHECK(state_ != GuardState::kPessimisticShared);
    if (state_ == GuardState::kPessimisticExclusive) {
      return;
    }

    if (state_ == GuardState::kOptimisticShared) {
      const uint64_t new_version = version_ + kLatchExclusiveBit;
      uint64_t expected = version_;
      // changed from try_lock because of possible retries b/c lots of readers
      latch_->mutex_.lock();
      if (!latch_->version_.compare_exchange_strong(expected, new_version)) {
        latch_->mutex_.unlock();
        jumpmu::Jump();
      }
      version_ = new_version;
      state_ = GuardState::kPessimisticExclusive;
    } else {
      lock_exclusive();
    }
  }

  inline void ToSharedMayJump() {
    LS_DCHECK(state_ == GuardState::kOptimisticShared || state_ == GuardState::kPessimisticShared);
    if (state_ == GuardState::kPessimisticShared) {
      return;
    }
    if (state_ == GuardState::kOptimisticShared) {
      latch_->mutex_.lock_shared();
      if (latch_->version_.load() != version_) {
        latch_->mutex_.unlock_shared();
        jumpmu::Jump();
      }
      state_ = GuardState::kPessimisticShared;
    } else {
      UNREACHABLE();
    }
  }

  // For buffer management
  inline void TryToExclusiveMayJump() {
    LS_DCHECK(state_ == GuardState::kOptimisticShared);
    const uint64_t new_version = version_ + kLatchExclusiveBit;
    uint64_t expected = version_;

    if (!latch_->mutex_.try_lock()) {
      jumpmu::Jump();
    }
    if (!latch_->version_.compare_exchange_strong(expected, new_version)) {
      latch_->mutex_.unlock();
      jumpmu::Jump();
    }

    version_ = new_version;
    state_ = GuardState::kPessimisticExclusive;
  }

  inline void TryToSharedMayJump() {
    LS_DCHECK(state_ == GuardState::kOptimisticShared);
    if (!latch_->mutex_.try_lock_shared()) {
      jumpmu::Jump();
    }
    if (latch_->version_.load() != version_) {
      latch_->mutex_.unlock_shared();
      jumpmu::Jump();
    }
    state_ = GuardState::kPessimisticShared;
  }

private:
  inline void lock_exclusive() {
    latch_->mutex_.lock();
    LS_DCHECK(!HasExclusiveMark(latch_->version_));
    version_ = latch_->version_.load() + kLatchExclusiveBit;
    latch_->version_.store(version_, std::memory_order_release);
    state_ = GuardState::kPessimisticExclusive;
  }

  inline void unlock_exclusive() {
    LS_DCHECK(HasExclusiveMark(latch_->version_));
    version_ += kLatchExclusiveBit;
    latch_->version_.store(version_, std::memory_order_release);
    LS_DCHECK(!HasExclusiveMark(latch_->version_));
    latch_->mutex_.unlock();
    state_ = GuardState::kOptimisticShared;
  }

  inline void lock_shared() {
    latch_->mutex_.lock_shared();
    LS_DCHECK(!HasExclusiveMark(latch_->version_));
    version_ = latch_->version_.load();
    state_ = GuardState::kPessimisticShared;
  }

  inline void unlock_shared() {
    LS_DCHECK(!HasExclusiveMark(latch_->version_));
    latch_->mutex_.unlock_shared();
    state_ = GuardState::kOptimisticShared;
  }
};

} // namespace leanstore::storage
