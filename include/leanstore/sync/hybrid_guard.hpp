#pragma once

#include "hybrid_latch.hpp"
#include "leanstore/exceptions.hpp"
#include "leanstore/utils/jump_mu.hpp"
#include "leanstore/utils/log.hpp"

#include <atomic>

#include <unistd.h>

namespace leanstore::storage {

enum class GuardState : uint8_t {
  kUninitialized = 0,
  kSharedOptimistic = 1,
  kSharedPessimistic = 2,
  kExclusivePessimistic = 3,
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

  bool contented_ = false;

public:
  HybridGuard() = default;

  HybridGuard(HybridLatch* latch) : latch_(latch), state_(GuardState::kUninitialized), version_(0) {
  }

  // Manually construct a guard from a snapshot. Use with caution!
  HybridGuard(HybridLatch& latch, const uint64_t last_seen_version)
      : latch_(&latch),
        state_(GuardState::kSharedOptimistic),
        version_(last_seen_version),
        contented_(false) {
  }

  HybridGuard(HybridLatch& latch, GuardState state)
      : latch_(&latch),
        state_(state),
        version_(latch.GetVersion()) {
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
    contented_ = other.contented_;
    other.state_ = GuardState::kMoved;
    return *this;
  }

public:
  void JumpIfModifiedByOthers() {
    LS_DCHECK(state_ == GuardState::kSharedOptimistic || version_ == latch_->GetVersion());

    if (state_ == GuardState::kSharedOptimistic && version_ != latch_->GetVersion()) {
      LS_DLOG("JumpIfModifiedByOthers, version_(expected)={}, "
              "latch_->GetVersion()(actual)={}",
              version_, latch_->GetVersion());
      jumpmu::Jump();
    }
  }

  inline void Unlock() {
    switch (state_) {
    case GuardState::kExclusivePessimistic: {
      unlock_exclusive();
      break;
    }
    case GuardState::kSharedPessimistic: {
      unlock_shared();
      break;
    }
    default: {
      break;
    }
    }
    LS_DCHECK(state_ == GuardState::kMoved || state_ == GuardState::kUninitialized ||
              state_ == GuardState::kSharedOptimistic);
  }

  inline void ToOptimisticSpin() {
    LS_DCHECK(state_ == GuardState::kUninitialized && latch_ != nullptr);
    version_ = latch_->GetVersion();
    while (HasExclusiveMark(version_)) {
      contented_ = true;
      version_ = latch_->GetVersion();
    }
    state_ = GuardState::kSharedOptimistic;
  }

  inline void ToOptimisticOrJump() {
    LS_DCHECK(state_ == GuardState::kUninitialized && latch_ != nullptr);
    version_ = latch_->GetVersion();
    if (HasExclusiveMark(version_)) {
      contented_ = true;
      jumpmu::Jump();
    }
    state_ = GuardState::kSharedOptimistic;
  }

  inline void ToOptimisticOrShared() {
    if (state_ == GuardState::kSharedOptimistic || state_ == GuardState::kSharedPessimistic) {
      return;
    }
    LS_DCHECK(state_ == GuardState::kUninitialized || latch_ != nullptr);
    version_ = latch_->GetVersion();
    if (HasExclusiveMark(version_)) {
      lock_shared();
      contented_ = true;
    } else {
      state_ = GuardState::kSharedOptimistic;
    }
  }

  inline void ToOptimisticOrExclusive() {
    LS_DCHECK(state_ == GuardState::kUninitialized && latch_ != nullptr);
    version_ = latch_->GetVersion();
    if (HasExclusiveMark(version_)) {
      lock_exclusive();
      contented_ = true;
    } else {
      state_ = GuardState::kSharedOptimistic;
    }
  }

  inline void ToExclusiveMayJump() {
    LS_DCHECK(state_ != GuardState::kSharedPessimistic);
    if (state_ == GuardState::kExclusivePessimistic) {
      return;
    }

    if (state_ == GuardState::kSharedOptimistic) {
      const uint64_t new_version = version_ + kLatchExclusiveBit;
      uint64_t expected = version_;
      // changed from try_lock because of possible retries b/c lots of readers
      latch_->mutex_.lock();
      if (!latch_->version_.compare_exchange_strong(expected, new_version)) {
        latch_->mutex_.unlock();
        jumpmu::Jump();
      }
      version_ = new_version;
      state_ = GuardState::kExclusivePessimistic;
    } else {
      lock_exclusive();
    }
  }

  inline void ToSharedMayJump() {
    LS_DCHECK(state_ == GuardState::kSharedOptimistic || state_ == GuardState::kSharedPessimistic);
    if (state_ == GuardState::kSharedPessimistic) {
      return;
    }
    if (state_ == GuardState::kSharedOptimistic) {
      latch_->mutex_.lock_shared();
      if (latch_->GetVersion() != version_) {
        latch_->mutex_.unlock_shared();
        jumpmu::Jump();
      }
      state_ = GuardState::kSharedPessimistic;
    } else {
      UNREACHABLE();
    }
  }

  // For buffer management
  inline void TryToExclusiveMayJump() {
    LS_DCHECK(state_ == GuardState::kSharedOptimistic);
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
    state_ = GuardState::kExclusivePessimistic;
  }

  inline void TryToSharedMayJump() {
    LS_DCHECK(state_ == GuardState::kSharedOptimistic);
    if (!latch_->mutex_.try_lock_shared()) {
      jumpmu::Jump();
    }
    if (latch_->GetVersion() != version_) {
      latch_->mutex_.unlock_shared();
      jumpmu::Jump();
    }
    state_ = GuardState::kSharedPessimistic;
  }

private:
  // NOLINTBEGIN

  inline void lock_exclusive() {
    latch_->mutex_.lock();
    LS_DCHECK(!HasExclusiveMark(latch_->GetVersion()));
    version_ = latch_->GetVersion() + kLatchExclusiveBit;
    latch_->version_.store(version_, std::memory_order_release);
    state_ = GuardState::kExclusivePessimistic;
  }

  inline void unlock_exclusive() {
    LS_DCHECK(HasExclusiveMark(latch_->GetVersion()));
    version_ += kLatchExclusiveBit;
    latch_->version_.store(version_, std::memory_order_release);
    LS_DCHECK(!HasExclusiveMark(latch_->GetVersion()));
    latch_->mutex_.unlock();
    state_ = GuardState::kSharedOptimistic;
  }

  inline void lock_shared() {
    latch_->mutex_.lock_shared();
    LS_DCHECK(!HasExclusiveMark(latch_->GetVersion()));
    version_ = latch_->GetVersion();
    state_ = GuardState::kSharedPessimistic;
  }

  inline void unlock_shared() {
    LS_DCHECK(!HasExclusiveMark(latch_->GetVersion()));
    latch_->mutex_.unlock_shared();
    state_ = GuardState::kSharedOptimistic;
  }

  // NOLINTEND
};

} // namespace leanstore::storage
