#pragma once

#include "coroutine/coro_env.hpp"
#include "leanstore/cpp/base/jump_mu.hpp"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/exceptions.hpp"
#include "leanstore/sync/hybrid_mutex.hpp"

#include <atomic>

#include <unistd.h>

namespace leanstore {

enum class GuardState : uint8_t {
  kUninitialized = 0,
  kSharedOptimistic = 1,
  kSharedPessimistic = 2,
  kExclusivePessimistic = 3,
  kMoved = 4,
};

/// Like std::unique_lock, std::shared_lock, std::lock_guard, this HybridGuard
/// is used together with HybridMutex to provide various lock mode.
///
/// TODO(jian.z): should we unlock the guard when it's destroied?
class HybridGuard {
public:
  HybridMutex* latch_ = nullptr;

  GuardState state_ = GuardState::kUninitialized;

  uint64_t version_;

  bool contented_ = false;

  HybridGuard() = default;

  explicit HybridGuard(HybridMutex* latch)
      : latch_(latch),
        state_(GuardState::kUninitialized),
        version_(0) {
  }

  // Manually construct a guard from a snapshot. Use with caution!
  HybridGuard(HybridMutex& latch, const uint64_t last_seen_version)
      : latch_(&latch),
        state_(GuardState::kSharedOptimistic),
        version_(last_seen_version),
        contented_(false) {
  }

  HybridGuard(HybridMutex& latch, GuardState state)
      : latch_(&latch),
        state_(state),
        version_(latch.GetVersion()) {
  }

  // Move constructor
  HybridGuard(HybridGuard&& other) noexcept {
    *this = std::move(other);
  }

  // Move assignment
  HybridGuard& operator=(HybridGuard&& other) noexcept {
    Unlock();

    latch_ = other.latch_;
    state_ = other.state_;
    version_ = other.version_;
    contented_ = other.contented_;
    other.state_ = GuardState::kMoved;
    return *this;
  }

  void JumpIfModifiedByOthers() {
    LEAN_DCHECK(state_ == GuardState::kSharedOptimistic || version_ == latch_->GetVersion());

    if (state_ == GuardState::kSharedOptimistic && version_ != latch_->GetVersion()) {
      LEAN_DLOG("JumpIfModifiedByOthers, version_(expected)={}, "
                "latch_->GetVersion()(actual)={}",
                version_, latch_->GetVersion());
      JumpContext::Jump();
    }
  }

  void Unlock() {
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
    LEAN_DCHECK(state_ == GuardState::kMoved || state_ == GuardState::kUninitialized ||
                state_ == GuardState::kSharedOptimistic);
  }

  bool TryLockOptimistic() {
    version_ = latch_->GetVersion();
    if (HasExclusiveMark(version_)) {
      contented_ = true;
      return false;
    }
    return true;
  }

  void ToOptimisticSpin() {
    LEAN_DCHECK(state_ == GuardState::kUninitialized && latch_ != nullptr);
    while (!TryLockOptimistic()) {
#ifdef ENABLE_COROUTINE
      CoroEnv::CurCoro()->SetTryLockFunc([this]() { return TryLockOptimistic(); });
      CoroEnv::CurCoro()->Yield(CoroState::kWaitingMutex);
#endif
    }
    state_ = GuardState::kSharedOptimistic;
  }

  void ToOptimisticOrJump() {
    LEAN_DCHECK(state_ == GuardState::kUninitialized && latch_ != nullptr);
    version_ = latch_->GetVersion();
    if (HasExclusiveMark(version_)) {
      contented_ = true;
      JumpContext::Jump();
    }
    state_ = GuardState::kSharedOptimistic;
  }

  void ToOptimisticOrShared() {
    if (state_ == GuardState::kSharedOptimistic || state_ == GuardState::kSharedPessimistic) {
      return;
    }
    LEAN_DCHECK(state_ == GuardState::kUninitialized || latch_ != nullptr);
    version_ = latch_->GetVersion();
    if (HasExclusiveMark(version_)) {
      lock_shared();
      contented_ = true;
    } else {
      state_ = GuardState::kSharedOptimistic;
    }
  }

  void ToOptimisticOrExclusive() {
    LEAN_DCHECK(state_ == GuardState::kUninitialized && latch_ != nullptr);
    version_ = latch_->GetVersion();
    if (HasExclusiveMark(version_)) {
      lock_exclusive();
      contented_ = true;
    } else {
      state_ = GuardState::kSharedOptimistic;
    }
  }

  void ToExclusiveMayJump() {
    LEAN_DCHECK(state_ != GuardState::kSharedPessimistic);
    if (state_ == GuardState::kExclusivePessimistic) {
      return;
    }

    if (state_ == GuardState::kSharedOptimistic) {
      const uint64_t new_version = version_ + kLatchExclusiveBit;
      uint64_t expected = version_;
      // changed from try_lock because of possible retries b/c lots of readers
      latch_->shared_mutex_.lock();
      if (!latch_->version_.compare_exchange_strong(expected, new_version)) {
        latch_->shared_mutex_.unlock();
        JumpContext::Jump();
      }
      version_ = new_version;
      state_ = GuardState::kExclusivePessimistic;
    } else {
      lock_exclusive();
    }
  }

  void ToSharedMayJump() {
    LEAN_DCHECK(state_ == GuardState::kSharedOptimistic ||
                state_ == GuardState::kSharedPessimistic);
    if (state_ == GuardState::kSharedPessimistic) {
      return;
    }
    if (state_ == GuardState::kSharedOptimistic) {
      latch_->shared_mutex_.lock_shared();
      if (latch_->GetVersion() != version_) {
        latch_->shared_mutex_.unlock_shared();
        JumpContext::Jump();
      }
      state_ = GuardState::kSharedPessimistic;
    } else {
      UNREACHABLE();
    }
  }

  // For buffer management
  void TryToExclusiveMayJump() {
    LEAN_DCHECK(state_ == GuardState::kSharedOptimistic);
    const uint64_t new_version = version_ + kLatchExclusiveBit;
    uint64_t expected = version_;

    if (!latch_->shared_mutex_.try_lock()) {
      JumpContext::Jump();
    }

    if (!latch_->version_.compare_exchange_strong(expected, new_version)) {
      latch_->shared_mutex_.unlock();
      JumpContext::Jump();
    }

    version_ = new_version;
    state_ = GuardState::kExclusivePessimistic;
  }

  void TryToSharedMayJump() {
    LEAN_DCHECK(state_ == GuardState::kSharedOptimistic);
    if (!latch_->shared_mutex_.try_lock_shared()) {
      JumpContext::Jump();
    }
    if (latch_->GetVersion() != version_) {
      latch_->shared_mutex_.unlock_shared();
      JumpContext::Jump();
    }
    state_ = GuardState::kSharedPessimistic;
  }

private:
  // NOLINTBEGIN

  void lock_exclusive() {
    latch_->shared_mutex_.lock();
    LEAN_DCHECK(!HasExclusiveMark(latch_->GetVersion()));
    version_ = latch_->GetVersion() + kLatchExclusiveBit;
    latch_->version_.store(version_, std::memory_order_release);
    state_ = GuardState::kExclusivePessimistic;
  }

  void unlock_exclusive() {
    LEAN_DCHECK(HasExclusiveMark(latch_->GetVersion()));
    version_ += kLatchExclusiveBit;
    latch_->version_.store(version_, std::memory_order_release);
    LEAN_DCHECK(!HasExclusiveMark(latch_->GetVersion()));
    latch_->shared_mutex_.unlock();
    state_ = GuardState::kSharedOptimistic;
  }

  void lock_shared() {
    latch_->shared_mutex_.lock_shared();
    LEAN_DCHECK(!HasExclusiveMark(latch_->GetVersion()));
    version_ = latch_->GetVersion();
    state_ = GuardState::kSharedPessimistic;
  }

  void unlock_shared() {
    LEAN_DCHECK(!HasExclusiveMark(latch_->GetVersion()));
    latch_->shared_mutex_.unlock_shared();
    state_ = GuardState::kSharedOptimistic;
  }

  // NOLINTEND
};

} // namespace leanstore
