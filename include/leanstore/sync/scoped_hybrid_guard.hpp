#pragma once

#include "leanstore/base/jump_mu.hpp"
#include "leanstore/base/log.hpp"
#include "leanstore/sync/hybrid_mutex.hpp"

#include <atomic>
#include <functional>

#include <sys/mman.h>
#include <unistd.h>

namespace leanstore::test {

class ScopedHybridGuardTest;

} // namespace leanstore::test

namespace leanstore {

/// A scoped guard for the hybrid latch. It locks the latch in the specified
/// mode when constructed, and unlocks the latch when destructed.
/// The guard is movable but not copyable.
class ScopedHybridGuard {
private:
  /// The latch to guard.
  HybridMutex* latch_;

  /// The latch mode.
  LatchMode latch_mode_;

  /// The version of the latch when it was optimistically locked.
  uint64_t version_on_lock_;

  /// Whether the guard has encountered contention, checked when the latch is
  /// optimistically locked.
  bool contented_;

  /// Whether the guard has locked the latch.
  bool locked_;

public:
  /// Construct a guard for the latch, lock it immediately in the specified
  /// latch mode. It may jump if latchMode is kOptimisticOrJump and the latch is
  /// exclusive locked by others.
  ScopedHybridGuard(HybridMutex& latch, LatchMode latch_mode)
      : latch_(&latch),
        latch_mode_(latch_mode),
        version_on_lock_(0),
        contented_(false),
        locked_(false) {
    Lock();
  }

  /// Construct a guard for the latch, lock it in kOptimisticOrJump mode with
  /// the specified version. It may jump if the optimistic version does not
  /// match the current version.
  ScopedHybridGuard(HybridMutex& latch, uint64_t version)
      : latch_(&latch),
        latch_mode_(LatchMode::kOptimisticOrJump),
        version_on_lock_(version),
        contented_(false),
        locked_(false) {
    // jump if the optimistic lock is invalid
    jump_if_modified_by_others();
    locked_ = true;
  }

  /// Destruct the guard, unlock the latch if it is locked.
  ~ScopedHybridGuard() {
    Unlock();
  }

  /// No copy construct
  ScopedHybridGuard(const ScopedHybridGuard&) = delete;

  /// No copy assign
  ScopedHybridGuard& operator=(const ScopedHybridGuard& other) = delete;

  /// Move construct
  ScopedHybridGuard(ScopedHybridGuard&& other) noexcept {
    *this = std::move(other);
  }

  /// Move assign
  ScopedHybridGuard& operator=(ScopedHybridGuard&& other) noexcept {
    Unlock();

    latch_ = other.latch_;
    latch_mode_ = other.latch_mode_;
    version_on_lock_ = other.version_on_lock_;
    contented_ = other.contented_;
    locked_ = other.locked_;
    other.locked_ = false;
    return *this;
  }

  /// Lock the latch in the specified mode if it is not locked.
  void Lock();

  /// Unlock the latch if it is locked.
  void Unlock();

  static void GetOptimistic(HybridMutex& latch, LatchMode latch_mode, std::function<void()> copier);

  static void Get(HybridMutex& latch, std::function<void()> copier);

private:
  /// Lock the latch in kOptimisticOrJump mode.
  void lock_optimistic_or_jump(); // NOLINT (fixme)

  /// Lock the latch in kOptimisticSpin mode.
  void lock_optimistic_spin(); // NOLINT (fixme)

  /// Unlock the latch in kOptimisticOrJump or kOptimisticSpin mode.
  void unlock_optimistic_or_jump(); // NOLINT (fixme)

  /// Jump if the latch has been modified by others.
  void jump_if_modified_by_others(); // NOLINT (fixme)

  /// Lock the latch in kSharedPessimistic mode.
  void lock_pessimistic_shared(); // NOLINT (fixme)

  /// Unlock the latch in kSharedPessimistic mode.
  void unlock_pessimistic_shared(); // NOLINT (fixme)

  /// Lock the latch in kExclusivePessimistic mode.
  void lock_pessimistic_exclusive(); // NOLINT (fixme)

  /// Unlock the latch in kExclusivePessimistic mode.
  void unlock_pessimistic_exclusive(); // NOLINT (fixme)

  /// Allow the test class to access private members.
  friend class test::ScopedHybridGuardTest;
};

inline void ScopedHybridGuard::GetOptimistic(HybridMutex& latch, LatchMode latch_mode,
                                             std::function<void()> copier) {
  LEAN_DCHECK(latch_mode == LatchMode::kOptimisticOrJump ||
              latch_mode == LatchMode::kOptimisticSpin);
  while (true) {
    JUMPMU_TRY() {
      auto guard = ScopedHybridGuard(latch, latch_mode);
      copier();
    }
    JUMPMU_CATCH() {
      continue;
    }
    return;
  }
}

inline void ScopedHybridGuard::Get(HybridMutex& latch, std::function<void()> copier) {
  while (true) {
    JUMPMU_TRY() {
      auto guard = ScopedHybridGuard(latch, LatchMode::kOptimisticOrJump);
      copier();
      guard.Unlock();
      JUMPMU_RETURN;
    }
    JUMPMU_CATCH() {
      auto guard = ScopedHybridGuard(latch, LatchMode::kSharedPessimistic);
      copier();
      guard.Unlock();
      return;
    }
  }
}

inline void ScopedHybridGuard::Lock() {
  if (locked_) {
    return;
  }

  switch (latch_mode_) {
  case LatchMode::kOptimisticOrJump: {
    lock_optimistic_or_jump();
    break;
  }
  case LatchMode::kOptimisticSpin: {
    lock_optimistic_spin();
    break;
  }
  case LatchMode::kSharedPessimistic: {
    lock_pessimistic_shared();
    break;
  }
  case LatchMode::kExclusivePessimistic: {
    lock_pessimistic_exclusive();
    break;
  }
  default: {
    Log::Error("Unsupported latch mode: {}", (uint64_t)latch_mode_);
  }
  }
  locked_ = true;
}

inline void ScopedHybridGuard::Unlock() {
  if (!locked_) {
    return;
  }

  switch (latch_mode_) {
  case LatchMode::kOptimisticOrJump: {
    unlock_optimistic_or_jump();
    break;
  }
  case LatchMode::kOptimisticSpin: {
    unlock_optimistic_or_jump();
    break;
  }
  case LatchMode::kSharedPessimistic: {
    unlock_pessimistic_shared();
    break;
  }
  case LatchMode::kExclusivePessimistic: {
    unlock_pessimistic_exclusive();
    break;
  }
  default: {
    Log::Error("Unsupported latch mode: {}", (uint64_t)latch_mode_);
  }
  }

  locked_ = false;
}

inline void ScopedHybridGuard::lock_optimistic_or_jump() {
  LEAN_DCHECK(latch_mode_ == LatchMode::kOptimisticOrJump && latch_ != nullptr);
  version_on_lock_ = latch_->version_.load();
  if (HasExclusiveMark(version_on_lock_)) {
    contented_ = true;
    LEAN_DLOG("lockOptimisticOrJump() failed, target latch, latch={}, version={}", (void*)&latch_,
              version_on_lock_);
    JumpContext::Jump();
  }
}

inline void ScopedHybridGuard::lock_optimistic_spin() {
  LEAN_DCHECK(latch_mode_ == LatchMode::kOptimisticSpin && latch_ != nullptr);
  version_on_lock_ = latch_->version_.load();
  while (HasExclusiveMark(version_on_lock_)) {
    contented_ = true;
    version_on_lock_ = latch_->version_.load();
  }
}

inline void ScopedHybridGuard::unlock_optimistic_or_jump() {
  LEAN_DCHECK(
      (latch_mode_ == LatchMode::kOptimisticOrJump || latch_mode_ == LatchMode::kOptimisticSpin) &&
      latch_ != nullptr);
  jump_if_modified_by_others();
}

inline void ScopedHybridGuard::jump_if_modified_by_others() {
  auto cur_version = latch_->version_.load();
  if (version_on_lock_ != cur_version) {
    contented_ = true;
    LEAN_DLOG("jumpIfModifiedByOthers() failed, target latch, latch={}, "
              "version(expected)={}, version(actual)={}",
              (void*)&latch_, version_on_lock_, cur_version);
    JumpContext::Jump();
  }
}

inline void ScopedHybridGuard::lock_pessimistic_shared() {
  LEAN_DCHECK(latch_mode_ == LatchMode::kSharedPessimistic && latch_ != nullptr);
  latch_->shared_mutex_.lock_shared();
}

inline void ScopedHybridGuard::unlock_pessimistic_shared() {
  LEAN_DCHECK(latch_mode_ == LatchMode::kSharedPessimistic && latch_ != nullptr);
  latch_->shared_mutex_.unlock_shared();
}

inline void ScopedHybridGuard::lock_pessimistic_exclusive() {
  LEAN_DCHECK(latch_mode_ == LatchMode::kExclusivePessimistic && latch_ != nullptr);
  latch_->LockExclusively();
}

inline void ScopedHybridGuard::unlock_pessimistic_exclusive() {
  LEAN_DCHECK(latch_mode_ == LatchMode::kExclusivePessimistic && latch_ != nullptr);
  latch_->UnlockExclusively();
}

} // namespace leanstore