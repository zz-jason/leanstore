#pragma once

#include "coro_mutex.hpp"
#include "leanstore/utils/jump_mu.hpp"
#include "leanstore/utils/log.hpp"
#include "utils/coroutine/coro_mutex.hpp"

#include <cassert>
#include <cstdint>

namespace leanstore {

class CoroHybridLockGuard {
public:
  constexpr static uint64_t kVersionUnlocked = 0ull;
  constexpr static uint64_t kLatchExclusiveBit = 1ull;

  enum class State : uint8_t {
    kUninitialized = 0,
    kExclusivePessimistic,
    kSharedOptimistic,
    kSharedPessimistic,
    kMoved,
  };

  CoroHybridLockGuard(CoroHybridMutex* hybrid_mutex = nullptr,
                      uint64_t version_on_lock = kVersionUnlocked)
      : hybrid_mutex_(hybrid_mutex),
        version_on_lock_(version_on_lock),
        mutex_state_(version_on_lock == kVersionUnlocked ? State::kUninitialized
                                                         : State::kSharedOptimistic) {
  }

  CoroHybridLockGuard(CoroHybridMutex* hybrid_mutex, State state)
      : hybrid_mutex_(hybrid_mutex),
        version_on_lock_(state == State::kUninitialized ? kVersionUnlocked
                                                        : hybrid_mutex->GetVersion()),
        mutex_state_(state) {
  }

  // move constructor
  CoroHybridLockGuard(CoroHybridLockGuard&& other)
      : hybrid_mutex_(other.hybrid_mutex_),
        version_on_lock_(other.version_on_lock_),
        mutex_state_(other.mutex_state_),
        contented_(other.contented_) {
    other.hybrid_mutex_ = nullptr;
    other.version_on_lock_ = kVersionUnlocked;
    other.mutex_state_ = State::kMoved;
    other.contented_ = false;
  }

  // move assignment
  CoroHybridLockGuard& operator=(CoroHybridLockGuard&& other) {
    if (this != &other) {
      this->Unlock();

      this->hybrid_mutex_ = other.hybrid_mutex_;
      this->version_on_lock_ = other.version_on_lock_;
      this->mutex_state_ = other.mutex_state_;
      this->contented_ = other.contented_;

      other.hybrid_mutex_ = nullptr;
      other.version_on_lock_ = kVersionUnlocked;
      other.mutex_state_ = State::kMoved;
      other.contented_ = false;
    }

    return *this;
  }

  bool IsConflicted() {
    return (mutex_state_ == State::kSharedOptimistic &&
            version_on_lock_ != hybrid_mutex_->GetVersion());
  }

  void LockSharedOptimistic() {
    version_on_lock_ = hybrid_mutex_->LockSharedOptimistic();
    mutex_state_ = State::kSharedOptimistic;
  }

  void LockSharedPessimistic() {
    assert(false && "LockSharedPessimistic is not implemented");
  }

  void LockExclusivePessimistic() {
    assert(false && "LockExclusivePessimistic is not implemented");
  }

  void Unlock() {
    if (mutex_state_ == State::kExclusivePessimistic) {
      UnlockExclusivePessimistic();
    } else if (mutex_state_ == State::kSharedPessimistic) {
      UnlockSharedPessimistic();
    }

    assert(mutex_state_ == State::kMoved || mutex_state_ == State::kUninitialized ||
           mutex_state_ == State::kSharedOptimistic);
    return;
  }

private:
  /// Locks the mutex in exclusive mode.
  void LockExclusive() {
    version_on_lock_ = hybrid_mutex_->Lock();
    mutex_state_ = State::kExclusivePessimistic;
  }

  /// Unlocks the mutex in exclusive mode.
  /// Degrades the mutex from kExclusivePessimistic to kSharedOptimistic state.
  void UnlockExclusivePessimistic() {
    assert(mutex_state_ == State::kExclusivePessimistic);
    version_on_lock_ = hybrid_mutex_->Unlock(version_on_lock_);
    mutex_state_ = State::kSharedOptimistic;
  }

  /// Locks the mutex in shared mode.
  void LockShared() {
    version_on_lock_ = hybrid_mutex_->LockShared();
    mutex_state_ = State::kSharedPessimistic;
  }

  /// Unlocks the mutex in shared mode.
  /// Degrades the mutex from kSharedPessimistic to kSharedOptimistic state.
  void UnlockSharedPessimistic() {
    assert(mutex_state_ == State::kSharedPessimistic);
    hybrid_mutex_->UnlockShared();
    mutex_state_ = State::kSharedOptimistic;
  }

private:
  /// The hybrid mutex that this guard is managing. It can be null if the guard
  /// is uninitialized.
  CoroHybridMutex* hybrid_mutex_ = nullptr;

  /// The version of the mutex when it was locked.
  uint64_t version_on_lock_ = kVersionUnlocked;

  /// The state of the mutex.
  State mutex_state_ = State::kUninitialized;

  bool contented_ = false;
};

static_assert(sizeof(CoroHybridLockGuard) == 24);

} // namespace leanstore