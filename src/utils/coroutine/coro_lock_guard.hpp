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

  CoroHybridLockGuard() = default;

  CoroHybridLockGuard(CoroHybridMutex* hybrid_mutex, uint64_t version_on_lock = kVersionUnlocked)
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
  CoroHybridLockGuard(CoroHybridLockGuard&& other) {
    *this = std::move(other);
  }

  // move assignment
  CoroHybridLockGuard& operator=(CoroHybridLockGuard&& other) {
    if (this != &other) {
      hybrid_mutex_ = other.hybrid_mutex_;
      version_on_lock_ = other.version_on_lock_;
      mutex_state_ = other.mutex_state_;
      contented_ = other.contented_;

      other.hybrid_mutex_ = nullptr;
      other.version_on_lock_ = kVersionUnlocked;
      other.mutex_state_ = State::kMoved;
      other.contented_ = false;
    }
    return *this;
  }

  void JumpIfModifiedByOthers() {
    auto current_version = hybrid_mutex_->GetVersion();
    if (mutex_state_ == State::kSharedOptimistic && version_on_lock_ != current_version) {
      LS_DLOG("Jump, version mismatch, version_on_lock_(expected)={}, current_version(actual)={}",
              version_on_lock_, current_version);
      jumpmu::Jump();
    }
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

  void ToSharedOptimistic() {

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

  CoroHybridMutex* hybrid_mutex_ = nullptr;

  uint64_t version_on_lock_ = kVersionUnlocked;

  State mutex_state_ = State::kUninitialized;

  bool contented_ = false;
};

} // namespace leanstore