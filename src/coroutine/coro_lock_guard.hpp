#pragma once

#include "coro_mutex.hpp"
#include "coroutine/coro_mutex.hpp"

#include <cassert>
#include <cstdint>

namespace leanstore {

template <typename T>
class CoroUniqueLock {
public:
  explicit CoroUniqueLock(T& coro_mutex) : coro_mutex_(coro_mutex) {
    lock();
  }

  ~CoroUniqueLock() {
    if (owns_lock_) {
      unlock();
    }
  }

  // no copy && assignment
  CoroUniqueLock(const CoroUniqueLock&) = delete;
  CoroUniqueLock& operator=(const CoroUniqueLock&) = delete;

  /// NOLINTNEXTLINE
  void lock() {
    coro_mutex_.lock();
    owns_lock_ = true;
  }

  /// NOLINTNEXTLINE
  void unlock() {
    coro_mutex_.unlock();
    owns_lock_ = false;
  }

  /// NOLINTNEXTLINE
  bool owns_lock() const {
    return owns_lock_;
  }

private:
  T& coro_mutex_;
  bool owns_lock_;
};

template <typename T>
class CoroSharedLock {
public:
  explicit CoroSharedLock(T& coro_mutex) : coro_mutex_(coro_mutex) {
    coro_mutex_.lock_shared();
  }

  ~CoroSharedLock() {
    coro_mutex_.unlock_shared();
  }

  // no copy && assignment
  CoroSharedLock(const CoroSharedLock&) = delete;
  CoroSharedLock& operator=(const CoroSharedLock&) = delete;

private:
  T& coro_mutex_;
};

class CoroHybridLock {
public:
  static constexpr uint64_t kVersionUnlocked = 0ull;
  static constexpr uint64_t kLatchExclusiveBit = 1ull;

  enum class State : uint8_t {
    kUninitialized = 0,
    kExclusivePessimistic,
    kSharedOptimistic,
    kSharedPessimistic,
    kMoved,
  };

  explicit CoroHybridLock(CoroHybridMutex* hybrid_mutex = nullptr,
                          uint64_t version_on_lock = kVersionUnlocked)
      : hybrid_mutex_(hybrid_mutex),
        version_on_lock_(version_on_lock),
        mutex_state_(version_on_lock == kVersionUnlocked ? State::kUninitialized
                                                         : State::kSharedOptimistic) {
  }

  CoroHybridLock(CoroHybridMutex* hybrid_mutex, State state)
      : hybrid_mutex_(hybrid_mutex),
        version_on_lock_(state == State::kUninitialized ? kVersionUnlocked
                                                        : hybrid_mutex->GetVersion()),
        mutex_state_(state) {
  }

  // move constructor
  CoroHybridLock(CoroHybridLock&& other) noexcept
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
  CoroHybridLock& operator=(CoroHybridLock&& other) noexcept {
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

  /// The hybrid mutex that this guard is managing. It can be null if the guard
  /// is uninitialized.
  CoroHybridMutex* hybrid_mutex_ = nullptr;

  /// The version of the mutex when it was locked.
  uint64_t version_on_lock_ = kVersionUnlocked;

  /// The state of the mutex.
  State mutex_state_ = State::kUninitialized;

  bool contented_ = false;
};

static_assert(sizeof(CoroHybridLock) == 24);

} // namespace leanstore