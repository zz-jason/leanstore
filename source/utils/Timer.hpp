#pragma once

#include <chrono>
#include <functional>

namespace leanstore {
namespace utils {

class SteadyTimer {
public:
  typedef std::chrono::steady_clock::time_point TimePoint;

public:
  std::chrono::steady_clock::time_point mStartedAt;
  std::chrono::steady_clock::time_point mStoppedAt;

public:
  SteadyTimer() : mStartedAt(std::chrono::steady_clock::now()) {
  }

  ~SteadyTimer() = default;

  void Start() {
    mStartedAt = std::chrono::steady_clock::now();
  }

  void Stop() {
    mStoppedAt = std::chrono::steady_clock::now();
  }

  uint64_t ElaspedNS() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(mStoppedAt -
                                                                mStartedAt)
        .count();
  }

  uint64_t ElaspedUS() {
    return std::chrono::duration_cast<std::chrono::microseconds>(mStoppedAt -
                                                                 mStartedAt)
        .count();
  }

  uint64_t ElaspedMS() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(mStoppedAt -
                                                                 mStartedAt)
        .count();
  }

  uint64_t ElaspedSec() {
    return std::chrono::duration_cast<std::chrono::seconds>(mStoppedAt -
                                                            mStartedAt)
        .count();
  }
};

class ScopedTimer {
public:
  typedef std::chrono::steady_clock::time_point TimePoint;

public:
  std::chrono::steady_clock::time_point mStartedAt;

  std::function<void(TimePoint, TimePoint)> mActionOnExit;

public:
  ScopedTimer(std::function<void(TimePoint, TimePoint)> action = nullptr)
      : mStartedAt(std::chrono::steady_clock::now()), mActionOnExit(action) {
  }

  ~ScopedTimer() {
    auto endedAt = std::chrono::steady_clock::now();
    if (mActionOnExit != nullptr) {
      mActionOnExit(mStartedAt, endedAt);
    }
  }

  uint64_t ElaspedNS() {
    auto current = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(current -
                                                                mStartedAt)
        .count();
  }

  uint64_t ElaspedUS() {
    auto current = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(current -
                                                                 mStartedAt)
        .count();
  }

  uint64_t ElaspedMS() {
    auto current = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(current -
                                                                 mStartedAt)
        .count();
  }

  uint64_t ElaspedSec() {
    auto current = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::seconds>(current -
                                                            mStartedAt)
        .count();
  }
};

} // namespace utils
} // namespace leanstore
