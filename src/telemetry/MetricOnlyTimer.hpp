#pragma once

#include "leanstore/Exceptions.hpp"

#include <chrono>

namespace leanstore::telemetry {

class MetricOnlyTimer {
public:
  typedef std::chrono::steady_clock::time_point TimePoint;

public:
  MetricOnlyTimer() {
    COUNTERS_BLOCK() {
      mStartedAt = std::chrono::steady_clock::now();
    }
  }

  ~MetricOnlyTimer() = default;

  double ElaspedUs() {
    double elaspedUs{0};
    COUNTERS_BLOCK() {
      auto stoppedAt = std::chrono::steady_clock::now();
      auto elaspedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           stoppedAt - mStartedAt)
                           .count();
      elaspedUs = elaspedNs / 1000.0;
    }
    return elaspedUs;
  }

private:
  std::chrono::steady_clock::time_point mStartedAt;
};

} // namespace leanstore::telemetry
