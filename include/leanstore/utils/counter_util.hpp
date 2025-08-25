#pragma once

#include "leanstore-c/perf_counters.h"

#ifdef ENABLE_PERF_COUNTERS
#include <chrono>
#endif

namespace leanstore {

namespace cr {
inline thread_local PerfCounters tls_perf_counters;
} // namespace cr

/// ScopedCounterTimer for perf counters
class ScopedCounterTimer {
private:
#ifdef ENABLE_PERF_COUNTERS
  /// Counter to cumulate the time elasped
  CounterType* counter_to_cum_;

  /// Start timepoint
  std::chrono::steady_clock::time_point started_at_;
#endif

public:
  ScopedCounterTimer(CounterType* counter [[maybe_unused]]) {
#ifdef ENABLE_PERF_COUNTERS
    counter_to_cum_ = counter;
    started_at_ = std::chrono::steady_clock::now();
#endif
  }

  ~ScopedCounterTimer() {
#ifdef ENABLE_PERF_COUNTERS
    auto stopped_at = std::chrono::steady_clock::now();
    auto elasped_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(stopped_at - started_at_).count();
    *counter_to_cum_ += elasped_ns;
#endif
  }
};

#ifdef ENABLE_PERF_COUNTERS

//------------------------------------------------------------------------------
// Macros when counters are enabled
//------------------------------------------------------------------------------

#define SCOPED_TIME_INTERNAL_INTERNAL(LINE) scoped_timer_at_line##LINE
#define SCOPED_TIME_INTERNAL(LINE) SCOPED_TIME_INTERNAL_INTERNAL(LINE)

/// Macro to create a ScopedCounterTimer
#define COUNTER_TIMER_SCOPED(counter)                                                              \
  leanstore::ScopedCounterTimer SCOPED_TIME_INTERNAL(__LINE__){counter};

/// Macro to inc a counter
#define COUNTER_INC(counter) atomic_fetch_add(counter, 1);

/// Macro to declare a block of code that will be executed only if counters are enabled
#define COUNTERS_BLOCK() if constexpr (true)

#else

//------------------------------------------------------------------------------
// Macros when counters are disabled
//------------------------------------------------------------------------------

#define COUNTER_TIMER_SCOPED(counter)
#define COUNTER_INC(counter)
#define COUNTERS_BLOCK() if constexpr (false)

#endif

} // namespace leanstore