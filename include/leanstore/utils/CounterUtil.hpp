#pragma once

#include "leanstore-c/perf_counters.h"

#ifdef ENABLE_PERF_COUNTERS
#include <chrono>
#endif

namespace leanstore {

namespace cr {
extern thread_local PerfCounters tlsPerfCounters;
} // namespace cr

//! ScopedTimer for perf counters
class ScopedTimer {
private:
#ifdef ENABLE_PERF_COUNTERS
  //! Counter to cumulate the time elasped
  CounterType* mCounterToCum;

  //! Start timepoint
  std::chrono::steady_clock::time_point mStartedAt;
#endif

public:
  ScopedTimer(CounterType* counter [[maybe_unused]]) {
#ifdef ENABLE_PERF_COUNTERS
    mCounterToCum = counter;
    mStartedAt = std::chrono::steady_clock::now();
#endif
  }

  ~ScopedTimer() {
#ifdef ENABLE_PERF_COUNTERS
    auto stoppedAt = std::chrono::steady_clock::now();
    auto elaspedNs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(stoppedAt - mStartedAt).count();
    *mCounterToCum += elaspedNs;
#endif
  }
};

#ifdef ENABLE_PERF_COUNTERS

//------------------------------------------------------------------------------
// Macros when counters are enabled
//------------------------------------------------------------------------------

#define SCOPED_TIME_INTERNAL_INTERNAL(LINE) scopedTimerAtLine##LINE
#define SCOPED_TIME_INTERNAL(LINE) SCOPED_TIME_INTERNAL_INTERNAL(LINE)

//! Macro to create a ScopedTimer
#define COUNTER_TIMER_SCOPED(counter)                                                              \
  leanstore::ScopedTimer SCOPED_TIME_INTERNAL(__LINE__){counter};

//! Macro to inc a counter
#define COUNTER_INC(counter) atomic_fetch_add(counter, 1);

//! Macro to declare a block of code that will be executed only if counters are enabled
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