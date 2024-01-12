#pragma once

#include "shared-headers/Units.hpp"

#include <tbb/enumerable_thread_specific.h>

namespace leanstore {
namespace utils {
namespace threadlocal {

template <class CountersClass, class CounterType, typename T = u64>
T Sum(tbb::enumerable_thread_specific<CountersClass>& counters,
      CounterType CountersClass::*c) {
  T result = 0;
  for (auto i = counters.begin(); i != counters.end(); ++i) {
    result += ((*i).*c).exchange(0);
  }
  return result;
}

template <class CountersClass, class CounterType, typename T = u64>
T Sum(tbb::enumerable_thread_specific<CountersClass>& counters,
      CounterType CountersClass::*c, u64 index) {
  T result = 0;
  for (auto i = counters.begin(); i != counters.end(); ++i) {
    result += ((*i).*c)[index].exchange(0);
  }
  return result;
}

template <class CountersClass, class CounterType, typename T = u64>
T Sum(tbb::enumerable_thread_specific<CountersClass>& counters,
      CounterType CountersClass::*c, u64 row, u64 col) {
  T result = 0;
  for (auto i = counters.begin(); i != counters.end(); ++i) {
    result += ((*i).*c)[row][col].exchange(0);
  }
  return result;
}

template <class CountersClass, class CounterType, typename T = u64>
T max(tbb::enumerable_thread_specific<CountersClass>& counters,
      CounterType CountersClass::*c, u64 row) {
  T result = 0;
  for (auto i = counters.begin(); i != counters.end(); ++i) {
    result = std::max<T>(((*i).*c)[row].exchange(0), result);
  }
  return result;
}

} // namespace threadlocal
} // namespace utils
} // namespace leanstore
