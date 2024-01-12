#pragma once

#include "shared-headers/Units.hpp"

#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

namespace leanstore {
namespace utils {

template <typename T> class EnumerableThreadLocal {
public:
  inline static std::shared_mutex sMutex;

  inline static std::vector<std::unique_ptr<T>> sAllThreadLocals;

  inline static thread_local T* sThreadLocal;

public:
  inline static T* Local() {
    if (sThreadLocal == nullptr) {
      std::unique_lock<std::shared_mutex> guard(sMutex);
      sAllThreadLocals.push_back(std::make_unique<T>());
      sThreadLocal = sAllThreadLocals[sAllThreadLocals.size() - 1].get();
    }
    return sThreadLocal;
  }
};

template <class CountersT, class CounterT, typename SumT = u64>
inline static SumT Sum(EnumerableThreadLocal<CountersT>& counters,
                       CounterT CountersT::*c) {
  std::shared_lock<std::shared_mutex> guard(
      EnumerableThreadLocal<CountersT>::sMutex);
  SumT result = 0;
  for (auto& counter : counters.sAllThreadLocals) {
    result += ((*counter.get()).*c).exchange(0);
  }
  return result;
}

template <typename T>
inline static void ForEach(EnumerableThreadLocal<T>& tlsObjContainer,
                           std::function<void(T* tlsObj)> tlsObjCallBack) {
  std::shared_lock<std::shared_mutex> guard(EnumerableThreadLocal<T>::sMutex);
  for (auto& tlsObj : tlsObjContainer.sAllThreadLocals) {
    tlsObjCallBack(tlsObj.get());
  }
}

} // namespace utils
} // namespace leanstore