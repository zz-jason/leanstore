#include "Units.hpp"

#include <functional>
#include <list>
#include <memory>
#include <vector>

namespace leanstore {
namespace utils {

/**
 * Makes sense for single-writer multiple-readers pattern for short write
 */
template <typename T> class OptimisticSpinStruct {
public:
  T mValue;
  std::atomic<u64> mOptimisticLatch;

public:
  T getSync() {
  retry : {
    u64 version = mOptimisticLatch.load();
    while (version & LSB) {
      version = mOptimisticLatch.load();
    }
    T copy = mValue;
    if (version != mOptimisticLatch.load()) {
      goto retry;
    }
    copy.mVersion = version;
    return copy;
  }
  }

  // Only writer should call this
  T getNoSync() {
    return mValue;
  }

  void SetSync(const T& newValue) {
    mOptimisticLatch.store(mOptimisticLatch.load() + LSB,
                           std::memory_order_release);
    mValue = newValue;
    mOptimisticLatch.store(mOptimisticLatch.load() + LSB,
                           std::memory_order_release);
  }

  template <class AttributeType>
  void updateAttribute(AttributeType T::*a, const AttributeType& newValue) {
    mOptimisticLatch.store(mOptimisticLatch.load() + LSB,
                           std::memory_order_release);
    mValue.*a = newValue;
    mOptimisticLatch.store(mOptimisticLatch.load() + LSB,
                           std::memory_order_release);
  }

  void wait(T& copy) {
    mOptimisticLatch.wait(copy.mVersion);
  }
};

} // namespace utils
} // namespace leanstore