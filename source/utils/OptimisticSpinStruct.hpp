#include "Units.hpp"

#include <atomic>
#include <functional>
#include <list>
#include <memory>
#include <type_traits>
#include <vector>

namespace leanstore {
namespace utils {

/**
 * Makes sense for single-writer multiple-readers pattern for short write
 */
template <typename T>
  requires std::is_trivially_copy_assignable_v<T>
class OptimisticSpinStruct {
public:
  T mValue;
  std::atomic<u64> mOptimisticLatch;

public:
  T getSync() {
    while (true) {
      u64 version = mOptimisticLatch.load();
      while (version & LSB) {
        version = mOptimisticLatch.load();
      }
      T copy = mValue;
      if (version != mOptimisticLatch.load()) {
        continue;
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
    auto newVersion = mValue.mVersion + 1;
    mValue = newValue;
    mValue.mVersion = newVersion;
    mOptimisticLatch.store(mOptimisticLatch.load() + LSB,
                           std::memory_order_release);
  }

  template <class AttributeType>
  void updateAttribute(AttributeType T::*a, const AttributeType& newValue) {
    mOptimisticLatch.store(mOptimisticLatch.load() + LSB,
                           std::memory_order_release);
    auto newVersion = mValue.mVersion + 1;
    mValue.*a = newValue;
    mValue.mVersion = newVersion;
    mOptimisticLatch.store(mOptimisticLatch.load() + LSB,
                           std::memory_order_release);
  }
};

} // namespace utils
} // namespace leanstore
