#pragma once

#include "shared-headers/Units.hpp"

#include <atomic>
#include <type_traits>

namespace leanstore {
namespace storage {

/// Should only be used for single-writer-multi-reader scenarios
template <typename T>
  requires std::is_trivially_copy_assignable_v<T>
class OptimisticGuarded {
private:
  std::atomic<u64> mVersion;
  T mValue;

public:
  OptimisticGuarded(const T& value) : mVersion(0), mValue(value) {
  }

  u64 Get(T& copiedValue);

  void Set(const T& newValue);

  template <typename AttributeType>
  void UpdateAttribute(AttributeType T::*a, const AttributeType& newValue);
};

template <typename T>
  requires std::is_trivially_copy_assignable_v<T>
inline u64 OptimisticGuarded<T>::Get(T& copiedValue) {
  while (true) {
    auto version = mVersion.load();
    while (version & 1) {
      version = mVersion.load();
    }
    copiedValue = mValue;
    if (version == mVersion.load()) {
      return version;
    }
  }
}

template <typename T>
  requires std::is_trivially_copy_assignable_v<T>
inline void OptimisticGuarded<T>::Set(const T& newValue) {
  mVersion.store(mVersion + 1, std::memory_order_release);
  mValue = newValue;
  mVersion.store(mVersion + 1, std::memory_order_release);
}

template <typename T>
  requires std::is_trivially_copy_assignable_v<T>
template <typename AttributeType>
void OptimisticGuarded<T>::UpdateAttribute(AttributeType T::*a,
                                           const AttributeType& newValue) {
  mVersion.store(mVersion + 1, std::memory_order_release);
  mValue.*a = newValue;
  mVersion.store(mVersion + 1, std::memory_order_release);
}

} // namespace storage
} // namespace leanstore