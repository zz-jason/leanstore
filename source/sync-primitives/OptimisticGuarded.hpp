#pragma once

#include "shared-headers/Units.hpp"

#include <atomic>
#include <type_traits>

namespace leanstore {
namespace storage {

/// Optimized for single-writer-single-reader scenarios. The reader can read the
/// value without locking which is useful for performance-critical code. The
/// value must be trivially copyable.
template <typename T>
  requires std::is_trivially_copy_assignable_v<T>
class OptimisticGuarded {
private:
  /// Used for optimistic locking. The lowest 1 bit is used to indicate whether
  /// the value is being modified. The version is increased by 2 when the value
  /// is modified, which can be used to check whether the value is modified
  /// since the last read.
  std::atomic<u64> mVersion;

  /// The guarded value.
  T mValue = 0;

public:
  /// Constructor.
  OptimisticGuarded() = default;

  /// Constructor.
  OptimisticGuarded(const T& value) : mVersion(0), mValue(value) {
  }

  /// Copies the value and returns the version of the value. The version is
  /// guaranteed to be even.
  /// @param copiedVal The copied value.
  /// @return The version of the value.
  [[nodiscard]] u64 Get(T& copiedVal);

  /// Stores the given value. Only one thread can call this function at a time.
  /// @param newVal The value to store.
  void Set(const T& newVal);

  /// Updates the given attribute of the value. Only one thread can call this
  /// function at a time.
  template <typename Ta> void UpdateAttribute(Ta T::*a, const Ta& newVal);
};

template <typename T>
  requires std::is_trivially_copy_assignable_v<T>
inline u64 OptimisticGuarded<T>::Get(T& copiedVal) {
  while (true) {
    auto version = mVersion.load();
    while (version & 1) {
      version = mVersion.load();
    }
    copiedVal = mValue;
    if (version == mVersion.load()) {
      return version;
    }
  }
}

template <typename T>
  requires std::is_trivially_copy_assignable_v<T>
inline void OptimisticGuarded<T>::Set(const T& newVal) {
  mVersion.store(mVersion + 1, std::memory_order_release);
  mValue = newVal;
  mVersion.store(mVersion + 1, std::memory_order_release);
}

template <typename T>
  requires std::is_trivially_copy_assignable_v<T>
template <typename Ta>
inline void OptimisticGuarded<T>::UpdateAttribute(Ta T::*a, const Ta& newVal) {
  mVersion.store(mVersion + 1, std::memory_order_release);
  mValue.*a = newVal;
  mVersion.store(mVersion + 1, std::memory_order_release);
}

} // namespace storage
} // namespace leanstore