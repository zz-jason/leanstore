#pragma once

#include <atomic>
#include <type_traits>

namespace leanstore {

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
  std::atomic<uint64_t> version_ = 0;

  /// The guarded value.
  T value_;

public:
  /// Constructor.
  OptimisticGuarded() = default;

  /// Constructor.
  OptimisticGuarded(const T& value) : version_(0), value_(value) {
  }

  /// Copies the value and returns the version of the value. The version is
  /// guaranteed to be even.
  /// @param copiedVal The copied value.
  /// @return The version of the value.
  [[nodiscard]] uint64_t Get(T& copied_val);

  /// Stores the given value. Only one thread can call this function at a time.
  /// @param newVal The value to store.
  void Set(const T& new_val);

  /// Updates the given attribute of the value. Only one thread can call this
  /// function at a time.
  template <typename Ta>
  void UpdateAttribute(Ta T::* a, const Ta& new_val);
};

template <typename T>
  requires std::is_trivially_copy_assignable_v<T>
inline uint64_t OptimisticGuarded<T>::Get(T& copied_val) {
  while (true) {
    auto version = version_.load();
    while (version & 1) {
      version = version_.load();
    }
    copied_val = value_;
    if (version == version_.load()) {
      return version;
    }
  }
}

template <typename T>
  requires std::is_trivially_copy_assignable_v<T>
inline void OptimisticGuarded<T>::Set(const T& new_val) {
  version_.store(version_ + 1, std::memory_order_release);
  value_ = new_val;
  version_.store(version_ + 1, std::memory_order_release);
}

template <typename T>
  requires std::is_trivially_copy_assignable_v<T>
template <typename Ta>
inline void OptimisticGuarded<T>::UpdateAttribute(Ta T::* a, const Ta& new_val) {
  version_.store(version_ + 1, std::memory_order_release);
  value_.*a = new_val;
  version_.store(version_ + 1, std::memory_order_release);
}

} // namespace leanstore