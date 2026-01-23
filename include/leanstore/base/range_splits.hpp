#pragma once

#include "leanstore/base/log.hpp"

#include <cstddef>
#include <type_traits>

namespace leanstore {

/// A utility class to split a range [begin, end) into n sub-ranges as evenly as
/// possible.  Each sub-range is represented by a Range object, which provides
/// begin() and end() methods.
///
/// NOTE:
///   1. T must be an integral type.
///   2. num_splits must be > 0, and range_size must be >= num_splits.
template <typename T>
  requires(std::is_integral_v<T>)
class RangeSplits {
public:
  /// Represents a range [begin, end).
  class Range {
  public:
    /// Default constructor for Range.
    Range() = default;

    /// Constructs a Range with the given begin and end.
    Range(T begin, T end) : begin_(begin), end_(end) {
    }

    /// Returns the beginning of the range.
    T begin() const { // NOLINT: mimic STL iterator
      return begin_;
    }

    /// Returns the end of the range.
    T end() const { // NOLINT: mimic STL iterator
      return end_;
    }

  private:
    T begin_ = 0;
    T end_ = 0;
  };

  /// An iterator to iterate over the sub-ranges.
  class Iterator {
  public:
    /// Constructs an Iterator for the given parent RangeSplits and index.
    Iterator(const RangeSplits& parent, size_t idx) : parent_(parent), idx_(idx) {
      auto range_begin = parent_.GetRangeBegin(idx_);
      auto range_end = range_begin + parent_.GetRangeLength(idx_);
      cur_range_ = {range_begin, range_end};
    }

    /// Dereferences the iterator to get the current sub-range.
    Range operator*() const {
      return cur_range_;
    }

    /// Advances the iterator to the next sub-range.
    Iterator& operator++() {
      idx_++;
      auto last_end = cur_range_.end();
      cur_range_ = {last_end, last_end + parent_.GetRangeLength(idx_)};
      return *this;
    }

    /// Checks if two iterators are equal.
    bool operator!=(const Iterator& other) const {
      return !(&parent_ == &other.parent_ && idx_ == other.idx_);
    }

  private:
    T CurRangeLength() const {
      return parent_.base_ + (idx_ < parent_.extra_ ? 1 : 0);
    }

    const RangeSplits& parent_;
    size_t idx_;
    Range cur_range_;
  };

  /// Constructs a RangeSplits for the range [begin, end) and splits it into n
  /// sub-ranges.
  RangeSplits(Range range_to_split, size_t n)
      : range_to_split_(range_to_split),
        n_(n),
        base_((range_to_split.end() - range_to_split.begin()) / n),
        extra_((range_to_split.end() - range_to_split.begin()) % n) {
    LEAN_DCHECK(n_ > 0, "Number of splits must be greater than 0");
    LEAN_DCHECK(base_ > 0 || extra_ > 0, "Range too small to split");
  }

  RangeSplits(T end, size_t n) : RangeSplits({0, end}, n) {
  }

  /// Returns an iterator to the beginning of the sub-ranges.
  ///
  /// begin() and end() APIs are provided so that RangeSplits can be used in
  /// range-based for loop like STL containers:
  ///
  ///   for (auto range : RangeSplits(...)) { ... }
  ///
  Iterator begin() const { // NOLINT: mimic STL iterator
    return {*this, 0};
  }

  /// Returns an iterator to the end of the sub-ranges.
  Iterator end() const { // NOLINT: mimic STL iterator
    return {*this, n_};
  }

  size_t size() const { // NOLINT: mimic STL container
    return n_;
  }

  /// Returns the sub-range at the given index.
  Range operator[](size_t idx) const {
    return {GetRangeBegin(idx), GetRangeBegin(idx) + GetRangeLength(idx)};
  }

  /// Returns the beginning of the sub-range at the given index.
  T GetRangeBegin(size_t idx) const {
    auto range_begin = range_to_split_.begin() + base_ * idx;
    if (static_cast<T>(idx) < extra_) {
      range_begin += idx;
    } else {
      range_begin += extra_;
    }
    return range_begin;
  }

  /// Returns the length of the sub-range at the given index.
  T GetRangeLength(size_t idx) const {
    if (idx >= n_) {
      return 0;
    }
    if (static_cast<T>(idx) < extra_) {
      return base_ + 1;
    }
    return base_;
  }

private:
  Range range_to_split_;
  size_t n_;
  T base_;
  T extra_;
};

} // namespace leanstore