#pragma once

#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/common/types.h"
#include "leanstore/cpp/base/log.hpp"

#include <cstdint>

namespace leanstore {

class BufferFrame;

/// Swip represents either the page id or the pointer to the buffer frame which
/// contains the page. A swip can be in one of the three states:
///
/// - EVICTED: page id, the most most significant bit is 1 which marks the swip as "EVICTED".
/// - COOL: buffer frame pointer, the 2nd most most significant bit is 1 which marks it as "COOL".
/// - HOT: buffer frame pointer, the 2nd most most significant bit is 0 which marks it as "HOT".
///
/// i.e.
/// - 1xxxxxxxxxxxx EVICTED, page id
/// - 01xxxxxxxxxxx COOL, buffer frame pointer
/// - 00xxxxxxxxxxx HOT, buffer frame pointer
///
/// The rationale behind this is pointer tagging, which allows us to store the
/// state of the swip without any additional space overhead.
class Swip {
public:
  /// The actual data, both are 64 bits, either page id or pointer to buffer frame.
  union {
    /// Alias to page id with evicted tag.
    uint64_t page_id_;

    /// Alias to buffer frame pointer with cool/hot tag.
    BufferFrame* bf_;

    /// Raw access to the underlying value, used for comparison, tagging, etc.
    uint64_t raw_value_;
  };

  /// Create an empty swip.
  Swip() : raw_value_(0) {
  }

  /// Create a swip from a buffer frame pointer.
  explicit Swip(BufferFrame* bf) : bf_(bf) {
  }

  /// Reset the swip to a buffer frame pointer. After this, the swip is in hot
  /// state.
  void FromBufferFrame(BufferFrame* bf) {
    this->bf_ = bf;
    LEAN_DCHECK(IsHot(), "The swip should be hot now");
  }

  /// Reset the swip to a page id. After this, the swip is in evicted state.
  void FromPageId(lean_pid_t page_id) {
    this->raw_value_ = page_id | kEvictedBit;
    LEAN_DCHECK(IsEvicted(), "The swip should be evicted now");
  }

  /// Set the swip to hot state. Only applicable when the swip is cool. The cool
  /// tag is removed from the underlying buffer frame pointer.
  void SetToHot() {
    LEAN_DCHECK(IsCool(), "The swip to hot should be cool");
    this->raw_value_ = raw_value_ & ~kCoolBit;
  }

  /// Set the swip to cool state. Only applicable when the swip is hot. The
  /// underlying buffer frame pointer is tagged with cool bit, i.e. the 2nd most
  /// significant bit is set to 1.
  void SetToCool() {
    LEAN_DCHECK(IsHot(), "The swip to cool should be hot");
    this->raw_value_ = raw_value_ | kCoolBit;
  }

  /// Set the swip to evicted state, the underlying raw value is changed to the
  /// corresponding page id with evicted tag, i.e. the most significant bit is
  /// set to 1.
  void SetToEvicted() {
    LEAN_DCHECK(IsHot() || IsCool(), "The swip to evict should be either hot or cool");
    this->raw_value_ = AsBufferFrameMasked().GetPageId() | kEvictedBit;
  }

  /// Whether two swip is equal.
  bool operator==(const Swip& other) const {
    return (raw_value_ == other.raw_value_);
  }

  /// Check whether the swip is empty.
  bool IsEmpty() {
    return raw_value_ == 0;
  }

  /// Check whether the swip is in hot state.
  bool IsHot() {
    return !IsEmpty() && (raw_value_ & (kEvictedBit | kCoolBit)) == 0;
  }

  /// Check whether the swip is in cool state.
  bool IsCool() {
    return !IsEmpty() && (raw_value_ & kCoolBit);
  }

  /// Check whether the swip is in evicted state.
  bool IsEvicted() {
    return !IsEmpty() && (raw_value_ & kEvictedBit);
  }

  /// Return the underlying buffer frame from a hot buffer frame.
  BufferFrame& AsBufferFrame() {
    LEAN_DCHECK(IsHot());
    return *bf_;
  }

  /// Return the underlying buffer frame from a cool buffer frame.
  BufferFrame& AsBufferFrameMasked() {
    LEAN_DCHECK(IsHot() || IsCool(), "The swip should be either hot or cool");
    return *reinterpret_cast<BufferFrame*>(raw_value_ & kHotMask);
  }

  /// Return the underlying page id from an evicted swip.
  uint64_t AsPageId() {
    LEAN_DCHECK(IsEvicted());
    return raw_value_ & kEvictedMask;
  }

private:
  static constexpr uint64_t kEvictedBit = uint64_t(1) << 63;
  static constexpr uint64_t kEvictedMask = ~(uint64_t(1) << 63);
  static constexpr uint64_t kCoolBit = uint64_t(1) << 62;
  static constexpr uint64_t kCoolMask = ~(uint64_t(1) << 62);
  static constexpr uint64_t kHotMask = ~(uint64_t(3) << 62);
};

static_assert(sizeof(Swip) == 8, "Swip size should be 8 bytes");

} // namespace leanstore
