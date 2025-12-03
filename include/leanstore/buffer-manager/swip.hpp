#pragma once

#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/common/types.h"
#include "leanstore/cpp/base/log.hpp"

namespace leanstore {

class BufferFrame;

/// Swip represents either the page id or the pointer to the buffer frame which contains the page.
/// It can be the following 3 stats:
/// - EVICTED: page id, the most most significant bit is 1 which marks the swip as "EVICTED".
/// - COOL: buffer frame pointer, the 2nd most most significant bit is 1 which marks it as "COOL".
/// - HOT: buffer frame pointer, the 2nd most most significant bit is 0 which marks it as "HOT".
///
/// i.e.
/// - 1xxxxxxxxxxxx EVICTED, page id
/// - 01xxxxxxxxxxx COOL, buffer frame pointer
/// - 00xxxxxxxxxxx HOT, buffer frame pointer
class Swip {
public:
  /// The actual data
  union {
    uint64_t page_id_;
    BufferFrame* bf_;
  };

  /// Create an empty swip.
  Swip() : page_id_(0) {
  }

  /// Create an swip pointing to the buffer frame.
  Swip(BufferFrame* bf) : bf_(bf) {
  }

  /// Whether two swip is equal.
  bool operator==(const Swip& other) const {
    return (Raw() == other.Raw());
  }

  bool IsHot() {
    return (page_id_ & (sEvictedBit | sCoolBit)) == 0;
  }

  bool IsCool() {
    return page_id_ & sCoolBit;
  }

  bool IsEvicted() {
    return page_id_ & sEvictedBit;
  }

  /// Whether this swip points to nothing, the memory pointer is nullptr
  bool IsEmpty() {
    return page_id_ == 0;
  }

  uint64_t AsPageId() {
    LEAN_DCHECK(IsEvicted());
    return page_id_ & sEvictedMask;
  }

  /// Return the underlying buffer frame from a hot buffer frame.
  BufferFrame& AsBufferFrame() {
    LEAN_DCHECK(IsHot());
    return *bf_;
  }

  /// Return the underlying buffer frame from a cool buffer frame.
  BufferFrame& AsBufferFrameMasked() {
    return *reinterpret_cast<BufferFrame*>(page_id_ & sHotMask);
  }

  uint64_t Raw() const {
    return page_id_;
  }

  void MarkHOT(BufferFrame* bf) {
    this->bf_ = bf;
  }

  void MarkHOT() {
    LEAN_DCHECK(IsCool());
    this->page_id_ = page_id_ & ~sCoolBit;
  }

  void Cool() {
    this->page_id_ = page_id_ | sCoolBit;
  }

  void Evict(lean_pid_t page_id) {
    this->page_id_ = page_id | sEvictedBit;
  }

private:
  static const uint64_t sEvictedBit = uint64_t(1) << 63;
  static const uint64_t sEvictedMask = ~(uint64_t(1) << 63);
  static const uint64_t sCoolBit = uint64_t(1) << 62;
  static const uint64_t sCoolMask = ~(uint64_t(1) << 62);
  static const uint64_t sHotMask = ~(uint64_t(3) << 62);

  static_assert(sEvictedBit == 0x8000000000000000, "");
  static_assert(sEvictedMask == 0x7FFFFFFFFFFFFFFF, "");
  static_assert(sHotMask == 0x3FFFFFFFFFFFFFFF, "");
};

} // namespace leanstore
