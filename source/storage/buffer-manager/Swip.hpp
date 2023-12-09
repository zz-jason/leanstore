#pragma once

#include "BufferFrame.hpp"
#include "Units.hpp"

#include <atomic>

namespace leanstore {
namespace storage {

struct BufferFrame;

/// Swip represents either the page id or the pointer to the buffer frame
/// which contains the page. It can be the following 3 stats:
/// 1. HOT. The swip represents the memory pointer to a buffer frame. The 2 most
///    significant bits are both 0s.
/// 2. COOL. The swip represents the memory pointer to a buffer frame. But the
///    2nd most most significant bits is 1 which marks the pointer as "COOL".
/// 3. EVICTED. The swip represents a page id. The most most significant bit is
///    1 which marks the swip as "EVICTED".
template <typename T> class Swip {

public:
  union {
    u64 mPageId;
    BufferFrame* bf;
  };

  Swip() = default;

  Swip(BufferFrame* bf) : bf(bf) {
  }

  template <typename T2> Swip(Swip<T2>& other) : mPageId(other.mPageId) {
  }

  bool operator==(const Swip& other) const {
    return (raw() == other.raw());
  }

  bool isHOT() {
    return (mPageId & (evicted_bit | cool_bit)) == 0;
  }

  bool isCOOL() {
    return mPageId & cool_bit;
  }

  bool isEVICTED() {
    return mPageId & evicted_bit;
  }

  u64 asPageID() {
    assert(isEVICTED());
    return mPageId & evicted_mask;
  }

  /// Return the underlying buffer frame from a HOT buffer frame.
  BufferFrame& AsBufferFrame() {
    assert(isHOT());
    return *bf;
  }

  /// Return the underlying buffer frame from a COOL buffer frame.
  BufferFrame& asBufferFrameMasked() {
    return *reinterpret_cast<BufferFrame*>(mPageId & hot_mask);
  }

  u64 raw() const {
    return mPageId;
  }

  template <typename T2> void MarkHOT(T2* bf) {
    this->bf = bf;
  }

  void MarkHOT() {
    assert(isCOOL());
    this->mPageId = mPageId & ~cool_bit;
  }

  void cool() {
    this->mPageId = mPageId | cool_bit;
  }

  void evict(PID pageId) {
    this->mPageId = pageId | evicted_bit;
  }

  template <typename T2> Swip<T2>& CastTo() {
    return *reinterpret_cast<Swip<T2>*>(this);
  }

private:
  // 1xxxxxxxxxxxx evicted,
  // 01xxxxxxxxxxx cool,
  // 00xxxxxxxxxxx hot
  static const u64 evicted_bit = u64(1) << 63;
  static const u64 evicted_mask = ~(u64(1) << 63);
  static const u64 cool_bit = u64(1) << 62;
  static const u64 cool_mask = ~(u64(1) << 62);
  static const u64 hot_mask = ~(u64(3) << 62);

  static_assert(evicted_bit == 0x8000000000000000, "");
  static_assert(evicted_mask == 0x7FFFFFFFFFFFFFFF, "");
  static_assert(hot_mask == 0x3FFFFFFFFFFFFFFF, "");
};

} // namespace storage
} // namespace leanstore
