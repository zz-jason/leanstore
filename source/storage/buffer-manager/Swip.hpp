#pragma once

#include "BufferFrame.hpp"
#include "shared-headers/Units.hpp"

#include <glog/logging.h>

namespace leanstore {
namespace storage {

class BufferFrame;

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

public:
  /// Create an empty swip.
  Swip() : mPageId(0){};

  /// Create an swip pointing to the buffer frame.
  Swip(BufferFrame* bf) : bf(bf) {
  }

  /// Copy construct from another swip.
  template <typename T2> Swip(Swip<T2>& other) : mPageId(other.mPageId) {
  }

public:
  /// Whether two swip is equal.
  bool operator==(const Swip& other) const {
    return (Raw() == other.Raw());
  }

  bool IsHot() {
    return (mPageId & (sEvictedBit | sCoolBit)) == 0;
  }

  bool IsCool() {
    return mPageId & sCoolBit;
  }

  bool IsEvicted() {
    return mPageId & sEvictedBit;
  }

  /// Indicates whether this swip points to nothing: no evicted bit, no cool
  /// bit, the memory pointer is nullptr
  bool IsEmpty() {
    return mPageId == 0;
  }

  u64 AsPageId() {
    DCHECK(IsEvicted());
    return mPageId & sEvictedMask;
  }

  /// Return the underlying buffer frame from a HOT buffer frame.
  BufferFrame& AsBufferFrame() {
    DCHECK(IsHot());
    return *bf;
  }

  /// Return the underlying buffer frame from a COOL buffer frame.
  BufferFrame& AsBufferFrameMasked() {
    return *reinterpret_cast<BufferFrame*>(mPageId & sHotMask);
  }

  u64 Raw() const {
    return mPageId;
  }

  template <typename T2> void MarkHOT(T2* bf) {
    this->bf = bf;
  }

  void MarkHOT() {
    DCHECK(IsCool());
    this->mPageId = mPageId & ~sCoolBit;
  }

  void Cool() {
    this->mPageId = mPageId | sCoolBit;
  }

  void evict(PID pageId) {
    this->mPageId = pageId | sEvictedBit;
  }

  template <typename T2> Swip<T2>& CastTo() {
    return *reinterpret_cast<Swip<T2>*>(this);
  }

private:
  // 1xxxxxxxxxxxx evicted
  // 01xxxxxxxxxxx cool
  // 00xxxxxxxxxxx hot

  static const u64 sEvictedBit = u64(1) << 63;
  static const u64 sEvictedMask = ~(u64(1) << 63);
  static const u64 sCoolBit = u64(1) << 62;
  static const u64 sCoolMask = ~(u64(1) << 62);
  static const u64 sHotMask = ~(u64(3) << 62);

  static_assert(sEvictedBit == 0x8000000000000000, "");
  static_assert(sEvictedMask == 0x7FFFFFFFFFFFFFFF, "");
  static_assert(sHotMask == 0x3FFFFFFFFFFFFFFF, "");
};

} // namespace storage
} // namespace leanstore
