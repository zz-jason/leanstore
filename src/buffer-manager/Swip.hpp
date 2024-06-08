#pragma once

#include "buffer-manager/BufferFrame.hpp"
#include "leanstore/Units.hpp"
#include "utils/Log.hpp"

namespace leanstore::storage {

class BufferFrame;

//! Swip represents either the page id or the pointer to the buffer frame which contains the page.
//! It can be the following 3 stats:
//! - EVICTED: page id, the most most significant bit is 1 which marks the swip as "EVICTED".
//! - COOL: buffer frame pointer, the 2nd most most significant bit is 1 which marks it as "COOL".
//! - HOT: buffer frame pointer, the 2nd most most significant bit is 0 which marks it as "HOT".
//!
//! i.e.
//! - 1xxxxxxxxxxxx EVICTED, page id
//! - 01xxxxxxxxxxx COOL, buffer frame pointer
//! - 00xxxxxxxxxxx HOT, buffer frame pointer
class Swip {
public:
  //! The actual data
  union {
    uint64_t mPageId;
    BufferFrame* mBf;
  };

  //! Create an empty swip.
  Swip() : mPageId(0) {};

  //! Create an swip pointing to the buffer frame.
  Swip(BufferFrame* bf) : mBf(bf) {
  }

  //! Whether two swip is equal.
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

  //! Whether this swip points to nothing, the memory pointer is nullptr
  bool IsEmpty() {
    return mPageId == 0;
  }

  uint64_t AsPageId() {
    LS_DCHECK(IsEvicted());
    return mPageId & sEvictedMask;
  }

  //! Return the underlying buffer frame from a hot buffer frame.
  BufferFrame& AsBufferFrame() {
    LS_DCHECK(IsHot());
    return *mBf;
  }

  //! Return the underlying buffer frame from a cool buffer frame.
  BufferFrame& AsBufferFrameMasked() {
    return *reinterpret_cast<BufferFrame*>(mPageId & sHotMask);
  }

  uint64_t Raw() const {
    return mPageId;
  }

  void MarkHOT(BufferFrame* bf) {
    this->mBf = bf;
  }

  void MarkHOT() {
    LS_DCHECK(IsCool());
    this->mPageId = mPageId & ~sCoolBit;
  }

  void Cool() {
    this->mPageId = mPageId | sCoolBit;
  }

  void Evict(PID pageId) {
    this->mPageId = pageId | sEvictedBit;
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

} // namespace leanstore::storage
