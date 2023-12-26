#pragma once

#include "Units.hpp"

#include <cstring>
#include <functional>

namespace leanstore {

enum class OP_RESULT : u8 {
  OK = 0,
  NOT_FOUND = 1,
  DUPLICATE = 2,
  ABORT_TX = 3,
  NOT_ENOUGH_SPACE = 4,
  OTHER = 5
};

inline std::string ToString(OP_RESULT result) {
  switch (result) {
  case OP_RESULT::OK: {
    return "OK";
  }
  case OP_RESULT::NOT_FOUND: {
    return "NOT_FOUND";
  }
  case OP_RESULT::DUPLICATE: {
    return "DUPLICATE";
  }
  case OP_RESULT::ABORT_TX: {
    return "ABORT_TX";
  }
  case OP_RESULT::NOT_ENOUGH_SPACE: {
    return "NOT_ENOUGH_SPACE";
  }
  case OP_RESULT::OTHER: {
    return "OTHER";
  }
  }
  return "Unknown OP_RESULT";
}

class UpdateDiffSlot {
public:
  u16 offset = 0;

  u16 length = 0;

public:
  bool operator==(const UpdateDiffSlot& other) const {
    return offset == other.offset && length == other.length;
  }
};

/// Memory layout:
/// ---------------------------------------
/// | N | UpdateDiffSlot 0..N | diff 0..N |
/// ---------------------------------------
class UpdateDesc {
public:
  u8 count = 0;

  UpdateDiffSlot mDiffSlots[];

public:
  u64 size() const {
    u64 totalSize = sizeof(UpdateDesc);
    totalSize += (count * sizeof(UpdateDiffSlot));
    return totalSize;
  }

  u64 TotalSize() const {
    return size() + diffSize();
  }

  bool operator==(const UpdateDesc& other) {
    if (count != other.count) {
      return false;
    }

    for (u8 i = 0; i < count; i++) {
      if (mDiffSlots[i].offset != other.mDiffSlots[i].offset ||
          mDiffSlots[i].length != other.mDiffSlots[i].length)
        return false;
    }
    return true;
  }

  void GenerateDiff(u8* dst, const u8* src) const {
    u64 dstOffset = 0;
    for (u64 i = 0; i < count; i++) {
      const auto& slot = mDiffSlots[i];
      std::memcpy(dst + dstOffset, src + slot.offset, slot.length);
      dstOffset += slot.length;
    }
  }

  void GenerateXORDiff(u8* dst, const u8* src) const {
    u64 dstOffset = 0;
    for (u64 i = 0; i < count; i++) {
      const auto& slot = mDiffSlots[i];
      for (u64 j = 0; j < slot.length; j++) {
        dst[dstOffset + j] ^= src[slot.offset + j];
      }
      dstOffset += slot.length;
    }
  }
  void ApplyDiff(u8* dst, const u8* src) const {
    u64 srcOffset = 0;
    for (u64 i = 0; i < count; i++) {
      const auto& slot = mDiffSlots[i];
      std::memcpy(dst + slot.offset, src + srcOffset, slot.length);
      srcOffset += slot.length;
    }
  }

  void ApplyXORDiff(u8* dst, const u8* src) const {
    u64 srcOffset = 0;
    for (u64 i = 0; i < count; i++) {
      const auto& slot = mDiffSlots[i];
      for (u64 j = 0; j < slot.length; j++) {
        dst[slot.offset + j] ^= src[srcOffset + j];
      }
      srcOffset += slot.length;
    }
  }

private:
  u64 diffSize() const {
    u64 length = 0;
    for (u8 i = 0; i < count; i++) {
      length += mDiffSlots[i].length;
    }
    return length;
  }

public:
  inline static const UpdateDesc* From(const u8* buffer) {
    return reinterpret_cast<const UpdateDesc*>(buffer);
  }
};

using StringU = std::basic_string<u8>;
using ValCallback = std::function<void(Slice val)>;
using ScanCallback = std::function<bool(Slice key, Slice val)>;
using PrefixLookupCallback = std::function<void(Slice key, Slice val)>;

class KVInterface {
public:
  virtual OP_RESULT Lookup(Slice key, ValCallback valCallback) = 0;
  virtual OP_RESULT insert(Slice key, Slice val) = 0;
  virtual OP_RESULT updateSameSizeInPlace(Slice key, ValCallback valCallback,
                                          UpdateDesc&) = 0;
  virtual OP_RESULT remove(Slice key) = 0;
  virtual OP_RESULT scanAsc(Slice startKey, ScanCallback callback) = 0;
  virtual OP_RESULT scanDesc(Slice startKey, ScanCallback callback) = 0;
  virtual OP_RESULT prefixLookup(Slice, PrefixLookupCallback) {
    return OP_RESULT::OTHER;
  }
  virtual OP_RESULT prefixLookupForPrev(Slice, PrefixLookupCallback) {
    return OP_RESULT::OTHER;
  }
  virtual OP_RESULT append(std::function<void(u8*)>, u16,
                           std::function<void(u8*)>, u16,
                           std::unique_ptr<u8[]>&) {
    return OP_RESULT::OTHER;
  }
  virtual OP_RESULT rangeRemove(Slice startKey [[maybe_unused]],
                                Slice endKey [[maybe_unused]],
                                bool page_wise [[maybe_unused]] = true) {
    return OP_RESULT::OTHER;
  }

  virtual u64 countPages() = 0;
  virtual u64 countEntries() = 0;
  virtual u64 getHeight() = 0;
};

struct MutableSlice {
  u8* ptr;
  u64 len;

  MutableSlice(u8* ptr, u64 len) : ptr(ptr), len(len) {
  }

  u64 length() {
    return len;
  }

  u8* data() {
    return ptr;
  }

  Slice Immutable() {
    return Slice(ptr, len);
  }
};

} // namespace leanstore
