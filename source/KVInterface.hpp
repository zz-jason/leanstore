#pragma once

#include "Units.hpp"

#include <cstring>
#include <functional>

namespace leanstore {

enum class OpCode : u8 {
  OK = 0,
  NOT_FOUND = 1,
  DUPLICATE = 2,
  ABORT_TX = 3,
  NOT_ENOUGH_SPACE = 4,
  OTHER = 5
};

inline std::string ToString(OpCode result) {
  switch (result) {
  case OpCode::OK: {
    return "OK";
  }
  case OpCode::NOT_FOUND: {
    return "NOT_FOUND";
  }
  case OpCode::DUPLICATE: {
    return "DUPLICATE";
  }
  case OpCode::ABORT_TX: {
    return "ABORT_TX";
  }
  case OpCode::NOT_ENOUGH_SPACE: {
    return "NOT_ENOUGH_SPACE";
  }
  case OpCode::OTHER: {
    return "OTHER";
  }
  }
  return "Unknown OpCode";
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
  u8 mNumSlots = 0;

  UpdateDiffSlot mDiffSlots[];

public:
  u64 size() const {
    return UpdateDesc::Size(mNumSlots);
  }

  u64 TotalSize() const {
    return size() + diffSize();
  }

  bool operator==(const UpdateDesc& other) {
    if (mNumSlots != other.mNumSlots) {
      return false;
    }

    for (u8 i = 0; i < mNumSlots; i++) {
      if (mDiffSlots[i].offset != other.mDiffSlots[i].offset ||
          mDiffSlots[i].length != other.mDiffSlots[i].length)
        return false;
    }
    return true;
  }

  void CopySlots(u8* dst, const u8* src) const {
    u64 dstOffset = 0;
    for (u64 i = 0; i < mNumSlots; i++) {
      const auto& slot = mDiffSlots[i];
      std::memcpy(dst + dstOffset, src + slot.offset, slot.length);
      dstOffset += slot.length;
    }
  }

  void XORSlots(u8* dst, const u8* src) const {
    u64 dstOffset = 0;
    for (u64 i = 0; i < mNumSlots; i++) {
      const auto& slot = mDiffSlots[i];
      for (u64 j = 0; j < slot.length; j++) {
        dst[dstOffset + j] ^= src[slot.offset + j];
      }
      dstOffset += slot.length;
    }
  }

  void ApplyDiff(u8* dst, const u8* src) const {
    u64 srcOffset = 0;
    for (u64 i = 0; i < mNumSlots; i++) {
      const auto& slot = mDiffSlots[i];
      std::memcpy(dst + slot.offset, src + srcOffset, slot.length);
      srcOffset += slot.length;
    }
  }

  void ApplyXORDiff(u8* dst, const u8* src) const {
    u64 srcOffset = 0;
    for (u64 i = 0; i < mNumSlots; i++) {
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
    for (u8 i = 0; i < mNumSlots; i++) {
      length += mDiffSlots[i].length;
    }
    return length;
  }

public:
  inline static const UpdateDesc* From(const u8* buffer) {
    return reinterpret_cast<const UpdateDesc*>(buffer);
  }

  inline static UpdateDesc* From(u8* buffer) {
    return reinterpret_cast<UpdateDesc*>(buffer);
  }

  inline static u64 Size(u8 numSlots) {
    u64 selfSize = sizeof(UpdateDesc);
    selfSize += (numSlots * sizeof(UpdateDiffSlot));
    return selfSize;
  }

  inline static UpdateDesc* CreateFrom(u8* buffer) {
    auto updateDesc = new (buffer) UpdateDesc();
    return updateDesc;
  }
};

class MutableSlice;
using StringU = std::basic_string<u8>;
using ValCallback = std::function<void(Slice val)>;
using MutValCallback = std::function<void(MutableSlice val)>;
using ScanCallback = std::function<bool(Slice key, Slice val)>;
using PrefixLookupCallback = std::function<void(Slice key, Slice val)>;

class KVInterface {
public:
  virtual OpCode Lookup(Slice key, ValCallback valCallback) = 0;
  virtual OpCode insert(Slice key, Slice val) = 0;

  /// Update the old value with a same sized new value.
  /// NOTE: The value is updated via user provided callback.
  virtual OpCode updateSameSizeInPlace(Slice key,
                                          MutValCallback updateCallBack,
                                          UpdateDesc& updateDesc) = 0;

  virtual OpCode remove(Slice key) = 0;
  virtual OpCode scanAsc(Slice startKey, ScanCallback callback) = 0;
  virtual OpCode scanDesc(Slice startKey, ScanCallback callback) = 0;
  virtual OpCode prefixLookup(Slice, PrefixLookupCallback) {
    return OpCode::OTHER;
  }
  virtual OpCode prefixLookupForPrev(Slice, PrefixLookupCallback) {
    return OpCode::OTHER;
  }
  virtual OpCode append(std::function<void(u8*)>, u16,
                           std::function<void(u8*)>, u16,
                           std::unique_ptr<u8[]>&) {
    return OpCode::OTHER;
  }
  virtual OpCode rangeRemove(Slice startKey [[maybe_unused]],
                                Slice endKey [[maybe_unused]],
                                bool page_wise [[maybe_unused]] = true) {
    return OpCode::OTHER;
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

  u64 Size() {
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
