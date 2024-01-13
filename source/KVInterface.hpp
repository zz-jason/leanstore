#pragma once

#include "shared-headers/Units.hpp"

#include <cstring>
#include <functional>

namespace leanstore {

enum class OpCode : u8 {
  kOK = 0,
  kNotFound = 1,
  kDuplicated = 2,
  kAbortTx = 3,
  kSpaceNotEnough = 4,
  kOther = 5
};

inline std::string ToString(OpCode result) {
  switch (result) {
  case OpCode::kOK: {
    return "OK";
  }
  case OpCode::kNotFound: {
    return "NOT_FOUND";
  }
  case OpCode::kDuplicated: {
    return "DUPLICATED";
  }
  case OpCode::kAbortTx: {
    return "ABORT_TX";
  }
  case OpCode::kSpaceNotEnough: {
    return "NOT_ENOUGH_SPACE";
  }
  case OpCode::kOther: {
    return "OTHER";
  }
  }
  return "Unknown OpCode";
}

class UpdateSlotInfo {
public:
  u16 mOffset = 0;

  u16 mSize = 0;

public:
  bool operator==(const UpdateSlotInfo& other) const {
    return mOffset == other.mOffset && mSize == other.mSize;
  }
};

/// Memory layout:
/// ---------------------------------------
/// | N | UpdateSlotInfo 0..N | diff 0..N |
/// ---------------------------------------
class UpdateDesc {
public:
  u8 mNumSlots = 0;

  UpdateSlotInfo mUpdateSlots[];

public:
  u64 Size() const {
    return UpdateDesc::Size(mNumSlots);
  }

  u64 TotalSize() const {
    return Size() + numBytesToUpdate();
  }

  bool operator==(const UpdateDesc& other) {
    if (mNumSlots != other.mNumSlots) {
      return false;
    }

    for (u8 i = 0; i < mNumSlots; i++) {
      if (mUpdateSlots[i].mOffset != other.mUpdateSlots[i].mOffset ||
          mUpdateSlots[i].mSize != other.mUpdateSlots[i].mSize)
        return false;
    }
    return true;
  }

  void CopySlots(u8* dst, const u8* src) const {
    u64 dstOffset = 0;
    for (u64 i = 0; i < mNumSlots; i++) {
      const auto& slot = mUpdateSlots[i];
      std::memcpy(dst + dstOffset, src + slot.mOffset, slot.mSize);
      dstOffset += slot.mSize;
    }
  }

  void XORSlots(u8* dst, const u8* src) const {
    u64 dstOffset = 0;
    for (u64 i = 0; i < mNumSlots; i++) {
      const auto& slot = mUpdateSlots[i];
      for (u64 j = 0; j < slot.mSize; j++) {
        dst[dstOffset + j] ^= src[slot.mOffset + j];
      }
      dstOffset += slot.mSize;
    }
  }

  void ApplyDiff(u8* dst, const u8* src) const {
    u64 srcOffset = 0;
    for (u64 i = 0; i < mNumSlots; i++) {
      const auto& slot = mUpdateSlots[i];
      std::memcpy(dst + slot.mOffset, src + srcOffset, slot.mSize);
      srcOffset += slot.mSize;
    }
  }

  void ApplyXORDiff(u8* dst, const u8* src) const {
    u64 srcOffset = 0;
    for (u64 i = 0; i < mNumSlots; i++) {
      const auto& slot = mUpdateSlots[i];
      for (u64 j = 0; j < slot.mSize; j++) {
        dst[slot.mOffset + j] ^= src[srcOffset + j];
      }
      srcOffset += slot.mSize;
    }
  }

private:
  u64 numBytesToUpdate() const {
    u64 length = 0;
    for (u8 i = 0; i < mNumSlots; i++) {
      length += mUpdateSlots[i].mSize;
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
    selfSize += (numSlots * sizeof(UpdateSlotInfo));
    return selfSize;
  }

  inline static UpdateDesc* CreateFrom(u8* buffer) {
    auto* updateDesc = new (buffer) UpdateDesc();
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
  virtual OpCode updateSameSizeInPlace(Slice key, MutValCallback updateCallBack,
                                       UpdateDesc& updateDesc) = 0;

  virtual OpCode remove(Slice key) = 0;

  virtual OpCode ScanAsc(Slice startKey, ScanCallback callback) = 0;

  virtual OpCode ScanDesc(Slice startKey, ScanCallback callback) = 0;

  virtual OpCode prefixLookup(Slice, PrefixLookupCallback) = 0;

  virtual OpCode prefixLookupForPrev(Slice, PrefixLookupCallback) = 0;

  virtual OpCode append(std::function<void(u8*)>, u16, std::function<void(u8*)>,
                        u16, std::unique_ptr<u8[]>&) = 0;

  virtual OpCode rangeRemove(Slice startKey [[maybe_unused]],
                             Slice endKey [[maybe_unused]],
                             bool page_wise [[maybe_unused]] = true) = 0;

  virtual u64 countPages() = 0;

  virtual u64 countEntries() = 0;

  virtual u64 getHeight() = 0;
};

class MutableSlice {
private:
  u8* mData;
  u64 mSize;

public:
  MutableSlice(u8* ptr, u64 len) : mData(ptr), mSize(len) {
  }

  u8* Data() {
    return mData;
  }

  u64 Size() {
    return mSize;
  }

  Slice Immutable() {
    return Slice(mData, mSize);
  }
};

} // namespace leanstore
