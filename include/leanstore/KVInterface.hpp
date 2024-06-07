#pragma once

#include "leanstore/Slice.hpp"

#include <functional>

namespace leanstore {

enum class OpCode : uint8_t {
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

enum class TxMode : uint8_t {
  kLongRunning = 0,
  kShortRunning = 1,
};

inline std::string ToString(TxMode txMode) {
  switch (txMode) {
  case TxMode::kLongRunning: {
    return "LongRunning";
  }
  case TxMode::kShortRunning: {
    return "ShortRunning";
  }
  }
  return "Unknown TxMode";
}

enum class IsolationLevel : uint8_t {
  // kReadUnCommitted = 0,
  // kReadCommitted = 1,
  kSnapshotIsolation = 2,
  kSerializable = 3,
};

inline IsolationLevel ParseIsolationLevel(std::string str) {
  if (str == "ser") {
    return leanstore::IsolationLevel::kSerializable;
  }
  if (str == "si") {
    return leanstore::IsolationLevel::kSnapshotIsolation;
  }
  return leanstore::IsolationLevel::kSnapshotIsolation;
}

class UpdateSlotInfo {
public:
  uint16_t mOffset = 0;

  uint16_t mSize = 0;

public:
  bool operator==(const UpdateSlotInfo& other) const {
    return mOffset == other.mOffset && mSize == other.mSize;
  }
};

//! Memory layout:
//! ---------------------------
//! | N | UpdateSlotInfo 0..N |
//! ---------------------------
class UpdateDesc {
public:
  uint8_t mNumSlots = 0;

  UpdateSlotInfo mUpdateSlots[];

public:
  uint64_t Size() const {
    return UpdateDesc::Size(mNumSlots);
  }

  uint64_t SizeWithDelta() const {
    return Size() + deltaSize();
  }

private:
  uint64_t deltaSize() const {
    uint64_t length = 0;
    for (uint8_t i = 0; i < mNumSlots; i++) {
      length += mUpdateSlots[i].mSize;
    }
    return length;
  }

public:
  inline static const UpdateDesc* From(const uint8_t* buffer) {
    return reinterpret_cast<const UpdateDesc*>(buffer);
  }

  inline static UpdateDesc* From(uint8_t* buffer) {
    return reinterpret_cast<UpdateDesc*>(buffer);
  }

  inline static uint64_t Size(uint8_t numSlots) {
    uint64_t selfSize = sizeof(UpdateDesc);
    selfSize += (numSlots * sizeof(UpdateSlotInfo));
    return selfSize;
  }

  inline static UpdateDesc* CreateFrom(uint8_t* buffer) {
    auto* updateDesc = new (buffer) UpdateDesc();
    return updateDesc;
  }
};

class MutableSlice;
using StringU = std::basic_string<uint8_t>;
using ValCallback = std::function<void(Slice val)>;
using MutValCallback = std::function<void(MutableSlice val)>;
using ScanCallback = std::function<bool(Slice key, Slice val)>;
using PrefixLookupCallback = std::function<void(Slice key, Slice val)>;

class KVInterface {
public:
  virtual OpCode Insert(Slice key, Slice val) = 0;

  //! Update old value with a same sized new value.
  //! NOTE: The value is updated via user provided callback.
  virtual OpCode UpdatePartial(Slice key, MutValCallback updateCallBack,
                               UpdateDesc& updateDesc) = 0;

  virtual OpCode Remove(Slice key) = 0;

  virtual OpCode RangeRemove(Slice startKey, Slice endKey, bool pageWise = true) = 0;

  virtual OpCode ScanAsc(Slice startKey, ScanCallback callback) = 0;

  virtual OpCode ScanDesc(Slice startKey, ScanCallback callback) = 0;

  virtual OpCode Lookup(Slice key, ValCallback valCallback) = 0;

  virtual OpCode PrefixLookup(Slice, PrefixLookupCallback) = 0;

  virtual OpCode PrefixLookupForPrev(Slice, PrefixLookupCallback) = 0;

  virtual uint64_t CountEntries() = 0;
};

} // namespace leanstore
