#pragma once

#include "leanstore/cpp/base/slice.hpp"

#include <cstdint>
#include <functional>
#include <string>

namespace leanstore {

enum class OpCode : uint8_t {
  kOK = 0,
  kNotFound,
  kDuplicated,
  kAbortTx,
  kSpaceNotEnough,
  kOther,
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

inline std::string ToString(TxMode tx_mode) {
  switch (tx_mode) {
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
    return IsolationLevel::kSerializable;
  }
  if (str == "si") {
    return IsolationLevel::kSnapshotIsolation;
  }
  return IsolationLevel::kSnapshotIsolation;
}

class UpdateSlotInfo {
public:
  uint16_t offset_ = 0;

  uint16_t size_ = 0;

public:
  bool operator==(const UpdateSlotInfo& other) const {
    return offset_ == other.offset_ && size_ == other.size_;
  }
};

/// Memory layout:
/// ---------------------------
/// | N | UpdateSlotInfo 0..N |
/// ---------------------------
class UpdateDesc {
public:
  uint8_t num_slots_ = 0;

  UpdateSlotInfo update_slots_[];

public:
  uint64_t Size() const {
    return UpdateDesc::Size(num_slots_);
  }

  uint64_t SizeWithDelta() const {
    return Size() + DeltaSize();
  }

private:
  uint64_t DeltaSize() const {
    uint64_t length = 0;
    for (uint8_t i = 0; i < num_slots_; i++) {
      length += update_slots_[i].size_;
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

  inline static uint64_t Size(uint8_t num_slots) {
    uint64_t self_size = sizeof(UpdateDesc);
    self_size += (num_slots * sizeof(UpdateSlotInfo));
    return self_size;
  }

  inline static UpdateDesc* CreateFrom(uint8_t* buffer) {
    auto* update_desc = new (buffer) UpdateDesc();
    return update_desc;
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

  /// Update old value with a same sized new value.
  /// NOTE: The value is updated via user provided callback.
  virtual OpCode UpdatePartial(Slice key, MutValCallback update_call_back,
                               UpdateDesc& update_desc) = 0;

  virtual OpCode Remove(Slice key) = 0;

  virtual OpCode RangeRemove(Slice start_key, Slice end_key, bool page_wise = true) = 0;

  virtual OpCode ScanAsc(Slice start_key, ScanCallback callback) = 0;

  virtual OpCode ScanDesc(Slice start_key, ScanCallback callback) = 0;

  virtual OpCode Lookup(Slice key, ValCallback val_callback) = 0;

  virtual OpCode PrefixLookup(Slice, PrefixLookupCallback) = 0;

  virtual OpCode PrefixLookupForPrev(Slice, PrefixLookupCallback) = 0;

  virtual uint64_t CountEntries() = 0;
};

} // namespace leanstore
