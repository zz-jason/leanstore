#pragma once

#include "Units.hpp"

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

class UpdateSameSizeInPlaceDescriptor {
public:
  class Slot {
  public:
    u16 offset;
    u16 length;

    bool operator==(const Slot& other) const {
      return offset == other.offset && length == other.length;
    }
  };

  u8 count = 0;
  Slot slots[];

  u64 size() const {
    u64 totalSize = sizeof(UpdateSameSizeInPlaceDescriptor);
    totalSize += (count * sizeof(UpdateSameSizeInPlaceDescriptor::Slot));
    return totalSize;
  }

  u64 diffLength() const {
    u64 length = 0;
    for (u8 i = 0; i < count; i++) {
      length += slots[i].length;
    }
    return length;
  }

  u64 totalLength() const {
    return size() + diffLength();
  }

  bool operator==(const UpdateSameSizeInPlaceDescriptor& other) {
    if (count != other.count) {
      return false;
    }

    for (u8 i = 0; i < count; i++) {
      if (slots[i].offset != other.slots[i].offset ||
          slots[i].length != other.slots[i].length)
        return false;
    }
    return true;
  }
};

using Slice = std::basic_string_view<u8>;
using StringU = std::basic_string<u8>;
using ValCallback = std::function<void(Slice val)>;
using ScanCallback = std::function<bool(Slice key, Slice val)>;
using PrefixLookupCallback = std::function<void(Slice key, Slice val)>;

class KVInterface {
public:
  virtual OP_RESULT Lookup(Slice key, ValCallback valCallback) = 0;
  virtual OP_RESULT insert(Slice key, Slice val) = 0;
  virtual OP_RESULT updateSameSizeInPlace(Slice key, ValCallback valCallback,
                                          UpdateSameSizeInPlaceDescriptor&) = 0;
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
