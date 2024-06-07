#pragma once

#include "btree/core/BTreeGeneric.hpp"
#include "leanstore/KVInterface.hpp"

using namespace leanstore::storage;

namespace leanstore {

class LeanStore;

} // namespace leanstore

namespace leanstore::storage::btree {

class BasicKV : public KVInterface, public BTreeGeneric {
public:
  BasicKV() {
    mTreeType = BTreeType::kBasicKV;
  }

public:
  virtual OpCode Lookup(Slice key, ValCallback valCallback) override;

  virtual OpCode Insert(Slice key, Slice val) override;

  virtual OpCode UpdatePartial(Slice key, MutValCallback updateCallBack,
                               UpdateDesc& updateDesc) override;

  virtual OpCode Remove(Slice key) override;

  virtual OpCode ScanAsc(Slice startKey, ScanCallback callback) override;

  virtual OpCode ScanDesc(Slice startKey, ScanCallback callback) override;

  virtual OpCode PrefixLookup(Slice, PrefixLookupCallback callback) override;

  virtual OpCode PrefixLookupForPrev(Slice key, PrefixLookupCallback callback) override;

  virtual OpCode RangeRemove(Slice staryKey, Slice endKey, bool pageUsed) override;

  virtual uint64_t CountEntries() override;

public:
  bool IsRangeEmpty(Slice startKey, Slice endKey);

public:
  static Result<BasicKV*> Create(leanstore::LeanStore* store, const std::string& treeName,
                                 BTreeConfig config);

  //! Copy the slots from the value to the buffer.
  ///
  //! @param[in] updateDesc The update descriptor which contains the slots to
  //! update.
  //! @param[in] value The value to copy the slots from.
  //! @param[out] buffer The buffer to copy the slots to.
  inline static void CopyToBuffer(const UpdateDesc& updateDesc, const uint8_t* value,
                                  uint8_t* buffer) {
    uint64_t bufferOffset = 0;
    for (uint64_t i = 0; i < updateDesc.mNumSlots; i++) {
      const auto& slot = updateDesc.mUpdateSlots[i];
      std::memcpy(buffer + bufferOffset, value + slot.mOffset, slot.mSize);
      bufferOffset += slot.mSize;
    }
  }

  //! Update the slots in the value with data in the buffer.
  ///
  //! @param[in] updateDesc The update descriptor which contains the slots to
  //! update.
  //! @param[in] buffer The buffer to copy the slots from.
  //! @param[out] value The value to update the slots in.
  inline static void CopyToValue(const UpdateDesc& updateDesc, const uint8_t* buffer,
                                 uint8_t* value) {
    uint64_t bufferOffset = 0;
    for (uint64_t i = 0; i < updateDesc.mNumSlots; i++) {
      const auto& slot = updateDesc.mUpdateSlots[i];
      std::memcpy(value + slot.mOffset, buffer + bufferOffset, slot.mSize);
      bufferOffset += slot.mSize;
    }
  }

  inline static void XorToBuffer(const UpdateDesc& updateDesc, const uint8_t* value,
                                 uint8_t* buffer) {
    uint64_t bufferOffset = 0;
    for (uint64_t i = 0; i < updateDesc.mNumSlots; i++) {
      const auto& slot = updateDesc.mUpdateSlots[i];
      for (uint64_t j = 0; j < slot.mSize; j++) {
        buffer[bufferOffset + j] ^= value[slot.mOffset + j];
      }
      bufferOffset += slot.mSize;
    }
  }

  inline static void XorToValue(const UpdateDesc& updateDesc, const uint8_t* buffer,
                                uint8_t* value) {
    uint64_t bufferOffset = 0;
    for (uint64_t i = 0; i < updateDesc.mNumSlots; i++) {
      const auto& slot = updateDesc.mUpdateSlots[i];
      for (uint64_t j = 0; j < slot.mSize; j++) {
        value[slot.mOffset + j] ^= buffer[bufferOffset + j];
      }
      bufferOffset += slot.mSize;
    }
  }
};

} // namespace leanstore::storage::btree
