#pragma once

#include "leanstore/KVInterface.hpp"
#include "leanstore/btree/core/BTreeGeneric.hpp"

namespace leanstore {

class LeanStore;

} // namespace leanstore

namespace leanstore::storage::btree {

class BasicKV : public KVInterface, public BTreeGeneric {
public:
  BasicKV() {
    mTreeType = BTreeType::kBasicKV;
  }

  virtual OpCode Lookup(Slice key, ValCallback valCallback) override;

  virtual OpCode Insert(Slice key, Slice val) override;

  virtual OpCode UpdatePartial(Slice key, MutValCallback updateCallBack,
                               UpdateDesc& updateDesc) override;

  virtual OpCode Remove(Slice key) override;

  virtual OpCode ScanAsc(Slice startKey, ScanCallback callback) override;

  virtual OpCode ScanDesc(Slice startKey, ScanCallback callback) override;

  virtual OpCode PrefixLookup(Slice, PrefixLookupCallback callback) override;

  virtual OpCode PrefixLookupForPrev(Slice key, PrefixLookupCallback callback) override;

  virtual OpCode RangeRemove(Slice startKey, Slice endKey, bool pageUsed) override;

  virtual uint64_t CountEntries() override;

  bool IsRangeEmpty(Slice startKey, Slice endKey);

  static Result<BasicKV*> Create(leanstore::LeanStore* store, const std::string& treeName,
                                 BTreeConfig config);

  //! Copy the slots from the value to the buffer.
  ///
  //! @param[in] updateDesc The update descriptor which contains the slots to
  //! update.
  //! @param[in] value The value to copy the slots from.
  //! @param[out] buffer The buffer to copy the slots to.
  static void CopyToBuffer(const UpdateDesc& updateDesc, const uint8_t* value, uint8_t* buffer) {
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
  static void CopyToValue(const UpdateDesc& updateDesc, const uint8_t* buffer, uint8_t* value) {
    uint64_t bufferOffset = 0;
    for (uint64_t i = 0; i < updateDesc.mNumSlots; i++) {
      const auto& slot = updateDesc.mUpdateSlots[i];
      std::memcpy(value + slot.mOffset, buffer + bufferOffset, slot.mSize);
      bufferOffset += slot.mSize;
    }
  }

  static void XorToBuffer(const UpdateDesc& updateDesc, const uint8_t* value, uint8_t* buffer) {
    uint64_t bufferOffset = 0;
    for (uint64_t i = 0; i < updateDesc.mNumSlots; i++) {
      const auto& slot = updateDesc.mUpdateSlots[i];
      for (uint64_t j = 0; j < slot.mSize; j++) {
        buffer[bufferOffset + j] ^= value[slot.mOffset + j];
      }
      bufferOffset += slot.mSize;
    }
  }

  static void XorToValue(const UpdateDesc& updateDesc, const uint8_t* buffer, uint8_t* value) {
    uint64_t bufferOffset = 0;
    for (uint64_t i = 0; i < updateDesc.mNumSlots; i++) {
      const auto& slot = updateDesc.mUpdateSlots[i];
      for (uint64_t j = 0; j < slot.mSize; j++) {
        value[slot.mOffset + j] ^= buffer[bufferOffset + j];
      }
      bufferOffset += slot.mSize;
    }
  }

private:
  OpCode lookupOptimistic(Slice key, ValCallback valCallback);
  OpCode lookupPessimistic(Slice key, ValCallback valCallback);
};

} // namespace leanstore::storage::btree
