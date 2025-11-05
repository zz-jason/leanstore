#pragma once

#include "leanstore/btree/core/b_tree_generic.hpp"
#include "leanstore/kv_interface.hpp"

namespace leanstore {

class LeanStore;

} // namespace leanstore

namespace leanstore {

class BasicKV : public KVInterface, public BTreeGeneric {
public:
  BasicKV() {
    tree_type_ = BTreeType::kBasicKV;
  }

  virtual OpCode Lookup(Slice key, ValCallback val_callback) override;

  virtual OpCode Insert(Slice key, Slice val) override;

  virtual OpCode UpdatePartial(Slice key, MutValCallback update_call_back,
                               UpdateDesc& update_desc) override;

  virtual OpCode Remove(Slice key) override;

  virtual OpCode ScanAsc(Slice start_key, ScanCallback callback) override;

  virtual OpCode ScanDesc(Slice start_key, ScanCallback callback) override;

  virtual OpCode PrefixLookup(Slice, PrefixLookupCallback callback) override;

  virtual OpCode PrefixLookupForPrev(Slice key, PrefixLookupCallback callback) override;

  virtual OpCode RangeRemove(Slice start_key, Slice end_key, bool page_used) override;

  virtual uint64_t CountEntries() override;

  bool IsRangeEmpty(Slice start_key, Slice end_key);

  static Result<BasicKV*> Create(leanstore::LeanStore* store, const std::string& tree_name,
                                 lean_btree_config config);

  /// Copy the slots from the value to the buffer.
  ///
  /// @param[in] updateDesc The update descriptor which contains the slots to
  /// update.
  /// @param[in] value The value to copy the slots from.
  /// @param[out] buffer The buffer to copy the slots to.
  static void CopyToBuffer(const UpdateDesc& update_desc, const uint8_t* value, uint8_t* buffer) {
    uint64_t buffer_offset = 0;
    for (uint64_t i = 0; i < update_desc.num_slots_; i++) {
      const auto& slot = update_desc.update_slots_[i];
      std::memcpy(buffer + buffer_offset, value + slot.offset_, slot.size_);
      buffer_offset += slot.size_;
    }
  }

  /// Update the slots in the value with data in the buffer.
  ///
  /// @param[in] updateDesc The update descriptor which contains the slots to
  /// update.
  /// @param[in] buffer The buffer to copy the slots from.
  /// @param[out] value The value to update the slots in.
  static void CopyToValue(const UpdateDesc& update_desc, const uint8_t* buffer, uint8_t* value) {
    uint64_t buffer_offset = 0;
    for (uint64_t i = 0; i < update_desc.num_slots_; i++) {
      const auto& slot = update_desc.update_slots_[i];
      std::memcpy(value + slot.offset_, buffer + buffer_offset, slot.size_);
      buffer_offset += slot.size_;
    }
  }

  static void XorToBuffer(const UpdateDesc& update_desc, const uint8_t* value, uint8_t* buffer) {
    uint64_t buffer_offset = 0;
    for (uint64_t i = 0; i < update_desc.num_slots_; i++) {
      const auto& slot = update_desc.update_slots_[i];
      for (uint64_t j = 0; j < slot.size_; j++) {
        buffer[buffer_offset + j] ^= value[slot.offset_ + j];
      }
      buffer_offset += slot.size_;
    }
  }

  static void XorToValue(const UpdateDesc& update_desc, const uint8_t* buffer, uint8_t* value) {
    uint64_t buffer_offset = 0;
    for (uint64_t i = 0; i < update_desc.num_slots_; i++) {
      const auto& slot = update_desc.update_slots_[i];
      for (uint64_t j = 0; j < slot.size_; j++) {
        value[slot.offset_ + j] ^= buffer[buffer_offset + j];
      }
      buffer_offset += slot.size_;
    }
  }

private:
  OpCode LookupOptimistic(Slice key, ValCallback val_callback);
  OpCode LookupPessimistic(Slice key, ValCallback val_callback);
};

} // namespace leanstore
