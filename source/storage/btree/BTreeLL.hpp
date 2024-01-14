#pragma once

#include "KVInterface.hpp"
#include "core/BTreeGeneric.hpp"
#include "storage/btree/core/BTreeWALPayload.hpp"
#include "utils/Error.hpp"

#include <expected>

using namespace leanstore::storage;

namespace leanstore {
namespace storage {
namespace btree {

class BTreeLL : public KVInterface, public BTreeGeneric {
public:
  struct WALUpdate : WALPayload {
    u16 mKeySize;
    u16 mDeltaLength;
    u8 payload[];
  };

  struct WALRemove : WALPayload {
    u16 mKeySize;
    u16 mValSize;
    u8 payload[];

    WALRemove(Slice key, Slice val)
        : WALPayload(TYPE::WALRemove),
          mKeySize(key.size()),
          mValSize(val.size()) {
      std::memcpy(payload, key.data(), key.size());
      std::memcpy(payload + key.size(), val.data(), val.size());
    }
  };

public:
  //---------------------------------------------------------------------------
  // Constructor and Destructors
  //---------------------------------------------------------------------------
  BTreeLL() {
    mTreeType = BTREE_TYPE::LL;
  }

public:
  //---------------------------------------------------------------------------
  // KV Interfaces
  //---------------------------------------------------------------------------
  virtual OpCode Lookup(Slice key, ValCallback valCallback) override;

  virtual OpCode insert(Slice key, Slice val) override;

  virtual OpCode updateSameSizeInPlace(Slice key, MutValCallback updateCallBack,
                                       UpdateDesc& updateDesc) override;

  virtual OpCode remove(Slice key) override;
  virtual OpCode ScanAsc(Slice startKey, ScanCallback callback) override;
  virtual OpCode ScanDesc(Slice startKey, ScanCallback callback) override;
  virtual OpCode prefixLookup(Slice, PrefixLookupCallback callback) override;
  virtual OpCode prefixLookupForPrev(Slice key,
                                     PrefixLookupCallback callback) override;

  virtual OpCode rangeRemove(Slice staryKey, Slice endKey,
                             bool page_used) override;

  // virtual u64 countPages() override;
  virtual u64 countEntries() override;
  // virtual u64 getHeight() override;

public:
  //---------------------------------------------------------------------------
  // Graveyard Interfaces
  //---------------------------------------------------------------------------
  bool isRangeSurelyEmpty(Slice start_key, Slice end_key);

public:
  [[nodiscard]] static auto Create(const std::string& treeName, Config& config)
      -> std::expected<BTreeLL*, utils::Error> {
    auto [treePtr, treeId] =
        TreeRegistry::sInstance->CreateTree(treeName, [&]() {
          return std::unique_ptr<BufferManagedTree>(
              static_cast<BufferManagedTree*>(new BTreeLL()));
        });
    if (treePtr == nullptr) {
      return std::unexpected<utils::Error>(
          utils::Error::General("Tree name has been taken"));
    }
    auto* tree = dynamic_cast<BTreeLL*>(treePtr);
    tree->Init(treeId, config);
    return tree;
  }

  /// Copy the slots from the value to the buffer.
  ///
  /// @param[in] updateDesc The update descriptor which contains the slots to
  /// update.
  /// @param[in] value The value to copy the slots from.
  /// @param[out] buffer The buffer to copy the slots to.
  inline static void CopyToBuffer(const UpdateDesc& updateDesc, const u8* value,
                                  u8* buffer) {
    u64 bufferOffset = 0;
    for (u64 i = 0; i < updateDesc.mNumSlots; i++) {
      const auto& slot = updateDesc.mUpdateSlots[i];
      std::memcpy(buffer + bufferOffset, value + slot.mOffset, slot.mSize);
      bufferOffset += slot.mSize;
    }
  }

  /// Update the slots in the value with data in the buffer.
  ///
  /// @param[in] updateDesc The update descriptor which contains the slots to
  /// update.
  /// @param[in] buffer The buffer to copy the slots from.
  /// @param[out] value The value to update the slots in.
  inline static void CopyToValue(const UpdateDesc& updateDesc, const u8* buffer,
                                 u8* value) {
    u64 bufferOffset = 0;
    for (u64 i = 0; i < updateDesc.mNumSlots; i++) {
      const auto& slot = updateDesc.mUpdateSlots[i];
      std::memcpy(value + slot.mOffset, buffer + bufferOffset, slot.mSize);
      bufferOffset += slot.mSize;
    }
  }

  inline static void XorToBuffer(const UpdateDesc& updateDesc, const u8* value,
                                 u8* buffer) {
    u64 bufferOffset = 0;
    for (u64 i = 0; i < updateDesc.mNumSlots; i++) {
      const auto& slot = updateDesc.mUpdateSlots[i];
      for (u64 j = 0; j < slot.mSize; j++) {
        buffer[bufferOffset + j] ^= value[slot.mOffset + j];
      }
      bufferOffset += slot.mSize;
    }
  }

  inline static void XorToValue(const UpdateDesc& updateDesc, const u8* buffer,
                                u8* value) {
    u64 bufferOffset = 0;
    for (u64 i = 0; i < updateDesc.mNumSlots; i++) {
      const auto& slot = updateDesc.mUpdateSlots[i];
      for (u64 j = 0; j < slot.mSize; j++) {
        value[slot.mOffset + j] ^= buffer[bufferOffset + j];
      }
      bufferOffset += slot.mSize;
    }
  }
};

} // namespace btree
} // namespace storage
} // namespace leanstore
