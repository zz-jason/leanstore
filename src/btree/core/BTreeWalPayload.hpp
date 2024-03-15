#pragma once

#include "btree/core/BTreeNode.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/Units.hpp"

#include <glog/logging.h>
#include <rapidjson/document.h>

#include <string>

namespace leanstore::storage::btree {

// forward declarations
class WalInsert;
class WalTxInsert;
class WalUpdate;
class WalTxUpdate;
class WalRemove;
class WalTxRemove;
class WalInitPage;
class WalSplitRoot;
class WalSplitNonRoot;

#define DO_WITH_TYPES(ACTION, ...)                                             \
  ACTION(kWalInsert, 1, "kWalInsert", __VA_ARGS__)                             \
  ACTION(kWalTxInsert, 2, "kWalTxInsert", __VA_ARGS__)                         \
  ACTION(kWalUpdate, 3, "kWalUpdate", __VA_ARGS__)                             \
  ACTION(kWalTxUpdate, 4, "kWalTxUpdate", __VA_ARGS__)                         \
  ACTION(kWalRemove, 5, "kWalRemove", __VA_ARGS__)                             \
  ACTION(kWalTxRemove, 6, "kWalTxRemove", __VA_ARGS__)                         \
  ACTION(kWalInitPage, 10, "kWalInitPage", __VA_ARGS__)                        \
  ACTION(kWalSplitRoot, 11, "kWalSplitRoot", __VA_ARGS__)                      \
  ACTION(kWalSplitNonRoot, 12, "kWalSplitNonRoot", __VA_ARGS__)                \
  ACTION(kWalUndefined, 100, "kWalUndefined", __VA_ARGS__)

#define DECR_TYPE(type, type_value, type_name, ...) type = type_value,
#define TYPE_NAME(type, type_value, type_name, ...)                            \
  case Type::type:                                                             \
    return type_name;

class WalPayload {
public:
  enum class Type : uint8_t { DO_WITH_TYPES(DECR_TYPE) };

  /// Type of WalPayload
  Type mType = Type::kWalUndefined;

  WalPayload() = default;

  WalPayload(Type type) : mType(type) {
  }

  std::string WalLogTypeName(Type type) const {
    switch (type) {
      DO_WITH_TYPES(TYPE_NAME);
    default:
      return "Unknown WAL log type";
    }
  }

  static const WalPayload* From(const void* data) {
    return reinterpret_cast<const WalPayload*>(const_cast<void*>(data));
  }

  static void ToJson(const WalPayload* wal, rapidjson::Document* doc);

  static std::string ToJsonString(const WalPayload* wal);

private:
  static void toJson(const WalPayload* wal, rapidjson::Document* doc);
  static void toJson(const WalInsert* wal, rapidjson::Document* doc);
  static void toJson(const WalTxInsert* wal, rapidjson::Document* doc);
  static void toJson(const WalUpdate* wal, rapidjson::Document* doc);
  static void toJson(const WalTxUpdate* wal, rapidjson::Document* doc);
  static void toJson(const WalRemove* wal, rapidjson::Document* doc);
  static void toJson(const WalTxRemove* wal, rapidjson::Document* doc);
  static void toJson(const WalInitPage* wal, rapidjson::Document* doc);
  static void toJson(const WalSplitRoot* wal, rapidjson::Document* doc);
  static void toJson(const WalSplitNonRoot* wal, rapidjson::Document* doc);
};

#undef TYPE_NAME
#undef DECR_TYPE
#undef DO_WITH_TYPES

class WalInsert : public WalPayload {
public:
  uint16_t mKeySize;

  uint16_t mValSize;

  uint8_t mPayload[];

  WalInsert(Slice key, Slice val)
      : WalPayload(Type::kWalInsert),
        mKeySize(key.size()),
        mValSize(val.size()) {
    std::memcpy(mPayload, key.data(), mKeySize);
    std::memcpy(mPayload + mKeySize, val.data(), mValSize);
  }

  inline Slice GetKey() const {
    return Slice(mPayload, mKeySize);
  }

  inline Slice GetVal() const {
    return Slice(mPayload + mKeySize, mValSize);
  }
};

class WalTxInsert : public WalPayload {
public:
  uint16_t mKeySize;

  uint16_t mValSize;

  WORKERID mPrevWorkerId;

  TXID mPrevTxId;

  COMMANDID mPrevCommandId;

  uint8_t mPayload[];

  WalTxInsert(Slice key, Slice val, WORKERID prevWorkerId, TXID prevTxId,
              COMMANDID prevCommandId)
      : WalPayload(Type::kWalTxInsert),
        mKeySize(key.size()),
        mValSize(val.size()),
        mPrevWorkerId(prevWorkerId),
        mPrevTxId(prevTxId),
        mPrevCommandId(prevCommandId) {
    std::memcpy(mPayload, key.data(), mKeySize);
    std::memcpy(mPayload + mKeySize, val.data(), mValSize);
  }

  Slice GetKey() const {
    return Slice(mPayload, mKeySize);
  }

  Slice GetVal() const {
    return Slice(mPayload + mKeySize, mValSize);
  }
};

class WalUpdate : public WalPayload {
public:
  uint16_t mKeySize;

  uint16_t mDeltaLength;

  uint8_t mPayload[];

  WalUpdate() : WalPayload(WalPayload::Type::kWalUpdate) {}
};

class WalTxUpdate : public WalPayload {
public:
  uint16_t mKeySize;

  uint64_t mUpdateDescSize;

  uint64_t mDeltaSize;

  WORKERID mPrevWorkerId;

  TXID mPrevTxId;

  // Xor result of old and new command id
  COMMANDID mXorCommandId;

  // Stores key, UpdateDesc, and Delta in order
  uint8_t mPayload[];

  WalTxUpdate(Slice key, UpdateDesc& updateDesc,
              uint64_t sizeOfUpdateDescAndDelta, WORKERID prevWorkerId,
              TXID prevTxId, COMMANDID xorCommandId)
      : WalPayload(Type::kWalTxUpdate),
        mKeySize(key.size()),
        mUpdateDescSize(updateDesc.Size()),
        mDeltaSize(sizeOfUpdateDescAndDelta - updateDesc.Size()),
        mPrevWorkerId(prevWorkerId),
        mPrevTxId(prevTxId),
        mXorCommandId(xorCommandId) {
    // key
    std::memcpy(mPayload, key.data(), key.size());
    // updateDesc
    std::memcpy(mPayload + key.size(), &updateDesc, updateDesc.Size());
  }

  inline Slice GetKey() const {
    return Slice(mPayload, mKeySize);
  }

  inline const UpdateDesc* GetUpdateDesc() const {
    auto* updateDesc = UpdateDesc::From(mPayload + mKeySize);
    DCHECK(updateDesc->Size() == mUpdateDescSize)
        << "Malformed WalTxUpdate: updateDesc->Size() != mUpdateDescSize"
        << ", updateDesc->Size() = " << updateDesc->Size()
        << ", mUpdateDescSize = " << mUpdateDescSize;
    return updateDesc;
  }

  inline uint8_t* GetDeltaPtr() {
    return mPayload + mKeySize + mUpdateDescSize;
  }

  inline const uint8_t* GetDeltaPtr() const {
    return mPayload + mKeySize + mUpdateDescSize;
  }

  uint64_t GetDeltaSize() const {
    return mDeltaSize;
  }
};

class WalRemove : public WalPayload {
public:
  uint16_t mKeySize;

  uint16_t mValSize;

  uint8_t mPayload[];

  WalRemove(Slice key, Slice val)
      : WalPayload(Type::kWalRemove),
        mKeySize(key.size()),
        mValSize(val.size()) {
    std::memcpy(mPayload, key.data(), key.size());
    std::memcpy(mPayload + key.size(), val.data(), val.size());
  }
};

class WalTxRemove : public WalPayload {
public:
  uint16_t mKeySize;

  uint16_t mValSize;

  WORKERID mPrevWorkerId;

  TXID mPrevTxId;

  COMMANDID mPrevCommandId;

  uint8_t mPayload[];

  WalTxRemove(Slice key, Slice val, WORKERID prevWorkerId, TXID prevTxId,
              COMMANDID prevCommandId)
      : WalPayload(Type::kWalTxRemove),
        mKeySize(key.size()),
        mValSize(val.size()),
        mPrevWorkerId(prevWorkerId),
        mPrevTxId(prevTxId),
        mPrevCommandId(prevCommandId) {
    std::memcpy(mPayload, key.data(), key.size());
    std::memcpy(mPayload + key.size(), val.data(), val.size());
  }

  Slice RemovedKey() const {
    return Slice(mPayload, mKeySize);
  }

  Slice RemovedVal() const {
    return Slice(mPayload + mKeySize, mValSize);
  }
};

class WalInitPage : public WalPayload {
public:
  TREEID mTreeId;

  bool mIsLeaf;

  WalInitPage(TREEID treeId, bool isLeaf)
      : WalPayload(Type::kWalInitPage),
        mTreeId(treeId),
        mIsLeaf(isLeaf) {
  }
};

class WalSplitRoot : public WalPayload {
public:
  PID mNewLeft;

  PID mNewRoot;

  PID mMetaNode;

  uint16_t mSplitSlot;

  uint16_t mSeparatorSize;

  bool mSeparatorTruncated;

  WalSplitRoot(PID newLeft, PID newRoot, PID metaNode,
               const BTreeNode::SeparatorInfo& sepInfo)
      : WalPayload(Type::kWalSplitRoot),
        mNewLeft(newLeft),
        mNewRoot(newRoot),
        mMetaNode(metaNode),
        mSplitSlot(sepInfo.mSlotId),
        mSeparatorSize(sepInfo.mSize),
        mSeparatorTruncated(sepInfo.trunc) {
  }
};

class WalSplitNonRoot : public WalPayload {
public:
  PID mParentPageId = -1;

  PID mNewLeft = -1;

  uint16_t mSplitSlot;

  uint16_t mSeparatorSize;

  bool mSeparatorTruncated;

  WalSplitNonRoot() : WalPayload(Type::kWalSplitNonRoot) {
  }

  WalSplitNonRoot(PID parent, PID newLeft,
                  const BTreeNode::SeparatorInfo& sepInfo)
      : WalPayload(Type::kWalSplitNonRoot),
        mParentPageId(parent),
        mNewLeft(newLeft),
        mSplitSlot(sepInfo.mSlotId),
        mSeparatorSize(sepInfo.mSize),
        mSeparatorTruncated(sepInfo.trunc) {
  }
};

} // namespace leanstore::storage::btree