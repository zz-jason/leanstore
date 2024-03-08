#pragma once

#include "btree/core/BTreeNode.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/Units.hpp"

#include <glog/logging.h>
#include <rapidjson/document.h>

#include <string>

namespace leanstore {
namespace storage {
namespace btree {

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

class WALPayload {
public:
  enum class Type : uint8_t { DO_WITH_TYPES(DECR_TYPE) };

public:
  /// Type of WALPayload
  Type mType = Type::kWalUndefined;

public:
  WALPayload() = default;

  WALPayload(Type type) : mType(type) {
  }

  virtual std::unique_ptr<rapidjson::Document> ToJson();

  inline std::string WalLogTypeName(Type type);

  inline static const WALPayload* From(const void* data) {
    return reinterpret_cast<const WALPayload*>(const_cast<void*>(data));
  }
};

class WALInitPage : WALPayload {
public:
  TREEID mTreeId;

  bool mIsLeaf;

public:
  WALInitPage(TREEID treeId, bool isLeaf)
      : WALPayload(Type::kWalInitPage),
        mTreeId(treeId),
        mIsLeaf(isLeaf) {
  }

public:
  std::unique_ptr<rapidjson::Document> ToJson() override;
};

struct WalSplitRoot : WALPayload {
  PID mNewLeft;

  PID mNewRoot;

  PID mMetaNode;

  uint16_t mSplitSlot;

  uint16_t mSeparatorSize;

  bool mSeparatorTruncated;

  WalSplitRoot(PID newLeft, PID newRoot, PID metaNode,
               const BTreeNode::SeparatorInfo& sepInfo)
      : WALPayload(Type::kWalSplitRoot),
        mNewLeft(newLeft),
        mNewRoot(newRoot),
        mMetaNode(metaNode),
        mSplitSlot(sepInfo.mSlotId),
        mSeparatorSize(sepInfo.mSize),
        mSeparatorTruncated(sepInfo.trunc) {
  }

  std::unique_ptr<rapidjson::Document> ToJson() override;
};

struct WalSplitNonRoot : WALPayload {
  PID mParentPageId = -1;

  PID mNewLeft = -1;

  uint16_t mSplitSlot;

  uint16_t mSeparatorSize;

  bool mSeparatorTruncated;

  WalSplitNonRoot() : WALPayload(Type::kWalSplitNonRoot) {
  }

  WalSplitNonRoot(PID parent, PID newLeft,
                  const BTreeNode::SeparatorInfo& sepInfo)
      : WALPayload(Type::kWalSplitNonRoot),
        mParentPageId(parent),
        mNewLeft(newLeft),
        mSplitSlot(sepInfo.mSlotId),
        mSeparatorSize(sepInfo.mSize),
        mSeparatorTruncated(sepInfo.trunc) {
  }

  std::unique_ptr<rapidjson::Document> ToJson() override;
};

struct WALInsert : WALPayload {
  uint16_t mKeySize;

  uint16_t mValSize;

  uint8_t mPayload[];

  WALInsert(Slice key, Slice val)
      : WALPayload(Type::kWalInsert),
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

  std::unique_ptr<rapidjson::Document> ToJson() override;
};

struct WALTxInsert : WALPayload {
  uint16_t mKeySize;

  uint16_t mValSize;

  WORKERID mPrevWorkerId;

  TXID mPrevTxId;

  COMMANDID mPrevCommandId;

  uint8_t mPayload[];

  WALTxInsert(Slice key, Slice val, WORKERID prevWorkerId, TXID prevTxId,
              COMMANDID prevCommandId)
      : WALPayload(Type::kWalTxInsert),
        mKeySize(key.size()),
        mValSize(val.size()),
        mPrevWorkerId(prevWorkerId),
        mPrevTxId(prevTxId),
        mPrevCommandId(prevCommandId) {
    std::memcpy(mPayload, key.data(), mKeySize);
    std::memcpy(mPayload + mKeySize, val.data(), mValSize);
  }

  inline Slice GetKey() const {
    return Slice(mPayload, mKeySize);
  }

  inline Slice GetVal() const {
    return Slice(mPayload + mKeySize, mValSize);
  }

  std::unique_ptr<rapidjson::Document> ToJson() override;
};

struct WalUpdate : WALPayload {
  uint16_t mKeySize;

  uint16_t mDeltaLength;

  uint8_t mPayload[];
};

struct WalTxUpdate : WALPayload {
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
      : WALPayload(Type::kWalTxUpdate),
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

struct WalRemove : WALPayload {
  uint16_t mKeySize;

  uint16_t mValSize;

  uint8_t mPayload[];

  WalRemove(Slice key, Slice val)
      : WALPayload(Type::kWalRemove),
        mKeySize(key.size()),
        mValSize(val.size()) {
    std::memcpy(mPayload, key.data(), key.size());
    std::memcpy(mPayload + key.size(), val.data(), val.size());
  }
};

struct WalTxRemove : WALPayload {
  uint16_t mKeySize;

  uint16_t mValSize;

  WORKERID mPrevWorkerId;

  TXID mPrevTxId;

  COMMANDID mPrevCommandId;

  uint8_t mPayload[];

  WalTxRemove(Slice key, Slice val, WORKERID prevWorkerId, TXID prevTxId,
              COMMANDID prevCommandId)
      : WALPayload(Type::kWalTxRemove),
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

//------------------------------------------------------------------------------
// WALPayload
//------------------------------------------------------------------------------
inline std::unique_ptr<rapidjson::Document> WALPayload::ToJson() {
  auto doc = std::make_unique<rapidjson::Document>();
  doc->SetObject();

  // type
  {
    auto typeName = WalLogTypeName(mType);
    rapidjson::Value member;
    member.SetString(typeName.data(), typeName.size(), doc->GetAllocator());
    doc->AddMember("mType", member, doc->GetAllocator());
  }

  return doc;
}

inline std::string WALPayload::WalLogTypeName(Type type) {
  switch (type) {
    DO_WITH_TYPES(TYPE_NAME);
  default:
    return "Unknown WAL log type";
  }
}

//------------------------------------------------------------------------------
// WALInitPage
//------------------------------------------------------------------------------
inline std::unique_ptr<rapidjson::Document> WALInitPage::ToJson() {
  auto doc = WALPayload::ToJson();

  // mTreeId
  {
    rapidjson::Value member;
    member.SetInt64(mTreeId);
    doc->AddMember("mTreeId", member, doc->GetAllocator());
  }

  // mIsLeaf
  {
    rapidjson::Value member;
    member.SetBool(mIsLeaf);
    doc->AddMember("mIsLeaf", member, doc->GetAllocator());
  }

  return doc;
}

//------------------------------------------------------------------------------
// WalSplitRoot
//------------------------------------------------------------------------------
inline std::unique_ptr<rapidjson::Document> WalSplitRoot::ToJson() {
  auto doc = WALPayload::ToJson();

  {
    rapidjson::Value member;
    member.SetUint64(mNewLeft);
    doc->AddMember("mNewLeft", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetUint64(mNewRoot);
    doc->AddMember("mNewRoot", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetUint64(mMetaNode);
    doc->AddMember("mMetaNode", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetUint64(mSplitSlot);
    doc->AddMember("mSplitSlot", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetUint64(mSeparatorSize);
    doc->AddMember("mSeparatorSize", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetBool(mSeparatorTruncated);
    doc->AddMember("mSeparatorTruncated", member, doc->GetAllocator());
  }

  return doc;
}

//------------------------------------------------------------------------------
// WalSplitNonRoot
//------------------------------------------------------------------------------
inline std::unique_ptr<rapidjson::Document> WalSplitNonRoot::ToJson() {
  auto doc = WALPayload::ToJson();

  // mParentPageId
  {
    rapidjson::Value member;
    member.SetUint64(mParentPageId);
    doc->AddMember("mParentPageId", member, doc->GetAllocator());
  }

  // mNewLeft
  {
    rapidjson::Value member;
    member.SetUint64(mNewLeft);
    doc->AddMember("mNewLeft", member, doc->GetAllocator());
  }

  return doc;
}

//------------------------------------------------------------------------------
// WALInsert
//------------------------------------------------------------------------------
inline std::unique_ptr<rapidjson::Document> WALInsert::ToJson() {
  auto doc = WALPayload::ToJson();

  // mKeySize
  {
    rapidjson::Value member;
    member.SetUint64(mKeySize);
    doc->AddMember("mKeySize", member, doc->GetAllocator());
  }

  // mValSize
  {
    rapidjson::Value member;
    member.SetUint64(mValSize);
    doc->AddMember("mValSize", member, doc->GetAllocator());
  }

  // key in payload
  {
    rapidjson::Value member;
    auto key = GetKey();
    member.SetString((char*)key.data(), key.size(), doc->GetAllocator());
    doc->AddMember("mKey", member, doc->GetAllocator());
  }

  // val in payload
  {
    rapidjson::Value member;
    auto val = GetVal();
    member.SetString((char*)val.data(), val.size(), doc->GetAllocator());
    doc->AddMember("mVal", member, doc->GetAllocator());
  }

  return doc;
}

//------------------------------------------------------------------------------
// WALTxInsert
//------------------------------------------------------------------------------
inline std::unique_ptr<rapidjson::Document> WALTxInsert::ToJson() {
  auto doc = WALPayload::ToJson();

  // mKeySize
  {
    rapidjson::Value member;
    member.SetUint64(mKeySize);
    doc->AddMember("mKeySize", member, doc->GetAllocator());
  }

  // mValSize
  {
    rapidjson::Value member;
    member.SetUint64(mValSize);
    doc->AddMember("mValSize", member, doc->GetAllocator());
  }

  // key in payload
  {
    rapidjson::Value member;
    auto key = GetKey();
    member.SetString((char*)key.data(), key.size(), doc->GetAllocator());
    doc->AddMember("mKey", member, doc->GetAllocator());
  }

  // val in payload
  {
    rapidjson::Value member;
    auto val = GetVal();
    member.SetString((char*)val.data(), val.size(), doc->GetAllocator());
    doc->AddMember("mVal", member, doc->GetAllocator());
  }

  return doc;
}

#undef TYPE_NAME
#undef DECR_TYPE
#undef DO_WITH_TYPES

} // namespace btree
} // namespace storage
} // namespace leanstore
