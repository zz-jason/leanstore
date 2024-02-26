#pragma once

#include "KVInterface.hpp"
#include "shared-headers/Units.hpp"
#include "storage/btree/core/BTreeNode.hpp"

#include "glog/logging.h"
#include <rapidjson/document.h>

#include <string>

namespace leanstore {
namespace storage {
namespace btree {

#define DO_WITH_TYPES(ACTION, ...)                                             \
  ACTION(kWalInsert, 1, "kWalInsert", __VA_ARGS__)                             \
  ACTION(kWalTxInsert, 2, "kWalTxInsert", __VA_ARGS__)                         \
  ACTION(WALUpdate, 3, "WALUpdate", __VA_ARGS__)                               \
  ACTION(WALTxUpdate, 4, "WALTxUpdate", __VA_ARGS__)                           \
  ACTION(WALRemove, 5, "WALRemove", __VA_ARGS__)                               \
  ACTION(WALTxRemove, 6, "WALTxRemove", __VA_ARGS__)                           \
  ACTION(kWalInitPage, 10, "kWalInitPage", __VA_ARGS__)                        \
  ACTION(kWalSplitRoot, 11, "kWalSplitRoot", __VA_ARGS__)                      \
  ACTION(kWalSplit, 12, "kWalSplit", __VA_ARGS__)                              \
  ACTION(WALUndefined, 100, "WALUndefined", __VA_ARGS__)

#define DECR_TYPE(type, type_value, type_name, ...) type = type_value,
#define TYPE_NAME(type, type_value, type_name, ...)                            \
  case TYPE::type:                                                             \
    return type_name;

class WALPayload {
public:
  enum class TYPE : u8 { DO_WITH_TYPES(DECR_TYPE) };

public:
  /// Type of WALPayload
  TYPE mType = TYPE::WALUndefined;

public:
  WALPayload() = default;

  WALPayload(TYPE type) : mType(type) {
  }

  virtual std::unique_ptr<rapidjson::Document> ToJson();

  inline std::string WalLogTypeName(TYPE type);

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
      : WALPayload(TYPE::kWalInitPage),
        mTreeId(treeId),
        mIsLeaf(isLeaf) {
  }

public:
  std::unique_ptr<rapidjson::Document> ToJson() override;
};

struct WalSplitRoot : WALPayload {
  PID mToSplit;

  u16 mSplitSlot;

  u16 mSeparatorSize;

  bool mSeparatorTruncated;

  WalSplitRoot(PID toSplit, const BTreeNode::SeparatorInfo& sepInfo)
      : WALPayload(TYPE::kWalSplitRoot),
        mToSplit(toSplit),
        mSplitSlot(sepInfo.mSlotId),
        mSeparatorSize(sepInfo.mSize),
        mSeparatorTruncated(sepInfo.trunc) {
  }

  std::unique_ptr<rapidjson::Document> ToJson() override;
};

struct WalSplitNonRoot : WALPayload {
  PID mParentPageId = -1;

  PID mLhsPageId = -1;

  PID mRhsPageId = -1;

  WalSplitNonRoot() : WALPayload(TYPE::kWalSplit) {
  }

  WalSplitNonRoot(PID parent, PID lhs, PID rhs)
      : WALPayload(TYPE::kWalSplit),
        mParentPageId(parent),
        mLhsPageId(lhs),
        mRhsPageId(rhs) {
  }

  std::unique_ptr<rapidjson::Document> ToJson() override;
};

struct WALInsert : WALPayload {
  u16 mKeySize;

  u16 mValSize;

  u8 mPayload[];

  WALInsert(Slice key, Slice val)
      : WALPayload(TYPE::kWalInsert),
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
  u16 mKeySize;

  u16 mValSize;

  WORKERID mPrevWorkerId;

  TXID mPrevTxId;

  COMMANDID mPrevCommandId;

  u8 mPayload[];

  WALTxInsert(Slice key, Slice val, WORKERID prevWorkerId, TXID prevTxId,
              COMMANDID prevCommandId)
      : WALPayload(TYPE::kWalTxInsert),
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

struct WALUpdate : WALPayload {
  u16 mKeySize;

  u16 mDeltaLength;

  u8 mPayload[];
};

struct WALTxUpdate : WALPayload {
  u16 mKeySize;

  u64 mUpdateDescSize;

  u64 mDeltaSize;

  WORKERID mPrevWorkerId;

  TXID mPrevTxId;

  COMMANDID mPrevCommandId;

  // Stores key, UpdateDesc, and Delta in order
  u8 mPayload[];

  WALTxUpdate(Slice key, UpdateDesc& updateDesc, u64 sizeOfUpdateDescAndDelta,
              WORKERID prevWorkerId, TXID prevTxId, COMMANDID prevCommandId)
      : WALPayload(TYPE::WALTxUpdate),
        mKeySize(key.size()),
        mUpdateDescSize(updateDesc.Size()),
        mDeltaSize(sizeOfUpdateDescAndDelta - updateDesc.Size()),
        mPrevWorkerId(prevWorkerId),
        mPrevTxId(prevTxId),
        mPrevCommandId(prevCommandId) {
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
        << "Malformed WALTxUpdate: updateDesc->Size() != mUpdateDescSize"
        << ", updateDesc->Size() = " << updateDesc->Size()
        << ", mUpdateDescSize = " << mUpdateDescSize;
    return updateDesc;
  }

  inline u8* GetDeltaPtr() {
    return mPayload + mKeySize + mUpdateDescSize;
  }

  inline const u8* GetDeltaPtr() const {
    return mPayload + mKeySize + mUpdateDescSize;
  }

  u64 GetDeltaSize() const {
    return mDeltaSize;
  }
};

struct WALRemove : WALPayload {
  u16 mKeySize;

  u16 mValSize;

  u8 mPayload[];

  WALRemove(Slice key, Slice val)
      : WALPayload(TYPE::WALRemove),
        mKeySize(key.size()),
        mValSize(val.size()) {
    std::memcpy(mPayload, key.data(), key.size());
    std::memcpy(mPayload + key.size(), val.data(), val.size());
  }
};

struct WALTxRemove : WALPayload {
  u16 mKeySize;

  u16 mValSize;

  WORKERID mPrevWorkerId;

  TXID mPrevTxId;

  COMMANDID mPrevCommandId;

  u8 mPayload[];

  WALTxRemove(Slice key, Slice val, WORKERID prevWorkerId, TXID prevTxId,
              COMMANDID prevCommandId)
      : WALPayload(TYPE::WALTxRemove),
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

inline std::string WALPayload::WalLogTypeName(TYPE type) {
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
    member.SetUint64(mToSplit);
    doc->AddMember("mToSplit", member, doc->GetAllocator());
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

  // mLhsPageId
  {
    rapidjson::Value member;
    member.SetUint64(mLhsPageId);
    doc->AddMember("mLhsPageId", member, doc->GetAllocator());
  }

  // mRhsPageId
  {
    rapidjson::Value member;
    member.SetUint64(mRhsPageId);
    doc->AddMember("mRhsPageId", member, doc->GetAllocator());
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
