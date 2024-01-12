#pragma once

#include "KVInterface.hpp"
#include "shared-headers/Units.hpp"

#include <rapidjson/document.h>

#include <string>

namespace leanstore {
namespace storage {
namespace btree {

#define DO_WITH_TYPES(ACTION, ...)                                             \
  ACTION(WALInsert, 1, "WALInsert", __VA_ARGS__)                               \
  ACTION(WALUpdate, 2, "WALUpdate", __VA_ARGS__)                               \
  ACTION(WALRemove, 3, "WALRemove", __VA_ARGS__)                               \
  ACTION(WALAfterBeforeImage, 4, "WALAfterBeforeImage", __VA_ARGS__)           \
  ACTION(WALAfterImage, 5, "WALAfterImage", __VA_ARGS__)                       \
  ACTION(WALLogicalSplit, 10, "WALLogicalSplit", __VA_ARGS__)                  \
  ACTION(WALInitPage, 11, "WALInitPage", __VA_ARGS__)                          \
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
  TYPE type = TYPE::WALUndefined;

public:
  WALPayload() = default;

  WALPayload(TYPE type) : type(type) {
  }

  virtual std::unique_ptr<rapidjson::Document> ToJSON() {
    auto doc = std::make_unique<rapidjson::Document>();
    doc->SetObject();

    // type
    {
      auto typeName = WalLogTypeName(type);
      rapidjson::Value member;
      member.SetString(typeName.data(), typeName.size(), doc->GetAllocator());
      doc->AddMember("type", member, doc->GetAllocator());
    }

    return doc;
  }

  inline std::string WalLogTypeName(TYPE type) {
    switch (type) {
      DO_WITH_TYPES(TYPE_NAME);
    default:
      return "Unknown WAL log type";
    }
  }
};

#undef TYPE_NAME
#undef DECR_TYPE

class WALInitPage : WALPayload {
public:
  TREEID mTreeId;
  bool mIsLeaf;

public:
  WALInitPage(TREEID treeId, bool isLeaf)
      : WALPayload(TYPE::WALInitPage),
        mTreeId(treeId),
        mIsLeaf(isLeaf) {
  }

public:
  virtual std::unique_ptr<rapidjson::Document> ToJSON() override {
    auto doc = WALPayload::ToJSON();

    // mTreeId
    {
      rapidjson::Value member;
      member.SetInt64(mTreeId);
      doc->AddMember("treeId", member, doc->GetAllocator());
    }

    return doc;
  }
};

struct WALLogicalSplit : WALPayload {
  PID parent_pid = -1;
  PID left_pid = -1;
  PID right_pid = -1;

  WALLogicalSplit() : WALPayload(TYPE::WALLogicalSplit) {
  }

  WALLogicalSplit(PID parent, PID lhs, PID rhs)
      : WALPayload(TYPE::WALLogicalSplit),
        parent_pid(parent),
        left_pid(lhs),
        right_pid(rhs) {
  }

  virtual std::unique_ptr<rapidjson::Document> ToJSON() override {
    auto doc = WALPayload::ToJSON();

    // parent_pid
    {
      rapidjson::Value member;
      member.SetUint64(parent_pid);
      doc->AddMember("parent_pid", member, doc->GetAllocator());
    }

    // left_pid
    {
      rapidjson::Value member;
      member.SetUint64(left_pid);
      doc->AddMember("left_pid", member, doc->GetAllocator());
    }

    // right_pid
    {
      rapidjson::Value member;
      member.SetUint64(right_pid);
      doc->AddMember("right_pid", member, doc->GetAllocator());
    }

    return doc;
  }
};

struct WALBeforeAfterImage : WALPayload {
  u16 image_size;
  u8 payload[];
};

struct WALAfterImage : WALPayload {
  u16 image_size;
  u8 payload[];
};

struct WALInsert : WALPayload {
  u16 mKeySize;
  u16 mValSize;
  u8 payload[];

  WALInsert(Slice key, Slice val)
      : WALPayload(TYPE::WALInsert),
        mKeySize(key.size()),
        mValSize(val.size()) {
    std::memcpy(payload, key.data(), mKeySize);
    std::memcpy(payload + mKeySize, val.data(), mValSize);
  }

  Slice GetKey() {
    return Slice(payload, mKeySize);
  }

  Slice GetVal() {
    return Slice(payload + mKeySize, mValSize);
  }

  virtual std::unique_ptr<rapidjson::Document> ToJSON() override {
    auto doc = WALPayload::ToJSON();

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

    // payload
    {
      rapidjson::Value member;
      member.SetString(reinterpret_cast<const char*>(payload),
                       mKeySize + mValSize, doc->GetAllocator());
      doc->AddMember("payload", member, doc->GetAllocator());
    }

    return doc;
  }
};

// WAL for BTreeVI
struct WALUpdateSSIP : WALPayload {
  u16 mKeySize;
  u64 delta_length;
  WORKERID mPrevWorkerId;
  TXID mPrevTxId;
  COMMANDID mPrevCommandId;
  u8 payload[];

  WALUpdateSSIP(Slice key, UpdateDesc& updateDesc, u64 deltaSize,
                WORKERID prevWorkerId, TXID prevTxId, COMMANDID prevCommandId)
      : WALPayload(TYPE::WALUpdate),
        mKeySize(key.size()),
        delta_length(deltaSize),
        mPrevWorkerId(prevWorkerId),
        mPrevTxId(prevTxId),
        mPrevCommandId(prevCommandId) {
    std::memcpy(payload, key.data(), key.size());
    std::memcpy(payload + key.size(), &updateDesc, updateDesc.size());
  }
};

} // namespace btree
} // namespace storage
} // namespace leanstore
