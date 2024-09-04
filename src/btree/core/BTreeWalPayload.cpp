
#include "btree/core/BTreeWalPayload.hpp"

#include "leanstore/utils/Log.hpp"

#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <string>

namespace leanstore::storage::btree {

void WalPayload::ToJson(const WalPayload* wal, rapidjson::Document* doc) {
  // collect common fields
  toJson(wal, doc);

  // collect the rest fields
  switch (wal->mType) {
  case Type::kWalInsert: {
    return toJson(reinterpret_cast<const WalInsert*>(wal), doc);
  }
  case Type::kWalTxInsert: {
    return toJson(reinterpret_cast<const WalTxInsert*>(wal), doc);
  }
  case Type::kWalUpdate: {
    return toJson(reinterpret_cast<const WalUpdate*>(wal), doc);
  }
  case Type::kWalTxUpdate: {
    return toJson(reinterpret_cast<const WalTxUpdate*>(wal), doc);
  }
  case Type::kWalRemove: {
    return toJson(reinterpret_cast<const WalRemove*>(wal), doc);
  }
  case Type::kWalTxRemove: {
    return toJson(reinterpret_cast<const WalTxRemove*>(wal), doc);
  }
  case Type::kWalInitPage: {
    return toJson(reinterpret_cast<const WalInitPage*>(wal), doc);
  }
  case Type::kWalSplitRoot: {
    return toJson(reinterpret_cast<const WalSplitRoot*>(wal), doc);
  }
  case Type::kWalSplitNonRoot: {
    return toJson(reinterpret_cast<const WalSplitNonRoot*>(wal), doc);
  }
  default:
    break;
  }
}

std::string WalPayload::ToJsonString(const WalPayload* wal [[maybe_unused]]) {
  rapidjson::Document doc(rapidjson::kObjectType);
  ToJson(wal, &doc);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

void WalPayload::toJson(const WalPayload* wal, rapidjson::Document* doc) {
  // type
  auto typeName = wal->WalLogTypeName(wal->mType);
  rapidjson::Value member;
  member.SetString(typeName.data(), typeName.size(), doc->GetAllocator());
  doc->AddMember("mType", member, doc->GetAllocator());
}

void WalPayload::toJson(const WalInsert* wal, rapidjson::Document* doc) {
  // mKeySize
  {
    rapidjson::Value member;
    member.SetUint64(wal->mKeySize);
    doc->AddMember("mKeySize", member, doc->GetAllocator());
  }

  // mValSize
  {
    rapidjson::Value member;
    member.SetUint64(wal->mValSize);
    doc->AddMember("mValSize", member, doc->GetAllocator());
  }

  // key in payload
  {
    rapidjson::Value member;
    auto key = wal->GetKey();
    member.SetString((char*)key.data(), key.size(), doc->GetAllocator());
    doc->AddMember("mKey", member, doc->GetAllocator());
  }

  // val in payload
  {
    rapidjson::Value member;
    auto val = wal->GetVal();
    member.SetString((char*)val.data(), val.size(), doc->GetAllocator());
    doc->AddMember("mVal", member, doc->GetAllocator());
  }
}

void WalPayload::toJson(const WalTxInsert* wal [[maybe_unused]],
                        rapidjson::Document* doc [[maybe_unused]]) {
  // mKeySize
  {
    rapidjson::Value member;
    member.SetUint64(wal->mKeySize);
    doc->AddMember("mKeySize", member, doc->GetAllocator());
  }

  // mValSize
  {
    rapidjson::Value member;
    member.SetUint64(wal->mValSize);
    doc->AddMember("mValSize", member, doc->GetAllocator());
  }

  // key in payload
  {
    rapidjson::Value member;
    auto key = wal->GetKey();
    member.SetString((char*)key.data(), key.size(), doc->GetAllocator());
    doc->AddMember("mKey", member, doc->GetAllocator());
  }

  // val in payload
  {
    rapidjson::Value member;
    auto val = wal->GetVal();
    member.SetString((char*)val.data(), val.size(), doc->GetAllocator());
    doc->AddMember("mVal", member, doc->GetAllocator());
  }
}

void WalPayload::toJson(const WalUpdate* wal [[maybe_unused]],
                        rapidjson::Document* doc [[maybe_unused]]) {
  Log::Warn("toJson for WalUpdate not implemented");
  rapidjson::Value member;
  member.SetString("Not implemented", doc->GetAllocator());
  doc->AddMember("mVal", member, doc->GetAllocator());
}

void WalPayload::toJson(const WalTxUpdate* wal [[maybe_unused]],
                        rapidjson::Document* doc [[maybe_unused]]) {
  Log::Warn("toJson for WalTxUpdate not implemented");
  rapidjson::Value member;
  member.SetString("Not implemented", doc->GetAllocator());
  doc->AddMember("mVal", member, doc->GetAllocator());
}

void WalPayload::toJson(const WalRemove* wal [[maybe_unused]],
                        rapidjson::Document* doc [[maybe_unused]]) {
  Log::Warn("toJson for WalRemove not implemented");
  rapidjson::Value member;
  member.SetString("Not implemented", doc->GetAllocator());
  doc->AddMember("mVal", member, doc->GetAllocator());
}

void WalPayload::toJson(const WalTxRemove* wal [[maybe_unused]],
                        rapidjson::Document* doc [[maybe_unused]]) {
  Log::Warn("toJson for WalTxRemove not implemented");
  rapidjson::Value member;
  member.SetString("Not implemented", doc->GetAllocator());
  doc->AddMember("mVal", member, doc->GetAllocator());
}

void WalPayload::toJson(const WalInitPage* wal, rapidjson::Document* doc) {
  // mSysTxId
  {
    rapidjson::Value member;
    member.SetInt64(wal->mSysTxId);
    doc->AddMember("mSysTxId", member, doc->GetAllocator());
  }

  // mTreeId
  {
    rapidjson::Value member;
    member.SetInt64(wal->mTreeId);
    doc->AddMember("mTreeId", member, doc->GetAllocator());
  }

  // mIsLeaf
  {
    rapidjson::Value member;
    member.SetBool(wal->mIsLeaf);
    doc->AddMember("mIsLeaf", member, doc->GetAllocator());
  }
}

void WalPayload::toJson(const WalSplitRoot* wal, rapidjson::Document* doc) {
  {
    rapidjson::Value member;
    member.SetUint64(wal->mSysTxId);
    doc->AddMember("mSysTxId", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetUint64(wal->mNewLeft);
    doc->AddMember("mNewLeft", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetUint64(wal->mNewRoot);
    doc->AddMember("mNewRoot", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetUint64(wal->mMetaNode);
    doc->AddMember("mMetaNode", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetUint64(wal->mSplitSlot);
    doc->AddMember("mSplitSlot", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetUint64(wal->mSeparatorSize);
    doc->AddMember("mSeparatorSize", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetBool(wal->mSeparatorTruncated);
    doc->AddMember("mSeparatorTruncated", member, doc->GetAllocator());
  }
}

void WalPayload::toJson(const WalSplitNonRoot* wal, rapidjson::Document* doc) {
  {
    rapidjson::Value member;
    member.SetUint64(wal->mSysTxId);
    doc->AddMember("mSysTxId", member, doc->GetAllocator());
  }

  // mParentPageId
  {
    rapidjson::Value member;
    member.SetUint64(wal->mParentPageId);
    doc->AddMember("mParentPageId", member, doc->GetAllocator());
  }

  // mNewLeft
  {
    rapidjson::Value member;
    member.SetUint64(wal->mNewLeft);
    doc->AddMember("mNewLeft", member, doc->GetAllocator());
  }
}
} // namespace leanstore::storage::btree