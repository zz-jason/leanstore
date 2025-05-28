
#include "btree/core/b_tree_wal_payload.hpp"

#include "leanstore/utils/log.hpp"

#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <string>

namespace leanstore::storage::btree {

void WalPayload::ToJson(const WalPayload* wal, rapidjson::Document* doc) {
  // collect common fields
  to_json(wal, doc);

  // collect the rest fields
  switch (wal->type_) {
  case Type::kWalInsert: {
    return to_json(reinterpret_cast<const WalInsert*>(wal), doc);
  }
  case Type::kWalTxInsert: {
    return to_json(reinterpret_cast<const WalTxInsert*>(wal), doc);
  }
  case Type::kWalUpdate: {
    return to_json(reinterpret_cast<const WalUpdate*>(wal), doc);
  }
  case Type::kWalTxUpdate: {
    return to_json(reinterpret_cast<const WalTxUpdate*>(wal), doc);
  }
  case Type::kWalRemove: {
    return to_json(reinterpret_cast<const WalRemove*>(wal), doc);
  }
  case Type::kWalTxRemove: {
    return to_json(reinterpret_cast<const WalTxRemove*>(wal), doc);
  }
  case Type::kWalInitPage: {
    return to_json(reinterpret_cast<const WalInitPage*>(wal), doc);
  }
  case Type::kWalSplitRoot: {
    return to_json(reinterpret_cast<const WalSplitRoot*>(wal), doc);
  }
  case Type::kWalSplitNonRoot: {
    return to_json(reinterpret_cast<const WalSplitNonRoot*>(wal), doc);
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

void WalPayload::to_json(const WalPayload* wal, rapidjson::Document* doc) {
  // type
  auto type_name = wal->WalLogTypeName(wal->type_);
  rapidjson::Value member;
  member.SetString(type_name.data(), type_name.size(), doc->GetAllocator());
  doc->AddMember("type_", member, doc->GetAllocator());
}

void WalPayload::to_json(const WalInsert* wal, rapidjson::Document* doc) {
  // key_size_
  {
    rapidjson::Value member;
    member.SetUint64(wal->key_size_);
    doc->AddMember("key_size_", member, doc->GetAllocator());
  }

  // val_size_
  {
    rapidjson::Value member;
    member.SetUint64(wal->val_size_);
    doc->AddMember("val_size_", member, doc->GetAllocator());
  }

  // key in payload
  {
    rapidjson::Value member;
    auto key = wal->GetKey();
    member.SetString((char*)key.data(), key.size(), doc->GetAllocator());
    doc->AddMember("key_", member, doc->GetAllocator());
  }

  // val in payload
  {
    rapidjson::Value member;
    auto val = wal->GetVal();
    member.SetString((char*)val.data(), val.size(), doc->GetAllocator());
    doc->AddMember("val_", member, doc->GetAllocator());
  }
}

void WalPayload::to_json(const WalTxInsert* wal [[maybe_unused]],
                         rapidjson::Document* doc [[maybe_unused]]) {
  // key_size_
  {
    rapidjson::Value member;
    member.SetUint64(wal->key_size_);
    doc->AddMember("key_size_", member, doc->GetAllocator());
  }

  // val_size_
  {
    rapidjson::Value member;
    member.SetUint64(wal->val_size_);
    doc->AddMember("val_size_", member, doc->GetAllocator());
  }

  // key in payload
  {
    rapidjson::Value member;
    auto key = wal->GetKey();
    member.SetString((char*)key.data(), key.size(), doc->GetAllocator());
    doc->AddMember("key_", member, doc->GetAllocator());
  }

  // val in payload
  {
    rapidjson::Value member;
    auto val = wal->GetVal();
    member.SetString((char*)val.data(), val.size(), doc->GetAllocator());
    doc->AddMember("val_", member, doc->GetAllocator());
  }
}

void WalPayload::to_json(const WalUpdate* wal [[maybe_unused]],
                         rapidjson::Document* doc [[maybe_unused]]) {
  Log::Warn("toJson for WalUpdate not implemented");
  rapidjson::Value member;
  member.SetString("Not implemented", doc->GetAllocator());
  doc->AddMember("val_", member, doc->GetAllocator());
}

void WalPayload::to_json(const WalTxUpdate* wal [[maybe_unused]],
                         rapidjson::Document* doc [[maybe_unused]]) {
  Log::Warn("toJson for WalTxUpdate not implemented");
  rapidjson::Value member;
  member.SetString("Not implemented", doc->GetAllocator());
  doc->AddMember("val_", member, doc->GetAllocator());
}

void WalPayload::to_json(const WalRemove* wal [[maybe_unused]],
                         rapidjson::Document* doc [[maybe_unused]]) {
  Log::Warn("toJson for WalRemove not implemented");
  rapidjson::Value member;
  member.SetString("Not implemented", doc->GetAllocator());
  doc->AddMember("val_", member, doc->GetAllocator());
}

void WalPayload::to_json(const WalTxRemove* wal [[maybe_unused]],
                         rapidjson::Document* doc [[maybe_unused]]) {
  Log::Warn("toJson for WalTxRemove not implemented");
  rapidjson::Value member;
  member.SetString("Not implemented", doc->GetAllocator());
  doc->AddMember("val_", member, doc->GetAllocator());
}

void WalPayload::to_json(const WalInitPage* wal, rapidjson::Document* doc) {
  // sys_tx_id_
  {
    rapidjson::Value member;
    member.SetInt64(wal->sys_tx_id_);
    doc->AddMember("sys_tx_id_", member, doc->GetAllocator());
  }

  // tree_id_
  {
    rapidjson::Value member;
    member.SetInt64(wal->tree_id_);
    doc->AddMember("tree_id_", member, doc->GetAllocator());
  }

  // is_leaf_
  {
    rapidjson::Value member;
    member.SetBool(wal->is_leaf_);
    doc->AddMember("is_leaf_", member, doc->GetAllocator());
  }
}

void WalPayload::to_json(const WalSplitRoot* wal, rapidjson::Document* doc) {
  {
    rapidjson::Value member;
    member.SetUint64(wal->sys_tx_id_);
    doc->AddMember("sys_tx_id_", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetUint64(wal->new_left_);
    doc->AddMember("new_left_", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetUint64(wal->new_root_);
    doc->AddMember("new_root_", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetUint64(wal->meta_node_);
    doc->AddMember("meta_node_", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetUint64(wal->split_slot_);
    doc->AddMember("split_slot_", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetUint64(wal->separator_size_);
    doc->AddMember("separator_size_", member, doc->GetAllocator());
  }

  {
    rapidjson::Value member;
    member.SetBool(wal->separator_truncated_);
    doc->AddMember("separator_truncated_", member, doc->GetAllocator());
  }
}

void WalPayload::to_json(const WalSplitNonRoot* wal, rapidjson::Document* doc) {
  {
    rapidjson::Value member;
    member.SetUint64(wal->sys_tx_id_);
    doc->AddMember("sys_tx_id_", member, doc->GetAllocator());
  }

  // parent_page_id_
  {
    rapidjson::Value member;
    member.SetUint64(wal->parent_page_id_);
    doc->AddMember("parent_page_id_", member, doc->GetAllocator());
  }

  // new_left_
  {
    rapidjson::Value member;
    member.SetUint64(wal->new_left_);
    doc->AddMember("new_left_", member, doc->GetAllocator());
  }
}
} // namespace leanstore::storage::btree