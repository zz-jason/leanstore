#include "leanstore/btree/core/b_tree_node.hpp"
#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/concurrency/wal_entry.hpp"
#include "leanstore/utils/json_util.hpp"
#include "leanstore/utils/log.hpp"

#include <sstream>

using leanstore::utils::AddMemberToJson;

namespace leanstore::utils {

const char kCrc32[] = "crc32_";
const char kLsn[] = "lsn_";
const char kSize[] = "size_";
const char kType[] = "type_";
const char kTxId[] = "tx_id_";
const char kWorkerId[] = "worker_id_";
const char kPrevLsn[] = "prev_lsn_";
const char kPsn[] = "psn_";
const char kTreeId[] = "tree_id_";
const char kPageId[] = "page_id_";

/// Convert the object to a JSON document.
template <typename ObjType>
void ToJson(const ObjType* obj [[maybe_unused]], rapidjson::Document* doc [[maybe_unused]]) {
}

/// Convert the object to a JSON document.
template <typename ObjType, typename JsonType, typename JsonAllocator>
void ToJson(ObjType* obj [[maybe_unused]], JsonType* doc [[maybe_unused]],
            JsonAllocator* allocator [[maybe_unused]]) {
}

template <>
inline void ToJson(const leanstore::cr::WalEntry* entry, rapidjson::Document* doc) {
  // type
  {
    auto type_name = entry->TypeName();
    rapidjson::Value member;
    member.SetString(type_name.data(), type_name.size(), doc->GetAllocator());
    doc->AddMember(kType, std::move(member), doc->GetAllocator());
  }
}

template <>
inline void ToJson(const leanstore::cr::WalTxAbort* entry, rapidjson::Document* doc) {
  // base
  ToJson(static_cast<const leanstore::cr::WalEntry*>(entry), doc);

  // txid
  {
    rapidjson::Value member;
    member.SetUint64(entry->tx_id_);
    doc->AddMember(kTxId, member, doc->GetAllocator());
  }
}

template <>
inline void ToJson(const leanstore::cr::WalTxFinish* entry, rapidjson::Document* doc) {
  // base
  ToJson(static_cast<const leanstore::cr::WalEntry*>(entry), doc);

  // txid
  {
    rapidjson::Value member;
    member.SetUint64(entry->tx_id_);
    doc->AddMember(kTxId, member, doc->GetAllocator());
  }
}

template <>
inline void ToJson(const leanstore::cr::WalCarriageReturn* entry, rapidjson::Document* doc) {
  // base
  ToJson(static_cast<const leanstore::cr::WalEntry*>(entry), doc);

  // size
  {
    rapidjson::Value member;
    member.SetUint64(entry->size_);
    doc->AddMember(kTxId, member, doc->GetAllocator());
  }
}

template <>
inline void ToJson(const leanstore::cr::WalEntryComplex* obj, rapidjson::Document* doc) {
  // base
  ToJson(static_cast<const leanstore::cr::WalEntry*>(obj), doc);

  // crc
  {
    rapidjson::Value member;
    member.SetUint(obj->crc32_);
    doc->AddMember(kCrc32, member, doc->GetAllocator());
  }

  // lsn
  {
    rapidjson::Value member;
    member.SetUint64(obj->lsn_);
    doc->AddMember(kLsn, member, doc->GetAllocator());
  }

  // size
  {
    rapidjson::Value member;
    member.SetUint64(obj->size_);
    doc->AddMember(kSize, member, doc->GetAllocator());
  }

  // txId
  {
    rapidjson::Value member;
    member.SetUint64(obj->tx_id_);
    doc->AddMember(kTxId, member, doc->GetAllocator());
  }

  // workerId
  {
    rapidjson::Value member;
    member.SetUint64(obj->worker_id_);
    doc->AddMember(kWorkerId, member, doc->GetAllocator());
  }

  // prev_lsn_in_tx
  {
    rapidjson::Value member;
    member.SetUint64(obj->prev_lsn_);
    doc->AddMember(kPrevLsn, member, doc->GetAllocator());
  }

  // psn
  {
    rapidjson::Value member;
    member.SetUint64(obj->psn_);
    doc->AddMember(kPsn, member, doc->GetAllocator());
  }

  // treeId
  {
    rapidjson::Value member;
    member.SetInt64(obj->tree_id_);
    doc->AddMember(kTreeId, member, doc->GetAllocator());
  }

  // pageId
  {
    rapidjson::Value member;
    member.SetUint64(obj->page_id_);
    doc->AddMember(kPageId, member, doc->GetAllocator());
  }
}

/// Convert BufferFrame to JSON
template <>
inline void ToJson(leanstore::storage::BufferFrame* obj, rapidjson::Value* doc,
                   rapidjson::Value::AllocatorType* allocator) {
  LS_DCHECK(doc->IsObject());

  // header
  rapidjson::Value header_obj(rapidjson::kObjectType);
  {
    // write the memory address of the buffer frame
    rapidjson::Value member;
    std::stringstream ss;
    ss << reinterpret_cast<void*>(obj);
    auto hex_str = ss.str();
    member.SetString(hex_str.data(), hex_str.size(), *allocator);
    header_obj.AddMember("address_", member, *allocator);
  }

  {
    auto state_str = obj->header_.StateString();
    rapidjson::Value member;
    member.SetString(state_str.data(), state_str.size(), *allocator);
    header_obj.AddMember("state_", member, *allocator);
  }

  {
    rapidjson::Value member;
    member.SetBool(obj->header_.keep_in_memory_);
    header_obj.AddMember("keep_in_memory_", member, *allocator);
  }

  {
    rapidjson::Value member;
    member.SetUint64(obj->header_.page_id_);
    header_obj.AddMember("page_id_", member, *allocator);
  }

  {
    rapidjson::Value member;
    member.SetUint64(obj->header_.last_writer_worker_);
    header_obj.AddMember("last_writer_worker_", member, *allocator);
  }

  {
    rapidjson::Value member;
    member.SetUint64(obj->header_.flushed_psn_);
    header_obj.AddMember("flushed_psn_", member, *allocator);
  }

  {
    rapidjson::Value member;
    member.SetBool(obj->header_.is_being_written_back_);
    header_obj.AddMember("is_being_written_back_", member, *allocator);
  }

  doc->AddMember("header", header_obj, *allocator);

  // page without payload
  rapidjson::Value page_meta_obj(rapidjson::kObjectType);
  {
    rapidjson::Value member;
    member.SetUint64(obj->page_.gsn_);
    page_meta_obj.AddMember("gsn_", member, *allocator);
  }
  {
    rapidjson::Value member;
    member.SetUint64(obj->page_.psn_);
    page_meta_obj.AddMember("psn_", member, *allocator);
  }
  {
    rapidjson::Value member;
    member.SetUint64(obj->page_.btree_id_);
    page_meta_obj.AddMember("btree_id_", member, *allocator);
  }
  {
    rapidjson::Value member;
    member.SetUint64(obj->page_.magic_debugging_);
    page_meta_obj.AddMember("magic_debugging_", member, *allocator);
  }
  doc->AddMember("pageWithoutPayload", page_meta_obj, *allocator);
}

/// Convert BufferFrame to JSON
template <>
inline void ToJson(leanstore::storage::btree::BTreeNode* obj, rapidjson::Value* doc,
                   rapidjson::Value::AllocatorType* allocator) {
  LS_DCHECK(doc->IsObject());

  auto lower_fence = obj->GetLowerFence();
  if (lower_fence.size() == 0) {
    AddMemberToJson(doc, *allocator, "lower_fence_", "-inf");
  } else {
    AddMemberToJson(doc, *allocator, "lower_fence_", lower_fence);
  }

  auto upper_fence = obj->GetUpperFence();
  if (upper_fence.size() == 0) {
    AddMemberToJson(doc, *allocator, "upper_fence_", "+inf");
  } else {
    AddMemberToJson(doc, *allocator, "upper_fence_", upper_fence);
  }

  AddMemberToJson(doc, *allocator, "num_slots_", obj->num_slots_);
  AddMemberToJson(doc, *allocator, "is_leaf_", obj->is_leaf_);
  AddMemberToJson(doc, *allocator, "space_used_", obj->space_used_);
  AddMemberToJson(doc, *allocator, "data_offset_", obj->data_offset_);
  AddMemberToJson(doc, *allocator, "prefix_size_", obj->prefix_size_);

  // hints
  {
    rapidjson::Value member_array(rapidjson::kArrayType);
    for (auto i = 0; i < leanstore::storage::btree::BTreeNode::sHintCount; ++i) {
      rapidjson::Value hint_json;
      hint_json.SetUint64(obj->hint_[i]);
      member_array.PushBack(hint_json, *allocator);
    }
    doc->AddMember("hints_", member_array, *allocator);
  }

  AddMemberToJson(doc, *allocator, "has_garbage_", obj->has_garbage_);

  // slots
  {
    rapidjson::Value member_array(rapidjson::kArrayType);
    for (auto i = 0; i < obj->num_slots_; ++i) {
      rapidjson::Value array_element(rapidjson::kObjectType);
      AddMemberToJson(&array_element, *allocator, "offset_",
                      static_cast<uint64_t>(obj->slot_[i].offset_));
      AddMemberToJson(&array_element, *allocator, "key_len_",
                      static_cast<uint64_t>(obj->slot_[i].key_size_without_prefix_));
      AddMemberToJson(&array_element, *allocator, "key_", obj->KeyWithoutPrefix(i));
      AddMemberToJson(&array_element, *allocator, "payload_len_",
                      static_cast<uint64_t>(obj->slot_[i].val_size_));
      AddMemberToJson(&array_element, *allocator, "head_",
                      static_cast<uint64_t>(obj->slot_[i].head_));
      member_array.PushBack(array_element, *allocator);
    }
    doc->AddMember("slots_", member_array, *allocator);
  }
}

/// Convert the object to a JSON string.
template <typename ObjType>
inline std::string ToJsonString(const ObjType* entry [[maybe_unused]]) {
  rapidjson::Document doc(rapidjson::kObjectType);
  ToJson(entry, &doc);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

} // namespace leanstore::utils