#pragma once

#include "leanstore/btree/core/b_tree_node.hpp"
#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/slice.hpp"
#include "utils/json.hpp"

namespace leanstore::utils {

constexpr char kState[] = "state";
constexpr char kKeepInMemory[] = "keep_in_memory";
constexpr char kPageId[] = "page_id";
constexpr char kLastWriterWorker[] = "last_writer_worker";
constexpr char kFlushedPsn[] = "flushed_psn";
constexpr char kIsBeingWrittenBack[] = "is_being_written_back";
constexpr char kHeader[] = "header";

constexpr char kPsn[] = "psn";
constexpr char kBtreeId[] = "btree_id";
constexpr char kMagicDebugging[] = "magic_debugging";
constexpr char kPageWithoutPayload[] = "page_without_payload";

constexpr char kLowerFence[] = "lower_fence";
constexpr char kUpperFence[] = "upper_fence";
constexpr char kNumSlots[] = "num_slots";
constexpr char kIsLeaf[] = "is_leaf";
constexpr char kSpaceUsed[] = "space_used";
constexpr char kDataOffset[] = "data_offset";
constexpr char kPrefixSize[] = "prefix_size";
constexpr char kHints[] = "hints";
constexpr char kHasGarbage[] = "has_garbage";
constexpr char kSlots[] = "slots";
constexpr char kSlotOffset[] = "offset";
constexpr char kSlotKeyLen[] = "key_len";
constexpr char kSlotKey[] = "key";
constexpr char kSlotPayloadLen[] = "payload_len";
constexpr char kSlotHead[] = "head";
constexpr char kInfNegative[] = "-INF";
constexpr char kInfPositive[] = "+INF";

constexpr char kCrc32[] = "crc32";
constexpr char kLsn[] = "lsn";
constexpr char kSize[] = "size";
constexpr char kType[] = "type";
constexpr char kTxId[] = "tx_id";
constexpr char kWorkerId[] = "worker_id";
constexpr char kPrevLsn[] = "prev_lsn";
constexpr char kTreeId[] = "tree_id";

constexpr char kKeySize[] = "key_size";
constexpr char kValSize[] = "val_size";
constexpr char kKey[] = "key";
constexpr char kVal[] = "val";
constexpr char kSysTxId[] = "sys_tx_id";
constexpr char kNewLeft[] = "new_left";
constexpr char kNewRoot[] = "new_root";
constexpr char kMetaNode[] = "meta_node";
constexpr char kSplitSlot[] = "split_slot";
constexpr char kSeparatorSize[] = "separator_size";
constexpr char kSeparatorTruncated[] = "separator_truncated";
constexpr char kParentPageId[] = "parent_page_id";
constexpr char kNotImplemented[] = "NOT IMPLEMENTED";

/// Convert the object to a JSON document.
template <typename ObjType>
void ToJson(const ObjType* obj [[maybe_unused]], utils::JsonObj* json_obj [[maybe_unused]]) {
}

/// Convert the object to a JSON string.
template <typename ObjType>
inline std::string ToJsonString(const ObjType* entry) {
  utils::JsonObj json_obj;
  ToJson(entry, &json_obj);
  return json_obj.Serialize();
}

// -----------------------------------------------------------------------------
// ToJson specializations for BufferFrame and BTreeNode
// -----------------------------------------------------------------------------

inline void ToJson(leanstore::storage::BufferFrame* bf, utils::JsonObj* json_obj) {
  // header
  JsonObj header_obj;
  header_obj.AddString(kState, bf->header_.StateString());
  header_obj.AddBool(kKeepInMemory, bf->header_.keep_in_memory_);
  header_obj.AddInt64(kPageId, bf->header_.page_id_);
  header_obj.AddInt64(kLastWriterWorker, bf->header_.last_writer_worker_);
  header_obj.AddInt64(kFlushedPsn, bf->header_.flushed_psn_);
  header_obj.AddBool(kIsBeingWrittenBack, bf->header_.is_being_written_back_);

  // page without payload
  JsonObj page_meta_obj;
  page_meta_obj.AddInt64(kPsn, bf->page_.psn_);
  page_meta_obj.AddInt64(kBtreeId, bf->page_.btree_id_);
  page_meta_obj.AddInt64(kMagicDebugging, bf->page_.magic_debugging_);

  json_obj->AddJsonObj(kHeader, header_obj);
  json_obj->AddJsonObj(kPageWithoutPayload, page_meta_obj);
}

inline void ToJson(leanstore::storage::btree::BTreeNode* obj, JsonObj* btree_node_json_obj) {
  auto lower_fence = obj->GetLowerFence();
  if (lower_fence.size() == 0) {
    lower_fence = kInfNegative;
  }

  auto upper_fence = obj->GetUpperFence();
  if (upper_fence.size() == 0) {
    upper_fence = kInfPositive;
  }

  utils::JsonArray hints_json_array;
  for (auto i = 0; i < leanstore::storage::btree::BTreeNode::kHintCount; ++i) {
    hints_json_array.AppendInt64(obj->hint_[i]);
  }

  utils::JsonArray slots_json_array;
  for (auto i = 0; i < obj->num_slots_; ++i) {
    auto k = obj->KeyWithoutPrefix(i);
    utils::JsonObj slot_json_obj;
    slot_json_obj.AddInt64(kSlotOffset, obj->slot_[i].offset_);
    slot_json_obj.AddInt64(kSlotKeyLen, obj->slot_[i].key_size_without_prefix_);
    slot_json_obj.AddString(kSlotKey, {reinterpret_cast<const char*>(k.data()), k.size()});
    slot_json_obj.AddInt64(kSlotPayloadLen, obj->slot_[i].val_size_);
    slot_json_obj.AddInt64(kSlotHead, obj->slot_[i].head_);

    slots_json_array.AppendJsonObj(std::move(slot_json_obj));
  }

  btree_node_json_obj->AddString(
      kLowerFence, {reinterpret_cast<const char*>(lower_fence.data()), lower_fence.size()});
  btree_node_json_obj->AddString(
      kUpperFence, {reinterpret_cast<const char*>(upper_fence.data()), upper_fence.size()});
  btree_node_json_obj->AddInt64(kNumSlots, obj->num_slots_);
  btree_node_json_obj->AddBool(kIsLeaf, obj->is_leaf_);
  btree_node_json_obj->AddInt64(kSpaceUsed, obj->space_used_);
  btree_node_json_obj->AddInt64(kDataOffset, obj->data_offset_);
  btree_node_json_obj->AddInt64(kPrefixSize, obj->prefix_size_);
  btree_node_json_obj->AddInt64(kHasGarbage, obj->has_garbage_);
  btree_node_json_obj->AddJsonArray(kHints, std::move(hints_json_array));
  btree_node_json_obj->AddJsonArray(kSlots, std::move(slots_json_array));
}

} // namespace leanstore::utils