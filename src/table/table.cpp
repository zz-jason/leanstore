#include "leanstore/table/table.hpp"

#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/core/b_tree_generic.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/lean_store.hpp"

#include <cstring>
#include <limits>
#include <utility>
#include <vector>

namespace leanstore {

TableCursor::TableCursor(KVInterface* kv_interface, const TableDefinition& def)
    : kv_interface_(kv_interface),
      codec_(def) {
}

TableCursor::~TableCursor() = default;

bool TableCursor::SeekToFirst() {
  is_valid_ = false;
  current_key_.clear();
  auto rc = kv_interface_->ScanAsc(Slice(), [&](Slice key, Slice val) { return Assign(key, val); });
  return rc == OpCode::kOK && is_valid_;
}

bool TableCursor::SeekToFirstGreaterEqual(const lean_row* key_row) {
  is_valid_ = false;
  auto key_res = codec_.EncodeKey(key_row);
  if (!key_res) {
    return false;
  }
  const auto& key = key_res.value();
  auto rc = kv_interface_->ScanAsc(Slice(reinterpret_cast<const uint8_t*>(key.data()), key.size()),
                                   [&](Slice k, Slice v) { return Assign(k, v); });
  return rc == OpCode::kOK && is_valid_;
}

bool TableCursor::SeekToLast() {
  is_valid_ = false;
  auto rc =
      kv_interface_->ScanDesc(Slice(), [&](Slice key, Slice val) { return Assign(key, val); });
  return rc == OpCode::kOK && is_valid_;
}

bool TableCursor::SeekToLastLessEqual(const lean_row* key_row) {
  is_valid_ = false;
  auto key_res = codec_.EncodeKey(key_row);
  if (!key_res) {
    return false;
  }
  const auto& key = key_res.value();
  auto rc = kv_interface_->ScanDesc(Slice(reinterpret_cast<const uint8_t*>(key.data()), key.size()),
                                    [&](Slice k, Slice v) { return Assign(k, v); });
  return rc == OpCode::kOK && is_valid_;
}

bool TableCursor::Next() {
  if (!is_valid_) {
    return false;
  }
  auto start = current_key_;
  bool skipped = false;
  is_valid_ = false;
  auto rc =
      kv_interface_->ScanAsc(Slice(reinterpret_cast<const uint8_t*>(start.data()), start.size()),
                             [&](Slice key, Slice val) {
                               if (!skipped) {
                                 skipped = true;
                                 if (key.size() == start.size() &&
                                     std::memcmp(key.data(), start.data(), start.size()) == 0) {
                                   return true;
                                 }
                               }
                               return Assign(key, val);
                             });
  return rc == OpCode::kOK && is_valid_;
}

bool TableCursor::Prev() {
  if (!is_valid_) {
    return false;
  }
  auto start = current_key_;
  bool skipped = false;
  is_valid_ = false;
  auto rc =
      kv_interface_->ScanDesc(Slice(reinterpret_cast<const uint8_t*>(start.data()), start.size()),
                              [&](Slice key, Slice val) {
                                if (!skipped) {
                                  skipped = true;
                                  if (key.size() == start.size() &&
                                      std::memcmp(key.data(), start.data(), start.size()) == 0) {
                                    return true;
                                  }
                                }
                                return Assign(key, val);
                              });
  return rc == OpCode::kOK && is_valid_;
}

bool TableCursor::IsValid() const {
  return is_valid_;
}

Result<void> TableCursor::CurrentRow(lean_row* out_row) {
  if (out_row == nullptr || out_row->columns == nullptr) {
    return Error::General("output row null");
  }
  if (!is_valid_) {
    return Error::General("cursor invalid");
  }
  Slice val_slice(reinterpret_cast<const uint8_t*>(current_value_.data()), current_value_.size());
  return codec_.DecodeValue(val_slice, out_row);
}

OpCode TableCursor::RemoveCurrent() {
  if (!is_valid_) {
    return OpCode::kNotFound;
  }
  auto res = kv_interface_->Remove(
      Slice(reinterpret_cast<const uint8_t*>(current_key_.data()), current_key_.size()));
  if (res == OpCode::kOK) {
    is_valid_ = false;
    current_value_.clear();
  }
  return res;
}

OpCode TableCursor::UpdateCurrent(const lean_row* row) {
  if (!is_valid_) {
    return OpCode::kNotFound;
  }

  auto encoded_res = codec_.Encode(row);
  if (!encoded_res) {
    return OpCode::kOther;
  }
  const auto& encoded = encoded_res.value();
  if (encoded.key.size() != current_key_.size() ||
      std::memcmp(encoded.key.data(), current_key_.data(), current_key_.size()) != 0) {
    return OpCode::kOther;
  }

  const auto key_slice =
      Slice(reinterpret_cast<const uint8_t*>(encoded.key.data()), encoded.key.size());
  const auto new_value_slice =
      Slice(reinterpret_cast<const uint8_t*>(encoded.value.data()), encoded.value.size());

  OpCode rc = OpCode::kOther;
  if (encoded.value.size() == current_value_.size() &&
      encoded.value.size() <= std::numeric_limits<uint16_t>::max()) {
    std::string update_desc_buf(UpdateDesc::Size(1), 0);
    auto* update_desc = UpdateDesc::CreateFrom(reinterpret_cast<uint8_t*>(update_desc_buf.data()));
    update_desc->num_slots_ = 1;
    update_desc->update_slots_[0].offset_ = 0;
    update_desc->update_slots_[0].size_ = static_cast<uint16_t>(encoded.value.size());

    rc = kv_interface_->UpdatePartial(
        key_slice,
        [&](MutableSlice val) {
          std::memcpy(val.data(), new_value_slice.data(), new_value_slice.size());
        },
        *update_desc);
  } else {
    auto original_value = current_value_;
    rc = kv_interface_->Remove(key_slice);
    if (rc != OpCode::kOK) {
      return rc;
    }
    rc = kv_interface_->Insert(key_slice, new_value_slice);
    if (rc != OpCode::kOK) {
      kv_interface_->Insert(
          key_slice,
          Slice(reinterpret_cast<const uint8_t*>(original_value.data()), original_value.size()));
      is_valid_ = false;
      current_value_.clear();
      return rc;
    }
  }

  if (rc == OpCode::kOK) {
    current_key_ = encoded.key;
    current_value_ = encoded.value;
    is_valid_ = true;
  }
  return rc;
}

bool TableCursor::Assign(Slice key, Slice val) {
  current_key_.assign(reinterpret_cast<const char*>(key.data()), key.size());
  current_value_.assign(reinterpret_cast<const char*>(val.data()), val.size());
  is_valid_ = true;
  return false; // stop scan
}

Result<std::unique_ptr<Table>> Table::Create(LeanStore* store, TableDefinition definition) {
  KVInterface* kv_interface = nullptr;
  BTreeGeneric* tree = nullptr;
  switch (definition.primary_index_type) {
  case lean_btree_type::LEAN_BTREE_TYPE_ATOMIC: {
    auto res = store->CreateBasicKv(definition.name, definition.primary_index_config);
    if (!res) {
      return std::move(res.error());
    }
    auto* kv = res.value();
    kv_interface = kv;
    tree = kv;
    break;
  }
  case lean_btree_type::LEAN_BTREE_TYPE_MVCC: {
    auto res = store->CreateTransactionKV(definition.name, definition.primary_index_config);
    if (!res) {
      return std::move(res.error());
    }
    auto* kv = res.value();
    kv_interface = kv;
    tree = kv;
    break;
  }
  default:
    return Error::General("unsupported primary index type");
  }

  auto table = std::unique_ptr<Table>(new Table(store, std::move(definition), kv_interface, tree));
  return table;
}

Result<std::unique_ptr<Table>> Table::WrapExisting(LeanStore* store, TableDefinition definition) {
  auto* tree = store->tree_registry_->GetTree(definition.name);
  if (tree == nullptr) {
    return Error::General("backing tree not found for table");
  }
  auto* generic = dynamic_cast<BTreeGeneric*>(tree);
  if (generic == nullptr) {
    return Error::General("backing tree type mismatch");
  }
  KVInterface* kv_interface = dynamic_cast<KVInterface*>(generic);
  if (kv_interface == nullptr) {
    return Error::General("backing tree not KVInterface");
  }
  auto table =
      std::unique_ptr<Table>(new Table(store, std::move(definition), kv_interface, generic));
  return table;
}

OpCode Table::Insert(const lean_row* row) {
  auto encoded_res = codec_.Encode(row);
  if (!encoded_res) {
    return OpCode::kOther;
  }
  const auto& enc = encoded_res.value();
  return kv_interface_->Insert(
      Slice(reinterpret_cast<const uint8_t*>(enc.key.data()), enc.key.size()),
      Slice(reinterpret_cast<const uint8_t*>(enc.value.data()), enc.value.size()));
}

OpCode Table::Remove(const lean_row* key_row) {
  auto key_res = codec_.EncodeKey(key_row);
  if (!key_res) {
    return OpCode::kOther;
  }
  auto& key = key_res.value();
  return kv_interface_->Remove(Slice(reinterpret_cast<const uint8_t*>(key.data()), key.size()));
}

OpCode Table::Lookup(const lean_row* key_row, lean_row* out_row) {
  if (out_row == nullptr || out_row->columns == nullptr) {
    return OpCode::kOther;
  }

  auto key_res = codec_.EncodeKey(key_row);
  if (!key_res) {
    return OpCode::kOther;
  }
  auto& key = key_res.value();

  OpCode decode_status = OpCode::kOK;
  auto rc = kv_interface_->Lookup(Slice(reinterpret_cast<const uint8_t*>(key.data()), key.size()),
                                  [&](Slice val) {
                                    auto decode_res = codec_.DecodeValue(val, out_row);
                                    if (!decode_res) {
                                      decode_status = OpCode::kOther;
                                    }
                                  });
  if (decode_status != OpCode::kOK) {
    return decode_status;
  }
  return rc;
}

std::unique_ptr<TableCursor> Table::NewCursor() {
  return std::make_unique<TableCursor>(kv_interface_, definition_);
}

} // namespace leanstore
