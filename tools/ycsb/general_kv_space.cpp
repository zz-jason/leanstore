#include "ycsb/general_kv_space.hpp"

#include "coroutine/coro_env.hpp"
#include "leanstore/cpp/base/defer.hpp"
#include "leanstore/cpp/base/error.hpp"
#include "leanstore/cpp/base/result.hpp"
#include "leanstore/cpp/base/small_vector.hpp"
#include "leanstore/kv_interface.hpp"

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <cassert>
#include <format>

namespace leanstore::ycsb {

//------------------------------------------------------------------------------
// RocksDbKvSpace
//------------------------------------------------------------------------------

Result<void> RocksDbKvSpace::Insert(Slice key, Slice value) {
  auto key_slice = rocksdb::Slice(reinterpret_cast<const char*>(key.data()), key.size());
  auto val_slice = rocksdb::Slice(reinterpret_cast<const char*>(value.data()), value.size());
  auto status = db_.Put(write_options_, key_slice, val_slice);
  if (!status.ok()) {
    return Error::General(std::format("RocksDB Insert failed: {}", status.ToString()));
  }
  return {};
}

Result<void> RocksDbKvSpace::Lookup(Slice key, ByteBuffer& value_out) {
  auto key_slice = rocksdb::Slice(reinterpret_cast<const char*>(key.data()), key.size());
  std::string val_read;
  auto status = db_.Get(read_options_, key_slice, &val_read);
  if (!status.ok()) {
    return Error::General(std::format("RocksDB Lookup failed: {}", status.ToString()));
  }
  value_out.resize(val_read.size());
  std::memcpy(value_out.data(), val_read.data(), val_read.size());
  return {};
}

Result<void> RocksDbKvSpace::Update(Slice key, Slice value) {
  return Insert(key, value);
}

//------------------------------------------------------------------------------
// WiredTigerKvSpace
//------------------------------------------------------------------------------

Result<void> WiredTigerKvSpace::Insert(Slice key, Slice value) {
  ResetItem(key_item_, key);
  ResetItem(val_item_, value);
  cursor_.set_key(&cursor_, &key_item_);
  cursor_.set_value(&cursor_, &val_item_);

  int ret = cursor_.insert(&cursor_);
  if (ret != 0) {
    return Error::General(std::format("WiredTiger Insert failed: {}", wiredtiger_strerror(ret)));
  }
  return {};
}

Result<void> WiredTigerKvSpace::Lookup(Slice key, ByteBuffer& value_out) {
  ResetItem(key_item_, key);
  cursor_.set_key(&cursor_, &key_item_);
  int ret = cursor_.search(&cursor_);
  if (ret != 0) {
    return Error::General(std::format("WiredTiger Lookup failed: {}", wiredtiger_strerror(ret)));
  }

  cursor_.get_value(&cursor_, &val_item_);

  value_out.resize(val_item_.size);
  std::memcpy(value_out.data(), val_item_.data, val_item_.size);
  return {};
}

Result<void> WiredTigerKvSpace::Update(Slice key, Slice value) {
  ResetItem(key_item_, key);
  ResetItem(val_item_, value);
  cursor_.set_key(&cursor_, &key_item_);
  cursor_.set_value(&cursor_, &val_item_);

  int ret = cursor_.update(&cursor_);
  if (ret != 0) {
    return Error::General(std::format("WiredTiger Update failed: {}", wiredtiger_strerror(ret)));
  }
  return {};
}

//------------------------------------------------------------------------------
// LeanAtomicKvSpace
//------------------------------------------------------------------------------

Result<void> LeanAtomicKvSpace::Insert(Slice key, Slice value) {
  auto op_code = atomic_btree_.Insert(key, value);
  if (op_code != OpCode::kOK) {
    return Error::General(
        std::format("Lean Atomic BTree Insert failed, opCode={}", static_cast<uint8_t>(op_code)));
  }
  return {};
}

Result<void> LeanAtomicKvSpace::Lookup(Slice key, ByteBuffer& value_out) {
  auto copy_value = [&](Slice value) {
    value_out.resize(value.size());
    std::memcpy(value_out.data(), value.data(), value.size());
  };

  auto op_code = atomic_btree_.Lookup(key, copy_value);
  if (op_code != OpCode::kOK) {
    return Error::General(
        std::format("Lean Atomic BTree Lookup failed, opCode={}", static_cast<uint8_t>(op_code)));
  }
  return {};
}

Result<void> LeanAtomicKvSpace::Update(Slice key, Slice value) {
  auto update_desc_buf_size = UpdateDesc::Size(1);
  SmallBuffer256 update_desc_buffer(update_desc_buf_size);
  uint8_t* update_desc_buf = update_desc_buffer.Data();

  auto* update_desc = UpdateDesc::CreateFrom(update_desc_buf);
  update_desc->num_slots_ = 1;
  update_desc->update_slots_[0].offset_ = 0;
  update_desc->update_slots_[0].size_ = value.size();

  auto update_callback = [&](MutableSlice to_update) {
    auto new_val_size = update_desc->update_slots_[0].size_;
    assert(to_update.size() >= value.size() && "to_update size is smaller than value size");
    std::memcpy(to_update.data(), value.data(), new_val_size);
  };

  auto op_code = atomic_btree_.UpdatePartial(key, update_callback, *update_desc);
  if (op_code != OpCode::kOK) {
    return Error::General(
        std::format("Lean Atomic BTree Update failed, opCode={}", static_cast<uint8_t>(op_code)));
  }
  return {};
}

//------------------------------------------------------------------------------
// LeanMvccKvSpace
//------------------------------------------------------------------------------

Result<void> LeanMvccKvSpace::Insert(Slice key, Slice value) {
  CoroEnv::CurTxMgr().StartTx(TxMode::kShortRunning, IsolationLevel::kSnapshotIsolation);
  LEAN_DEFER(CoroEnv::CurTxMgr().CommitTx());

  auto op_code = mvcc_btree_.Insert(key, value);
  if (op_code != OpCode::kOK) {
    return Error::General(
        std::format("Lean Mvcc BTree Insert failed, opCode={}", static_cast<uint8_t>(op_code)));
  }
  return {};
}

Result<void> LeanMvccKvSpace::Lookup(Slice key, ByteBuffer& value_out) {
  CoroEnv::CurTxMgr().StartTx(TxMode::kShortRunning, IsolationLevel::kSnapshotIsolation, true);
  LEAN_DEFER(CoroEnv::CurTxMgr().CommitTx());

  auto copy_value = [&](Slice value) {
    value_out.resize(value.size());
    std::memcpy(value_out.data(), value.data(), value.size());
  };

  auto op_code = mvcc_btree_.Lookup(key, copy_value);
  if (op_code != OpCode::kOK) {
    return Error::General(
        std::format("Lean Atomic BTree Lookup failed, opCode={}", static_cast<uint8_t>(op_code)));
  }
  return {};
}

Result<void> LeanMvccKvSpace::Update(Slice key, Slice value) {
  CoroEnv::CurTxMgr().StartTx(TxMode::kShortRunning, IsolationLevel::kSnapshotIsolation);
  LEAN_DEFER(CoroEnv::CurTxMgr().CommitTx());

  auto update_desc_buf_size = UpdateDesc::Size(1);
  SmallBuffer256 update_desc_buffer(update_desc_buf_size);
  uint8_t* update_desc_buf = update_desc_buffer.Data();

  auto* update_desc = UpdateDesc::CreateFrom(update_desc_buf);
  update_desc->num_slots_ = 1;
  update_desc->update_slots_[0].offset_ = 0;
  update_desc->update_slots_[0].size_ = value.size();

  auto update_callback = [&](MutableSlice to_update) {
    auto new_val_size = update_desc->update_slots_[0].size_;
    assert(to_update.size() >= value.size() && "to_update size is smaller than value size");
    std::memcpy(to_update.data(), value.data(), new_val_size);
  };

  auto op_code = mvcc_btree_.UpdatePartial(key, update_callback, *update_desc);
  if (op_code != OpCode::kOK) {
    return Error::General(
        std::format("Lean Atomic BTree Update failed, opCode={}", static_cast<uint8_t>(op_code)));
  }
  return {};
}

} // namespace leanstore::ycsb