#include "tools/ycsb/ycsb_backend_leanstore.hpp"

#include "leanstore/base/error.hpp"
#include "leanstore/base/result.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/coro/coro_env.hpp"
#include "leanstore/coro/coro_scheduler.hpp"
#include "leanstore/coro/coro_session.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/tx/transaction_kv.hpp"
#include "tools/ycsb/ycsb_backend.hpp"

#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>
#include <wiredtiger.h>

#include <cassert>
#include <cstdint>
#include <format>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace leanstore::ycsb {

//------------------------------------------------------------------------------
// YcsbLeanDb
//------------------------------------------------------------------------------

Result<std::unique_ptr<YcsbLeanDb>> YcsbLeanDb::Create(const YcsbOptions& options) {
  auto data_dir = options.DataDir();
  lean_store_option* lean_option = lean_store_option_create(data_dir.c_str());
  lean_option->create_from_scratch_ = options.IsCreateFromScratch();
  lean_option->enable_eager_gc_ = true;
  lean_option->enable_wal_ = true;
  lean_option->worker_threads_ = options.workers_;
  lean_option->buffer_pool_size_ = options.dram_ << 30;
  if (options.clients_ > 0) {
    lean_option->max_concurrent_transaction_per_worker_ =
        (options.clients_ + options.workers_ - 1) / options.workers_;
  }
  auto res = LeanStore::Open(lean_option);
  if (!res) {
    return std::move(res.error());
  }

  auto store = std::move(res.value());
  assert(store != nullptr);
  return std::make_unique<YcsbLeanDb>(std::move(store), options.IsBenchTransactionKv());
}

YcsbSession YcsbLeanDb::NewSession() {
  auto runs_on = round_robin_counter_++ % store_->store_option_->worker_threads_;
  return YcsbSession(std::make_unique<YcsbLeanSession>(*store_, runs_on, bench_transaction_kv_));
}

//------------------------------------------------------------------------------
// YcsbLeanSession
//------------------------------------------------------------------------------

Result<void> YcsbLeanSession::CreateKvSpace(std::string_view name) {
  Result<void> result{};
  auto task = [&]() {
    if (bench_transaction_kv_) {
      lean_btree_config config{.enable_wal_ = true, .use_bulk_insert_ = false};
      auto create_res = store_.CreateTransactionKV(std::string(name), config);
      if (!create_res) {
        result = std::move(create_res.error());
      }
    } else {
      lean_btree_config config{.enable_wal_ = true, .use_bulk_insert_ = false};
      auto create_res = store_.CreateBasicKv(std::string(name), config);
      if (!create_res) {
        result = std::move(create_res.error());
      }
    }
  };
  store_.SubmitAndWait(coro_session_, std::move(task));
  return result;
}

Result<YcsbKvSpace> YcsbLeanSession::GetKvSpace(std::string_view name) {
  std::unique_ptr<YcsbLeanAtomicKvSpace> atomic_kv_space = nullptr;
  std::unique_ptr<YcsbLeanMvccKvSpace> mvcc_kv_space = nullptr;
  auto task = [&]() {
    if (bench_transaction_kv_) {
      TransactionKV* btree;
      store_.GetTransactionKV(std::string(name), &btree);
      if (btree != nullptr) {
        mvcc_kv_space = std::make_unique<YcsbLeanMvccKvSpace>(store_, *coro_session_, *btree);
      }
      return;
    }
    BasicKV* btree;
    store_.GetBasicKV(std::string(name), &btree);
    if (btree != nullptr) {
      atomic_kv_space = std::make_unique<YcsbLeanAtomicKvSpace>(store_, *coro_session_, *btree);
    }
    return;
  };
  store_.SubmitAndWait(coro_session_, std::move(task));

  if (bench_transaction_kv_) {
    if (mvcc_kv_space == nullptr) {
      return Error::General(std::format("KvSpace not found: {}", name));
    }
    return YcsbKvSpace(std::move(mvcc_kv_space));
  }
  if (atomic_kv_space == nullptr) {
    return Error::General(std::format("KvSpace not found: {}", name));
  }
  return YcsbKvSpace(std::move(atomic_kv_space));
}

//------------------------------------------------------------------------------
// YcsbLeanAtomicKvSpace
//------------------------------------------------------------------------------

Result<void> YcsbLeanAtomicKvSpace::Put(std::string_view key, std::string_view value) {
  OpCode opcode = OpCode::kOther;
  store_.SubmitAndWait(&session_, [&]() { opcode = kv_space_.Insert(key, value); });
  if (opcode != OpCode::kOK) {
    return Error::General(std::format("Put failed, key={}, error={}", key, ToString(opcode)));
  }
  return {};
}

Result<void> YcsbLeanAtomicKvSpace::Update(std::string_view key, std::string_view value) {
  temp_buffer_.resize(UpdateDesc::Size(1));
  uint8_t* update_desc_buf = temp_buffer_.data();

  auto* update_desc = UpdateDesc::CreateFrom(update_desc_buf);
  update_desc->num_slots_ = 1;
  update_desc->update_slots_[0].offset_ = 0;
  update_desc->update_slots_[0].size_ = value.size();

  auto update_callback = [&](MutableSlice to_update) {
    auto new_val_size = update_desc->update_slots_[0].size_;
    assert(to_update.size() >= value.size() && "to_update size is smaller than value size");
    std::memcpy(to_update.data(), value.data(), new_val_size);
  };

  OpCode opcode = OpCode::kOther;
  store_.SubmitAndWait(
      &session_, [&]() { opcode = kv_space_.UpdatePartial(key, update_callback, *update_desc); });
  if (opcode != OpCode::kOK) {
    return Error::General(std::format("Update failed, key={}, error={}", key, ToString(opcode)));
  }
  return {};
}

Result<void> YcsbLeanAtomicKvSpace::Get(std::string_view key, std::string& value_out) {
  auto copy_value = [&](Slice value) {
    value_out.assign(reinterpret_cast<const char*>(value.data()), value.size());
  };

  OpCode opcode = OpCode::kOther;
  store_.SubmitAndWait(&session_, [&]() { opcode = kv_space_.Lookup(key, copy_value); });
  if (opcode != OpCode::kOK) {
    return Error::General(std::format("Get failed, key={}, error={}", key, ToString(opcode)));
  }
  return {};
}

//------------------------------------------------------------------------------
// YcsbLeanMvccKvSpace
//------------------------------------------------------------------------------

Result<void> YcsbLeanMvccKvSpace::Put(std::string_view key, std::string_view value) {
  OpCode opcode = OpCode::kOther;
  auto task = [&]() {
    CoroEnv::CurTxMgr().StartTx(TxMode::kShortRunning, IsolationLevel::kSnapshotIsolation);
    opcode = kv_space_.Insert(key, value);
    if (opcode != OpCode::kOK) {
      CoroEnv::CurTxMgr().AbortTx();
      return;
    }
    CoroEnv::CurTxMgr().CommitTx();
  };

  store_.SubmitAndWait(&session_, std::move(task));
  if (opcode != OpCode::kOK) {
    return Error::General(std::format("Put failed, key={}, error={}", key, ToString(opcode)));
  }
  return {};
}

Result<void> YcsbLeanMvccKvSpace::Update(std::string_view key, std::string_view value) {
  OpCode opcode = OpCode::kOther;
  auto task = [&]() {
    temp_buffer_.resize(UpdateDesc::Size(1));
    uint8_t* update_desc_buf = temp_buffer_.data();

    auto* update_desc = UpdateDesc::CreateFrom(update_desc_buf);
    update_desc->num_slots_ = 1;
    update_desc->update_slots_[0].offset_ = 0;
    update_desc->update_slots_[0].size_ = value.size();

    auto update_callback = [&](MutableSlice to_update) {
      auto new_val_size = update_desc->update_slots_[0].size_;
      assert(to_update.size() >= value.size() && "to_update size is smaller than value size");
      std::memcpy(to_update.data(), value.data(), new_val_size);
    };

    CoroEnv::CurTxMgr().StartTx(TxMode::kShortRunning, IsolationLevel::kSnapshotIsolation);
    opcode = kv_space_.UpdatePartial(key, update_callback, *update_desc);
    if (opcode != OpCode::kOK) {
      CoroEnv::CurTxMgr().AbortTx();
      return;
    }
    CoroEnv::CurTxMgr().CommitTx();
  };

  store_.SubmitAndWait(&session_, std::move(task));
  if (opcode != OpCode::kOK) {
    return Error::General(std::format("Put failed, key={}, error={}", key, ToString(opcode)));
  }
  return {};
}

Result<void> YcsbLeanMvccKvSpace::Get(std::string_view key, std::string& value_out) {
  auto copy_value = [&](Slice value) {
    value_out.assign(reinterpret_cast<const char*>(value.data()), value.size());
  };

  OpCode opcode = OpCode::kOther;
  auto task = [&]() {
    CoroEnv::CurTxMgr().StartTx(TxMode::kShortRunning, IsolationLevel::kSnapshotIsolation, true);
    opcode = kv_space_.Lookup(key, copy_value);
    if (opcode != OpCode::kOK) {
      CoroEnv::CurTxMgr().AbortTx();
      return;
    }
    CoroEnv::CurTxMgr().CommitTx();
  };

  store_.SubmitAndWait(&session_, std::move(task));
  if (opcode != OpCode::kOK) {
    return Error::General(std::format("Get failed, key={}, error={}", key, ToString(opcode)));
  }
  return {};
}

} // namespace leanstore::ycsb