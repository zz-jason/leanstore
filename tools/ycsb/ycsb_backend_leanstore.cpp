#include "tools/ycsb/ycsb_backend_leanstore.hpp"

#include "leanstore/base/error.hpp"
#include "leanstore/base/result.hpp"
#include "leanstore/coro/coro_env.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_session.hpp"
#include "leanstore/lean_store.hpp"
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
  const auto type = bench_transaction_kv_ ? lean_btree_type::LEAN_BTREE_TYPE_MVCC
                                          : lean_btree_type::LEAN_BTREE_TYPE_ATOMIC;
  auto create_res = session_.CreateBTree(
      std::string(name), type, lean_btree_config{.enable_wal_ = true, .use_bulk_insert_ = false});
  if (!create_res) {
    return std::move(create_res.error());
  }
  return {};
}

Result<YcsbKvSpace> YcsbLeanSession::GetKvSpace(std::string_view name) {
  auto get_res = session_.GetBTree(std::string(name));
  if (!get_res) {
    return Error::General(std::format("KvSpace not found: {}", name));
  }
  if (bench_transaction_kv_) {
    return YcsbKvSpace(std::make_unique<YcsbLeanMvccKvSpace>(session_, std::move(get_res.value())));
  }
  return YcsbKvSpace(std::make_unique<YcsbLeanAtomicKvSpace>(std::move(get_res.value())));
}

//------------------------------------------------------------------------------
// YcsbLeanAtomicKvSpace
//------------------------------------------------------------------------------

Result<void> YcsbLeanAtomicKvSpace::Put(std::string_view key, std::string_view value) {
  auto put_res = kv_space_.Insert(key, value);
  if (!put_res) {
    return Error::General(
        std::format("Put failed, key={}, error={}", key, put_res.error().ToString()));
  }
  return {};
}

Result<void> YcsbLeanAtomicKvSpace::Update(std::string_view key, std::string_view value) {
  auto update_res = kv_space_.Update(key, value);
  if (!update_res) {
    return Error::General(
        std::format("Update failed, key={}, error={}", key, update_res.error().ToString()));
  }
  return {};
}

Result<void> YcsbLeanAtomicKvSpace::Get(std::string_view key, std::string& value_out) {
  auto get_res = kv_space_.Lookup(key);
  if (!get_res) {
    return Error::General(
        std::format("Get failed, key={}, error={}", key, get_res.error().ToString()));
  }
  auto& value = get_res.value();
  value_out.assign(reinterpret_cast<const char*>(value.data()), value.size());
  return {};
}

//------------------------------------------------------------------------------
// YcsbLeanMvccKvSpace
//------------------------------------------------------------------------------

Result<void> YcsbLeanMvccKvSpace::Put(std::string_view key, std::string_view value) {
  session_.StartTx();
  auto put_res = kv_space_.Insert(key, value);
  if (!put_res) {
    session_.AbortTx();
    return Error::General(
        std::format("Put failed, key={}, error={}", key, put_res.error().ToString()));
  }
  session_.CommitTx();
  return {};
}

Result<void> YcsbLeanMvccKvSpace::Update(std::string_view key, std::string_view value) {
  session_.StartTx();
  auto update_res = kv_space_.Update(key, value);
  if (!update_res) {
    session_.AbortTx();
    return Error::General(
        std::format("Update failed, key={}, error={}", key, update_res.error().ToString()));
  }
  session_.CommitTx();
  return {};
}

Result<void> YcsbLeanMvccKvSpace::Get(std::string_view key, std::string& value_out) {
  session_.StartTx();
  auto get_res = kv_space_.Lookup(key);
  if (!get_res) {
    session_.AbortTx();
    return Error::General(
        std::format("Get failed, key={}, error={}", key, get_res.error().ToString()));
  }
  auto& value = get_res.value();
  value_out.assign(reinterpret_cast<const char*>(value.data()), value.size());
  session_.CommitTx();
  return {};
}

} // namespace leanstore::ycsb
