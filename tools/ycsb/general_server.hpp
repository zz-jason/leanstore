#pragma once

#include "coroutine/coro_future.hpp"
#include "leanstore/cpp/base/error.hpp"
#include "leanstore/cpp/base/result.hpp"
#include "leanstore/lean_store.hpp"
#include "tools/ycsb/general_kv_space.hpp"
#include "tools/ycsb/task_executor.hpp"
#include "tools/ycsb/terminal_log.hpp"
#include "ycsb_options.hpp"

#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>
#include <wiredtiger.h>

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <sys/types.h>

namespace leanstore::ycsb {

class GeneralServer;

class LeanStoreServer {
public:
  LeanStoreServer(std::unique_ptr<LeanStore> store, bool bench_transaction_kv)
      : store_(std::move(store)),
        bench_transaction_kv_(bench_transaction_kv) {
  }

  static std::unique_ptr<LeanStoreServer> Create(const YcsbOptions& options) {
    auto data_dir = options.DataDir();
    lean_store_option* lean_option = lean_store_option_create(data_dir.c_str());
    lean_option->create_from_scratch_ = options.IsCreateFromScratch();
    lean_option->enable_eager_gc_ = true;
    lean_option->enable_wal_ = true;
    lean_option->worker_threads_ = options.threads_;
    lean_option->buffer_pool_size_ = options.mem_gb_ << 30;
    if (options.clients_ > 0) {
      lean_option->max_concurrent_transaction_per_worker_ =
          (options.clients_ + options.threads_ - 1) / options.threads_;
    }
    auto res = LeanStore::Open(lean_option);
    if (!res) {
      TerminalLogFatal(std::format("Failed to open leanstore: {}", res.error().ToString()));
    }
    auto store = std::move(res.value());
    assert(store != nullptr);
    return std::make_unique<LeanStoreServer>(std::move(store), options.IsBenchTransactionKv());
  }

  template <typename Func>
  void AsyncExecOn(uint64_t wid, Func&& func) {
    reserved_sessions_.push_back(store_->GetCoroScheduler().TryReserveCoroSession(wid));
    assert(reserved_sessions_.back() != nullptr &&
           "Failed to reserve a CoroSession for parallel range execution");
    futures_.emplace_back(
        store_->GetCoroScheduler().Submit(reserved_sessions_.back(), std::forward<Func>(func)));
  }

  void WaitAll() {
    for (const auto& future : futures_) {
      future->Wait();
    }
    futures_.clear();

    for (auto* session : reserved_sessions_) {
      store_->GetCoroScheduler().ReleaseCoroSession(session);
    }
    reserved_sessions_.clear();
  }

  template <typename Func>
  auto ExecOn(uint64_t wid, Func&& func) {
    auto* session = store_->GetCoroScheduler().TryReserveCoroSession(wid);
    assert(session != nullptr && "Failed to reserve a CoroSession for a single execution");
    auto future = store_->GetCoroScheduler().Submit(session, std::forward<Func>(func));
    future->Wait();
    store_->GetCoroScheduler().ReleaseCoroSession(session);
    return future->GetResult();
  }

  void CreateKvSpace() {
    if (bench_transaction_kv_) {
      lean_btree_config config{.enable_wal_ = true, .use_bulk_insert_ = false};
      auto res = store_->CreateTransactionKV(kBTreeName, config);
      if (!res) {
        TerminalLogFatal(std::format("Failed to create kv space: name={}, error={}", kBTreeName,
                                     res.error().ToString()));
      }
    } else {
      lean_btree_config config{.enable_wal_ = true, .use_bulk_insert_ = false};
      auto res = store_->CreateBasicKv(kBTreeName, config);
      if (!res) {
        TerminalLogFatal(std::format("Failed to create kv space: name={}, error={}", kBTreeName,
                                     res.error().ToString()));
      }
    }
  }

  std::unique_ptr<GeneralKvSpace> GetKvSpace() {
    if (bench_transaction_kv_) {
      TransactionKV* btree;
      store_->GetTransactionKV(kBTreeName, &btree);
      if (btree == nullptr) {
        TerminalLogFatal(std::format("Failed to get existing kv space: name={}", kBTreeName));
      }
      return GeneralKvSpace::CreateLeanMvccKvSpace(*btree);
    }

    BasicKV* btree;
    store_->GetBasicKV(kBTreeName, &btree);
    if (btree == nullptr) {
      TerminalLogFatal(std::format("Failed to get existing kv space: name={}", kBTreeName));
    }
    assert(btree != nullptr);
    return GeneralKvSpace::CreateLeanAtomicKvSpace(*btree);
  }

private:
  static constexpr auto kBTreeName = "ycsb";

  std::unique_ptr<LeanStore> store_;
  bool bench_transaction_kv_;
  std::vector<std::shared_ptr<CoroFuture<void>>> futures_;
  std::vector<CoroSession*> reserved_sessions_;
};

class RocksDbServer {
public:
  static std::unique_ptr<RocksDbServer> Create(const YcsbOptions& options) {
    rocksdb::Options rocksdb_options;
    rocksdb_options.create_if_missing = true;
    rocksdb_options.error_if_exists = false;

    // ===== Memory limit =====
    size_t physical_limit = size_t(options.mem_gb_) << 30;
    size_t soft_limit = size_t(physical_limit * 0.9);

    auto shared_cache = rocksdb::NewLRUCache(soft_limit);
    rocksdb_options.write_buffer_manager =
        std::make_shared<rocksdb::WriteBufferManager>(soft_limit, shared_cache);

    // ===== Table =====
    rocksdb::BlockBasedTableOptions table_opts;
    table_opts.block_cache = shared_cache;
    table_opts.cache_index_and_filter_blocks = true;
    table_opts.pin_l0_filter_and_index_blocks_in_cache = true;
    table_opts.index_type = rocksdb::BlockBasedTableOptions::kTwoLevelIndexSearch;
    table_opts.partition_filters = true;
    table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));

    rocksdb_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_opts));

    // ===== Write =====
    rocksdb_options.write_buffer_size = 64 * 1024 * 1024;
    rocksdb_options.max_write_buffer_number = 4;
    rocksdb_options.min_write_buffer_number_to_merge = 1;
    rocksdb_options.allow_concurrent_memtable_write = true;
    rocksdb_options.enable_pipelined_write = true;

    // ===== Background =====
    rocksdb_options.max_background_jobs = std::max<uint64_t>(4u, options.threads_);

    // L0
    rocksdb_options.level0_file_num_compaction_trigger = 6;
    rocksdb_options.level0_slowdown_writes_trigger = 24;
    rocksdb_options.level0_stop_writes_trigger = 36;

    auto path = std::format("{}/ycsb", options.DataDir());
    std::unique_ptr<rocksdb::DB> db = nullptr;
    auto status = rocksdb::DB::Open(rocksdb_options, path, &db);
    if (!status.ok()) {
      TerminalLogFatal(std::format("Failed to open rocksdb: {}", status.ToString()));
    }

    auto worker_task_limit = 8;
    if (options.clients_ > 0) {
      worker_task_limit = (options.clients_ + options.threads_ - 1) / options.threads_;
    }

    return std::make_unique<RocksDbServer>(std::move(db), options.threads_, worker_task_limit);
  }

  RocksDbServer(std::unique_ptr<rocksdb::DB> db, uint64_t workers, uint64_t worker_task_limit)
      : db_(std::move(db)),
        task_executor_(std::make_unique<TaskExecutor>(workers, worker_task_limit)),
        futures_() {
    task_executor_->Start();
  }

  ~RocksDbServer() {
    if (task_executor_ != nullptr) {
      task_executor_->Stop();
      task_executor_ = nullptr;
    }

    if (db_ != nullptr) {
      db_->Close();
      db_ = nullptr;
    }
  }

  template <typename Func>
  auto AsyncExecOn(uint64_t wid, Func&& func) {
    futures_.emplace_back(task_executor_->SubmitOn(wid, std::forward<Func>(func)));
  }

  void WaitAll() {
    for (const auto& future : futures_) {
      future->Wait();
    }
    futures_.clear();
  }

  template <typename Func>
  auto ExecOn(uint64_t wid, Func&& func) {
    auto future = task_executor_->SubmitOn(wid, std::forward<Func>(func));
    future->Wait();
    return future->GetResult();
  }

  void CreateKvSpace() {
    // RocksDB creates column families (equivalent to tables) on the fly during first access.
    // Therefore, no explicit creation is needed here.
  }

  std::unique_ptr<GeneralKvSpace> GetKvSpace() {
    return GeneralKvSpace::CreateRocksDbKvSpace(*db_);
  }

private:
  std::unique_ptr<rocksdb::DB> db_;
  std::unique_ptr<TaskExecutor> task_executor_;
  std::vector<std::shared_ptr<CoroFuture<void>>> futures_;
};

class WiredTigerServer {
public:
  struct WorkerContext {
    WT_SESSION* session_;
  };

  static std::unique_ptr<WiredTigerServer> Create(const YcsbOptions& options) {
    auto data_dir = options.DataDir();
    std::string config_string(
        "create, direct_io=[data, log, checkpoint], "
        "log=(enabled=true,archive=true), statistics_log=(wait=1), "
        "statistics=(all, clear), session_max=2000, eviction=(threads_max=4), cache_size=" +
        std::to_string(options.mem_gb_ * 1024) + "M");

    WT_CONNECTION* conn;
    int ret = wiredtiger_open(data_dir.c_str(), nullptr, config_string.c_str(), &conn);
    if (ret != 0) {
      TerminalLogFatal(std::format("Failed to open wiredtiger: {}", wiredtiger_strerror(ret)));
    }

    auto worker_task_limit = 8;
    if (options.clients_ > 0) {
      worker_task_limit = (options.clients_ + options.threads_ - 1) / options.threads_;
    }
    return std::make_unique<WiredTigerServer>(conn, options.threads_, worker_task_limit);
  }

  WiredTigerServer(WT_CONNECTION* conn, uint64_t workers, uint64_t worker_task_limit)
      : conn_(conn),
        ctxs_(workers),
        task_executor_(nullptr), // Will be initialized below
        futures_() {
    for (auto i = 0U; i < workers; i++) {
      ctxs_[i].session_ = nullptr;
    }
    auto custom_worker_init = [this](uint64_t wid) {
      int ret = conn_->open_session(conn_, nullptr, "isolation=snapshot", &ctxs_[wid].session_);
      if (ret != 0) {
        TerminalLogFatal(
            std::format("Failed to open wiredtiger session: {}", wiredtiger_strerror(ret)));
      }
      s_tls_worker_ctx = &ctxs_[wid];
    };
    auto custom_worker_deinit = [this](uint64_t wid) {
      int ret = ctxs_[wid].session_->close(ctxs_[wid].session_, nullptr);
      if (ret != 0) {
        TerminalLogFatal(
            std::format("Failed to close wiredtiger session: {}", wiredtiger_strerror(ret)));
      }
      s_tls_worker_ctx = nullptr;
    };
    task_executor_ = std::make_unique<TaskExecutor>(
        workers, worker_task_limit, std::move(custom_worker_init), std::move(custom_worker_deinit));
    task_executor_->Start();
  }

  ~WiredTigerServer() {
    if (task_executor_ != nullptr) {
      task_executor_->Stop();
      task_executor_ = nullptr;
    }
    if (conn_ != nullptr) {
      conn_->close(conn_, nullptr);
      conn_ = nullptr;
    }
  }

  template <typename Func>
  auto AsyncExecOn(uint64_t wid, Func&& func) {
    futures_.emplace_back(task_executor_->SubmitOn(wid, std::forward<Func>(func)));
  }

  void WaitAll() {
    for (const auto& future : futures_) {
      future->Wait();
    }
    futures_.clear();
  }

  template <typename Func>
  auto ExecOn(uint64_t wid, Func&& func) {
    auto future = task_executor_->SubmitOn(wid, std::forward<Func>(func));
    future->Wait();
    return future->GetResult();
  }

  void CreateKvSpace() {
    auto& ctx = GetCurWorkerCtx();
    const char* config_string = "key_format=S,value_format=S";
    int ret = ctx.session_->create(ctx.session_, kTableName, config_string);
    if (ret != 0) {
      TerminalLogFatal(std::format("Failed to create table: {}", wiredtiger_strerror(ret)));
    }
  }

  std::unique_ptr<GeneralKvSpace> GetKvSpace() {
    auto& ctx = GetCurWorkerCtx();
    WT_CURSOR* cursor;
    int ret = ctx.session_->open_cursor(ctx.session_, kTableName, nullptr, "raw", &cursor);
    if (ret != 0) {
      TerminalLogFatal(std::format("Failed to open cursor: {}", wiredtiger_strerror(ret)));
    }
    return GeneralKvSpace::CreateWiredTigerKvSpace(*cursor);
  }

  static WorkerContext& GetCurWorkerCtx() {
    assert(s_tls_worker_ctx != nullptr && "No current worker context set for this thread");
    return *s_tls_worker_ctx;
  }

private:
  inline static thread_local WorkerContext* s_tls_worker_ctx = nullptr;
  inline static constexpr const char* kTableName = "table:ycsb";

  WT_CONNECTION* conn_;
  std::vector<WorkerContext> ctxs_;
  std::unique_ptr<TaskExecutor> task_executor_;
  std::vector<std::shared_ptr<CoroFuture<void>>> futures_;
};

class GeneralServer {
public:
  using ServerType = std::variant<std::unique_ptr<LeanStoreServer>, std::unique_ptr<RocksDbServer>,
                                  std::unique_ptr<WiredTigerServer>>;

  explicit GeneralServer(const YcsbOptions& options) {
    if (options.IsBenchBasicKv() || options.IsBenchTransactionKv()) {
      server_impl_ = LeanStoreServer::Create(options);
    } else if (options.IsBenchWiredTiger()) {
      server_impl_ = WiredTigerServer::Create(options);
    } else if (options.IsBenchRocksDb()) {
      server_impl_ = RocksDbServer::Create(options);
    } else {
      TerminalLogFatal("Unknown storage engine option: " + options.target_);
    }
  }

  explicit GeneralServer(std::unique_ptr<LeanStoreServer> server_impl)
      : server_impl_(std::move(server_impl)) {
  }

  explicit GeneralServer(std::unique_ptr<WiredTigerServer> server_impl)
      : server_impl_(std::move(server_impl)) {
  }

  explicit GeneralServer(std::unique_ptr<RocksDbServer> server_impl)
      : server_impl_(std::move(server_impl)) {
  }

  template <typename Func>
  auto AsyncExecOn(uint64_t wid, Func&& func) {
    return std::visit([wid, f = std::forward<Func>(func)](
                          auto& server) mutable { return server->AsyncExecOn(wid, std::move(f)); },
                      server_impl_);
  }

  void WaitAll() {
    std::visit([&](auto&& server) { server->WaitAll(); }, server_impl_);
  }

  template <typename Func>
  auto ExecOn(uint64_t wid, Func&& func) {
    return std::visit([wid, f = std::forward<Func>(func)](
                          auto& server) mutable { return server->ExecOn(wid, std::move(f)); },
                      server_impl_);
  }

  void CreateKvSpace() {
    std::visit([&](auto&& server) { server->CreateKvSpace(); }, server_impl_);
  }

  /// Get the underlying KvSpace while benchmarking.
  /// Should be called in the worker threads during execution.
  std::unique_ptr<GeneralKvSpace> GetKvSpace() {
    return std::visit([&](auto&& server) { return server->GetKvSpace(); }, server_impl_);
  }

private:
  ServerType server_impl_;
};

} // namespace leanstore::ycsb
