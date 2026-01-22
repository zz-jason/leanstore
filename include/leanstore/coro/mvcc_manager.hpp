#pragma once

#include "leanstore/base/log.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/c/types.h"
#include "leanstore/config/store_paths.hpp"
#include "leanstore/tx/concurrency_control.hpp"
#include "leanstore/tx/tx_manager.hpp"
#include "leanstore/wal/logging.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <vector>

namespace leanstore {

class LeanStore;

namespace utils {
class JsonObj;
} // namespace utils

class MvccManager {
public:
  static constexpr auto kKeyGlobalUsrTso = "global_user_tso";
  static constexpr auto kKeyGlobalSysTso = "global_system_tso";

  explicit MvccManager(LeanStore* store);

  utils::JsonObj Serialize() const;
  void Deserialize(const utils::JsonObj& json_obj);

  void InitHistoryStorage();

  WatermarkInfo& GlobalWmkInfo() {
    return global_wmk_info_;
  }

  /// Update the global minimum committed system transaction ID if the given one is larger.
  void UpdateMinCommittedSysTx(lean_txid_t min_committed_sys_tx) {
    auto cur = GetMinCommittedSysTx();
    while (cur < min_committed_sys_tx) {
      if (global_min_committed_sys_tx_.compare_exchange_weak(
              cur, min_committed_sys_tx, std::memory_order_release, std::memory_order_relaxed)) {
        break;
      }
    }
  }

  lean_txid_t GetMinCommittedSysTx() {
    return global_min_committed_sys_tx_.load(std::memory_order_acquire);
  }

  uint64_t AllocUsrTxTs() {
    return usr_tso_.fetch_add(1);
  }

  uint64_t GetUsrTxTs() const {
    return usr_tso_.load();
  }

  uint64_t AllocSysTxTs() {
    return sys_tso_.fetch_add(1);
  }

  uint64_t GetSysTxTs() const {
    return sys_tso_.load();
  }

  lean_lid_t AllocWalGsn() {
    return wal_gsn_.fetch_add(1);
  }

  std::vector<std::unique_ptr<Logging>>& Loggings() {
    return loggings_;
  }

  std::vector<std::unique_ptr<TxManager>>& TxMgrs() {
    return tx_mgrs_;
  }

private:
  LeanStore* store_;

  WatermarkInfo global_wmk_info_;

  /// The minimum flushed system transaction ID among all worker threads. User transactions whose
  /// max observed system transaction ID not larger than it can be committed safely.
  std::atomic<lean_txid_t> global_min_committed_sys_tx_ = 0;

  /// The global timestamp oracle for user transactions. Used to generate start and commit
  /// timestamps for user transactions. Start from a positive number, 0 indicates invalid timestamp
  std::atomic<uint64_t> usr_tso_ = 1;

  /// The global timestamp oracle for system transactions. Used to generate timestamps for system
  /// transactions. Start from a positive number, 0 indicates invalid timestamp
  std::atomic<uint64_t> sys_tso_ = 1;

  /// The global WAL GSN counter. Used to order the WAL records among all WAL files.
  std::atomic<lean_lid_t> wal_gsn_ = 0;

  /// All the logging instances in the system. Each worker thread should have 1 logging instance
  /// to write its own WAL entries.
  std::vector<std::unique_ptr<Logging>> loggings_;

  /// All the transaction managers in the system. Each thread or coroutine should have its own
  /// transaction manager if it needs to run transactions.
  std::vector<std::unique_ptr<TxManager>> tx_mgrs_;
};

inline MvccManager::MvccManager(LeanStore* store) : store_(store) {
  auto* store_option = store->store_option_;
  auto num_tx_mgrs =
      store_option->worker_threads_ * store_option->max_concurrent_transaction_per_worker_;

  // init logging
  loggings_.reserve(store_option->worker_threads_);
  for (auto i = 0U; i < store_option->worker_threads_; i++) {
    loggings_.emplace_back(std::make_unique<Logging>(store_option->wal_buffer_bytes_));

#ifdef LEAN_ENABLE_CORO
    if (!store_option->enable_wal_) {
      Log::Info("Skipping logging initialization, WAL is disabled");
      continue;
    }
    // create wal dir if not exists
    std::string wal_dir = StorePaths::WalDir(store_option->store_dir_);
    if (!std::filesystem::exists(wal_dir)) {
      std::filesystem::create_directories(wal_dir);
      Log::Info("Created WAL directory: {}", wal_dir);
    }

    std::string file_name = std::format(CoroExecutor::kCoroExecNamePattern, i);
    std::string file_path = std::format("{}/{}.wal", wal_dir, file_name);
    loggings_.back()->InitWalFd(file_path);
#endif
  }

  // init transaction managers
  tx_mgrs_.reserve(num_tx_mgrs);
  for (auto i = 0U; i < num_tx_mgrs; i++) {
    tx_mgrs_.emplace_back(std::make_unique<TxManager>(i, tx_mgrs_, store));
  }
}

inline void MvccManager::InitHistoryStorage() {
  static constexpr lean_btree_config kBtreeConfig = {.enable_wal_ = false,
                                                     .use_bulk_insert_ = true};
  static constexpr auto kUpdateNameFormat = "_history_updates_{}";
  static constexpr auto kRemoveNameFormat = "_history_removes_{}";

  auto create_btree = [&](const std::string& name) -> BasicKV* {
    auto res = BasicKV::Create(store_, name, kBtreeConfig);
    if (!res) {
      Log::Fatal("Create btree failed, name={}, error={}", name, res.error().ToString());
    }
    return res.value();
  };

  for (uint64_t i = 0; i < tx_mgrs_.size(); i++) {
    std::string update_btree_name = std::format(kUpdateNameFormat, i);
    std::string remove_btree_name = std::format(kRemoveNameFormat, i);
    tx_mgrs_[i]->cc_.history_storage_.SetUpdateIndex(create_btree(update_btree_name));
    tx_mgrs_[i]->cc_.history_storage_.SetRemoveIndex(create_btree(remove_btree_name));
  }
}

} // namespace leanstore