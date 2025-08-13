#pragma once

#include "leanstore/concurrency/concurrency_control.hpp"
#include "leanstore/concurrency/logging.hpp"
#include "leanstore/concurrency/tx_manager.hpp"
#include "utils/json.hpp"

#include <filesystem>
#include <memory>
#include <vector>

namespace leanstore {

class LeanStore;

class MvccManager {
public:
  static constexpr auto kKeyGlobalUsrTso = "global_user_tso";
  static constexpr auto kKeyGlobalSysTso = "global_system_tso";

  MvccManager(uint64_t num_tx_mgrs, LeanStore* store) {
    auto* store_option = store->store_option_;

    // init logging
    loggings_.reserve(store_option->worker_threads_);
    for (auto i = 0u; i < store_option->worker_threads_; i++) {
      loggings_.emplace_back(std::make_unique<cr::Logging>(store_option->wal_buffer_bytes_));

#ifdef ENABLE_COROUTINE
      if (!store_option->enable_wal_) {
        Log::Info("Skipping logging initialization, WAL is disabled");
        continue;
      }
      // create wal dir if not exists
      std::string wal_dir = std::string(store_option->store_dir_) + "/wal";
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
    for (auto i = 0u; i < num_tx_mgrs; i++) {
      tx_mgrs_.emplace_back(std::make_unique<cr::TxManager>(i, tx_mgrs_, store));
    }
  }

  utils::JsonObj Serialize() const {
    utils::JsonObj json_obj;
    json_obj.AddUint64(kKeyGlobalUsrTso, GetUsrTxTs());
    json_obj.AddUint64(kKeyGlobalSysTso, GetSysTxTs());
    return json_obj;
  }

  void Deserialize(const utils::JsonObj& json_obj) {
    usr_tso_.store(json_obj.GetUint64(kKeyGlobalUsrTso).value());
    sys_tso_.store(json_obj.GetUint64(kKeyGlobalSysTso).value());
    global_wmk_info_.wmk_of_all_tx_ = json_obj.GetUint64(kKeyGlobalUsrTso).value();
  }

  cr::WatermarkInfo& GlobalWmkInfo() {
    return global_wmk_info_;
  }

  void UpdateMinCommittedSysTx(TXID min_committed_sys_tx) {
    global_min_committed_sys_tx_.store(min_committed_sys_tx, std::memory_order_release);
  }

  TXID GetMinCommittedSysTx() {
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

  std::vector<std::unique_ptr<cr::Logging>>& Loggings() {
    return loggings_;
  }

  std::vector<std::unique_ptr<cr::TxManager>>& TxMgrs() {
    return tx_mgrs_;
  }

private:
  cr::WatermarkInfo global_wmk_info_;

  /// The minimum flushed system transaction ID among all worker threads. User transactions whose
  /// max observed system transaction ID not larger than it can be committed safely.
  std::atomic<TXID> global_min_committed_sys_tx_ = 0;

  /// The global timestamp oracle for user transactions. Used to generate start and commit
  /// timestamps for user transactions. Start from a positive number, 0 indicates invalid timestamp
  std::atomic<uint64_t> usr_tso_ = 1;

  /// The global timestamp oracle for system transactions. Used to generate timestamps for system
  /// transactions. Start from a positive number, 0 indicates invalid timestamp
  std::atomic<uint64_t> sys_tso_ = 1;

  /// All the logging instances in the system. Each worker thread should have 1 logging instance
  /// to write its own WAL entries.
  std::vector<std::unique_ptr<cr::Logging>> loggings_;

  /// All the transaction managers in the system. Each thread or coroutine should have its own
  /// transaction manager if it needs to run transactions.
  std::vector<std::unique_ptr<cr::TxManager>> tx_mgrs_;
};

} // namespace leanstore