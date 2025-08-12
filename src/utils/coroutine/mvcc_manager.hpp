#pragma once

#include "leanstore/concurrency/concurrency_control.hpp"
#include "leanstore/concurrency/tx_manager.hpp"
#include "utils/json.hpp"

#include <memory>
#include <vector>

namespace leanstore {

class LeanStore;

class MvccManager {
public:
  static constexpr auto kKeyGlobalUsrTso = "global_user_tso";
  static constexpr auto kKeyGlobalSysTso = "global_system_tso";

  MvccManager(uint64_t num_tx_mgrs, LeanStore* store) {
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

  /// All the thread-local worker references
  std::vector<std::unique_ptr<cr::TxManager>> tx_mgrs_;
};

} // namespace leanstore