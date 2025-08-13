#include "leanstore/concurrency/cr_manager.hpp"

#include "leanstore-c/store_option.h"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/concurrency/group_committer.hpp"
#include "leanstore/concurrency/history_storage.hpp"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/concurrency/worker_thread.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/log.hpp"
#include "utils/coroutine/coro_env.hpp"
#include "utils/coroutine/mvcc_manager.hpp"
#include "utils/json.hpp"

#include <cassert>
#include <memory>
#include <vector>

namespace {
constexpr auto kKeyWalSize = "wal_size";
constexpr auto kKeyGlobalUsrTso = "global_user_tso";
constexpr auto kKeyGlobalSysTso = "global_system_tso";
constexpr auto kUpdateNameFormat = "_history_tree_{}_updates";
constexpr auto kRemoveNameFormat = "_history_tree_{}_removes";
constexpr BTreeConfig kBtreeConfig = {.enable_wal_ = false, .use_bulk_insert_ = true};
} // namespace

namespace leanstore::cr {

CRManager::CRManager(leanstore::LeanStore* store) : store_(store), group_committer_(nullptr) {
  // start worker threads
  auto num_worker_threads = store->store_option_->worker_threads_;
  StartWorkerThreads(num_worker_threads);

  // start group commit thread
  StartGroupCommitter(num_worker_threads);

  // create history storage for each worker
  CreateWorkerHistories();
}

void CRManager::StartWorkerThreads(uint64_t num_worker_threads) {
  worker_threads_.reserve(num_worker_threads);
  auto& tx_mgrs = store_->MvccManager()->TxMgrs();
  for (auto i = 0u; i < num_worker_threads; i++) {
    auto worker_thread = std::make_unique<WorkerThread>(store_, i, i);
    worker_thread->Start();
    auto* tx_mgr = tx_mgrs[i].get();
    worker_thread->SetJob([tx_mgr]() { CoroEnv::SetCurTxMgr(tx_mgr); });
    worker_thread->Wait();
    worker_threads_.emplace_back(std::move(worker_thread));
  }
}

void CRManager::StartGroupCommitter(int cpu) {
  if (!store_->store_option_->enable_wal_) {
    Log::Info("WAL is disabled, skip starting group committer thread");
    return;
  }

  auto& tx_mgrs = store_->MvccManager()->TxMgrs();
  group_committer_ = std::make_unique<GroupCommitter>(store_, tx_mgrs, cpu);
  group_committer_->Start();
}

void CRManager::Stop() {
  if (group_committer_ != nullptr) {
    group_committer_->Stop();
  }

  for (auto& worker_thread : worker_threads_) {
    worker_thread->Stop();
  }
  worker_threads_.clear();
}

CRManager::~CRManager() {
  Stop();
}

void CRManager::CreateWorkerHistories() {
  if (worker_threads_.empty()) {
    return;
  }

  auto create_btree = [this](const std::string& name) -> storage::btree::BasicKV* {
    auto res = storage::btree::BasicKV::Create(store_, name, kBtreeConfig);
    if (!res) {
      Log::Fatal("Create btree failed, name={}, error={}", name, res.error().ToString());
    }
    return res.value();
  };

  worker_threads_[0]->SetJob([&]() {
    auto& tx_mgrs = store_->MvccManager()->TxMgrs();
    for (uint64_t i = 0; i < store_->store_option_->worker_threads_; i++) {
      std::string update_btree_name = std::format(kUpdateNameFormat, i);
      std::string remove_btree_name = std::format(kRemoveNameFormat, i);
      tx_mgrs[i]->cc_.history_storage_.SetUpdateIndex(create_btree(update_btree_name));
      tx_mgrs[i]->cc_.history_storage_.SetRemoveIndex(create_btree(remove_btree_name));
    }
  });
  worker_threads_[0]->Wait();
}

utils::JsonObj CRManager::Serialize() const {
  utils::JsonObj json_obj;
  if (group_committer_ != nullptr) {
    json_obj.AddUint64(kKeyWalSize, group_committer_->wal_size_);
  }
  return json_obj;
}

void CRManager::Deserialize(const utils::JsonObj& json_obj) {
  if (group_committer_ != nullptr && json_obj.HasMember(kKeyWalSize)) {
    group_committer_->wal_size_ = json_obj.GetUint64(kKeyWalSize).value();
  }
}

} // namespace leanstore::cr
