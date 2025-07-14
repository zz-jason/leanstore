#include "leanstore/concurrency/cr_manager.hpp"

#include "leanstore-c/store_option.h"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/concurrency/group_committer.hpp"
#include "leanstore/concurrency/history_storage.hpp"
#include "leanstore/concurrency/worker_context.hpp"
#include "leanstore/concurrency/worker_thread.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/log.hpp"
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
  worker_ctxs_.resize(num_worker_threads);
  worker_threads_.reserve(num_worker_threads);

  for (auto i = 0u; i < num_worker_threads; i++) {
    auto worker_thread = std::make_unique<WorkerThread>(store_, i, i);
    worker_thread->Start();

    worker_thread->SetJob([&]() {
      WorkerContext::s_tls_worker_ctx = std::make_unique<WorkerContext>(i, worker_ctxs_, store_);
      WorkerContext::s_tls_worker_ctx_ptr = WorkerContext::s_tls_worker_ctx.get();
      worker_ctxs_[i] = WorkerContext::s_tls_worker_ctx_ptr;
    });
    worker_thread->Wait();
    worker_threads_.emplace_back(std::move(worker_thread));
  }
}

void CRManager::StartGroupCommitter(int cpu) {
  if (!store_->store_option_->enable_wal_) {
    Log::Info("WAL is disabled, skip starting group committer thread");
    return;
  }

  group_committer_ = std::make_unique<GroupCommitter>(store_, worker_ctxs_, cpu);
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
    for (uint64_t i = 0; i < store_->store_option_->worker_threads_; i++) {
      std::string update_btree_name = std::format(kUpdateNameFormat, i);
      std::string remove_btree_name = std::format(kRemoveNameFormat, i);
      worker_ctxs_[i]->cc_.history_storage_.SetUpdateIndex(create_btree(update_btree_name));
      worker_ctxs_[i]->cc_.history_storage_.SetRemoveIndex(create_btree(remove_btree_name));
    }
  });
  worker_threads_[0]->Wait();
}

utils::JsonObj CRManager::Serialize() const {
  utils::JsonObj json_obj;
  if (group_committer_ != nullptr) {
    json_obj.AddUint64(kKeyWalSize, group_committer_->wal_size_);
  }
  json_obj.AddUint64(kKeyGlobalUsrTso, store_->usr_tso_.load());
  json_obj.AddUint64(kKeyGlobalSysTso, store_->sys_tso_.load());
  return json_obj;
}

void CRManager::Deserialize(const utils::JsonObj& json_obj) {
  if (group_committer_ != nullptr && json_obj.HasMember(kKeyWalSize)) {
    group_committer_->wal_size_ = json_obj.GetUint64(kKeyWalSize).value();
  }
  store_->usr_tso_.store(json_obj.GetUint64(kKeyGlobalUsrTso).value());
  store_->sys_tso_.store(json_obj.GetUint64(kKeyGlobalSysTso).value());
  global_wmk_info_.wmk_of_all_tx_ = store_->usr_tso_.load();
}

} // namespace leanstore::cr
