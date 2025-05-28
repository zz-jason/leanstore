#include "leanstore/concurrency/cr_manager.hpp"

#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/concurrency/group_committer.hpp"
#include "leanstore/concurrency/history_storage.hpp"
#include "leanstore/concurrency/worker_context.hpp"
#include "leanstore/concurrency/worker_thread.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/log.hpp"

#include <memory>
#include <vector>

namespace leanstore::cr {

CRManager::CRManager(leanstore::LeanStore* store) : store_(store), group_committer_(nullptr) {
  auto* store_option = store->store_option_;
  // start all worker threads
  worker_ctxs_.resize(store_option->worker_threads_);
  worker_threads_.reserve(store_option->worker_threads_);
  for (uint64_t worker_id = 0; worker_id < store_option->worker_threads_; worker_id++) {
    auto worker_thread = std::make_unique<WorkerThread>(store, worker_id, worker_id);
    worker_thread->Start();

    // create thread-local transaction executor on each worker thread
    worker_thread->SetJob([&]() {
      WorkerContext::sTlsWorkerCtx =
          std::make_unique<WorkerContext>(worker_id, worker_ctxs_, store_);
      WorkerContext::sTlsWorkerCtxRaw = WorkerContext::sTlsWorkerCtx.get();
      worker_ctxs_[worker_id] = WorkerContext::sTlsWorkerCtx.get();
    });
    worker_thread->Wait();
    worker_threads_.emplace_back(std::move(worker_thread));
  }

  // start group commit thread
  if (store_->store_option_->enable_wal_) {
    const int cpu = store_option->worker_threads_;
    group_committer_ = std::make_unique<GroupCommitter>(store_, store_->wal_fd_, worker_ctxs_, cpu);
    group_committer_->Start();
  }

  // create history storage for each worker
  // History tree should be created after worker thread and group committer are
  // started.
  if (store_option->worker_threads_ > 0) {
    worker_threads_[0]->SetJob([&]() { setup_history_storage4_each_worker(); });
    worker_threads_[0]->Wait();
  }
}

void CRManager::Stop() {
  group_committer_->Stop();

  for (auto& worker_thread : worker_threads_) {
    worker_thread->Stop();
  }
  worker_threads_.clear();
}

CRManager::~CRManager() {
  Stop();
}

void CRManager::setup_history_storage4_each_worker() {
  for (uint64_t i = 0; i < store_->store_option_->worker_threads_; i++) {
    // setup update tree
    std::string update_btree_name = std::format("_history_tree_{}_updates", i);
    auto res = storage::btree::BasicKV::Create(
        store_, update_btree_name, BTreeConfig{.enable_wal_ = false, .use_bulk_insert_ = true});
    if (!res) {
      Log::Fatal("Failed to set up update history tree, updateBtreeName={}, error={}",
                 update_btree_name, res.error().ToString());
    }
    auto* update_index = res.value();

    // setup delete tree
    std::string remove_btree_name = std::format("_history_tree_{}_removes", i);
    res = storage::btree::BasicKV::Create(
        store_, remove_btree_name, BTreeConfig{.enable_wal_ = false, .use_bulk_insert_ = true});
    if (!res) {
      Log::Fatal("Failed to set up remove history tree, removeBtreeName={}, error={}",
                 remove_btree_name, res.error().ToString());
    }
    auto* remove_index = res.value();
    worker_ctxs_[i]->cc_.history_storage_.SetUpdateIndex(update_index);
    worker_ctxs_[i]->cc_.history_storage_.SetRemoveIndex(remove_index);
  }
}

constexpr char kKeyWalSize[] = "wal_size";
constexpr char kKeyGlobalUsrTso[] = "global_user_tso";
constexpr char kKeyGlobalSysTso[] = "global_system_tso";

StringMap CRManager::Serialize() {
  StringMap map;
  map[kKeyWalSize] = std::to_string(group_committer_->wal_size_);
  map[kKeyGlobalUsrTso] = std::to_string(store_->usr_tso_.load());
  map[kKeyGlobalSysTso] = std::to_string(store_->sys_tso_.load());
  return map;
}

void CRManager::Deserialize(StringMap map) {
  group_committer_->wal_size_ = std::stoull(map[kKeyWalSize]);
  store_->usr_tso_ = std::stoull(map[kKeyGlobalUsrTso]);
  store_->sys_tso_ = std::stoull(map[kKeyGlobalSysTso]);

  store_->crmanager_->global_wmk_info_.wmk_of_all_tx_ = store_->usr_tso_.load();
}

} // namespace leanstore::cr
