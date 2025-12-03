#include "leanstore/concurrency/cr_manager.hpp"

#include "coroutine/coro_env.hpp"
#include "coroutine/mvcc_manager.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/common/types.h"
#include "leanstore/concurrency/group_committer.hpp"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/concurrency/worker_thread.hpp"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/lean_store.hpp"
#include "utils/json.hpp"

#include <cassert>
#include <memory>
#include <vector>

namespace {
constexpr auto kKeyWalSize = "wal_size";
} // namespace

namespace leanstore {

CRManager::CRManager(leanstore::LeanStore* store) : store_(store), group_committer_(nullptr) {
  // start worker threads
  auto num_worker_threads = store->store_option_->worker_threads_;
  StartWorkerThreads(num_worker_threads);

  // start group commit thread
  StartGroupCommitter(num_worker_threads);
}

void CRManager::StartWorkerThreads(uint64_t num_worker_threads) {
  worker_threads_.reserve(num_worker_threads);
  auto& tx_mgrs = store_->GetMvccManager().TxMgrs();
  auto& loggings = store_->GetMvccManager().Loggings();
  for (auto i = 0u; i < num_worker_threads; i++) {
    auto worker_thread = std::make_unique<WorkerThread>(store_, i, i);
    worker_thread->Start();
    auto* tx_mgr = tx_mgrs[i].get();
    auto* logging = loggings[i].get();
    worker_thread->SetJob([logging, tx_mgr]() {
      CoroEnv::SetCurLogging(logging);
      CoroEnv::SetCurTxMgr(tx_mgr);
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

  group_committer_ = std::make_unique<GroupCommitter>(store_, cpu);
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

utils::JsonObj CRManager::Serialize() const {
  utils::JsonObj json_obj;
  if (group_committer_ != nullptr) {
    json_obj.AddUint64(kKeyWalSize, group_committer_->wal_size_);
  }
  return json_obj;
}

void CRManager::Deserialize(const utils::JsonObj& json_obj) {
  if (group_committer_ != nullptr && json_obj.HasMember(kKeyWalSize)) {
    group_committer_->wal_size_ = *json_obj.GetUint64(kKeyWalSize);
  }
}

} // namespace leanstore
