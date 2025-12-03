#include "coroutine/coro_executor.hpp"

#include "coroutine/auto_commit_protocol.hpp"
#include "coroutine/coroutine.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/buffer-manager/page_evictor.hpp"
#include "leanstore/cpp/base/log.hpp"

#include <cassert>
#include <format>
#include <memory>
#include <utility>

namespace leanstore {

CoroExecutor::CoroExecutor(LeanStore* store, AutoCommitProtocol* commit_protocol, int64_t thread_id)
    : store_(store),
      commit_protocol_(commit_protocol),
      thread_id_(thread_id) {
  if (store_ != nullptr) {
    // init page evictor
    auto* buffer_manager = store_->buffer_manager_.get();
    page_evictor_ = std::make_unique<leanstore::PageEvictor>(
        store_, "PageEvictor", 0, buffer_manager->num_bfs_, buffer_manager->buffer_pool_,
        buffer_manager->num_partitions_, buffer_manager->partitions_);
  } else {
    page_evictor_ = nullptr;
  }

  if (!store)
    user_tasks_.resize(4);
  else {
    user_tasks_.resize(store->store_option_->max_concurrent_transaction_per_worker_);
  }

  CreateSysCoros();
}

void CoroExecutor::CreateSysCoros() {
  // System coroutine for autonomous transaction commit
  if (commit_protocol_ != nullptr) {
    auto sys_coro = std::make_unique<Coroutine>([this]() {
      while (keep_running_) {
        commit_protocol_->Run();
        CurrentCoro()->Yield(CoroState::kRunning);
      }
    });
    Log::Info("Creating system coroutine for transaction commit");
    sys_tasks_.emplace_back(std::move(sys_coro));
  }

  // System coroutine for page eviction
  if (page_evictor_ != nullptr) {
    auto sys_coro = std::make_unique<Coroutine>([this]() {
      while (keep_running_) {
        page_evictor_->Run4Partitions(eviction_pending_partitions_);
        eviction_pending_partitions_.clear();
        CurrentCoro()->Yield(CoroState::kRunning);
      }
    });
    Log::Info("Creating system coroutine for page eviction");
    sys_tasks_.emplace_back(std::move(sys_coro));
  }

  // System coroutine for IO polling
  auto sys_io_coro = std::make_unique<Coroutine>([this]() {
    while (keep_running_) {
      coro_io_.Poll();
      CurrentCoro()->Yield(CoroState::kRunning);
    }
  });
  Log::Info("Creating system coroutine for IO polling");
  sys_tasks_.emplace_back(std::move(sys_io_coro));
}

CoroExecutor::~CoroExecutor() {
  std::string thread_name = std::format(kCoroExecNamePattern, thread_id_);
  Log::Info("Destroying coro executor, thread_name={}", thread_name);

  Stop();
  if (thread_.joinable()) {
    thread_.join();
  }
}

void CoroExecutor::ThreadLoop() {
  static constexpr int kCoroutineRunsLimit = 1;
  int user_coro_runs = 0;
  bool sys_coro_required = false;
  int cur_slot = -1;
  int num_slots = user_tasks_.size();
  int num_yield_slots = 0;

  while (keep_running_) {
    std::unique_ptr<Coroutine> coro{nullptr};
    // previous round stop at this slot, so we move to next.
    cur_slot++;
    if (cur_slot >= num_slots) {
      cur_slot = 0;
    }
    if (user_tasks_[cur_slot] != nullptr) {
      if (IsCoroReadyToRun(user_tasks_[cur_slot], sys_coro_required)) {
        coro = std::move(user_tasks_[cur_slot]);
        user_tasks_[cur_slot] = nullptr;
        num_yield_slots--;
      } else {
        if (sys_coro_required) {
          RunSystemCoros();
          sys_coro_required = false;
        }
        continue;
      }
    } else {
      // only wait for new job if there are no other yield slots.
      bool wait = (num_yield_slots == 0);
      if (!DequeueCoro(coro, wait))
        continue;
    }

    // Shutdown if required
    if (!keep_running_) {
      break;
    }

    // run the coroutine
    RunCoroutine(coro.get());
    user_coro_runs++;
    if (coro->GetState() != CoroState::kDone) {
      user_tasks_[cur_slot] = std::move(coro);
      num_yield_slots++;
    }

    // Run system coroutines if needed
    if (user_coro_runs >= kCoroutineRunsLimit) {
      RunSystemCoros();
      user_coro_runs = 0;
    }
  }

  std::string thread_name = std::format(kCoroExecNamePattern, thread_id_);
  Log::Info("Coro executor stopped, thread_name={}", thread_name);
}

bool CoroExecutor::IsCoroReadyToRun(std::unique_ptr<Coroutine>& coro, bool& sys_coro_required) {
  switch (coro->GetState()) {
  case CoroState::kReady:
  case CoroState::kRunning: {
    return true;
  }
  case CoroState::kWaitingMutex: {
    if (!coro->TryLock()) {
      return false;
    }
    return true;
  }
  case CoroState::kWaitingIo: {
    if (!coro->IsIoCompleted()) {
      sys_coro_required = true;
      return false;
    }
    return true;
  }
  case CoroState::kDone:
  default: {
    LEAN_DCHECK(false, std::format("Un expected coro state: {}", (int)coro->GetState()));
    return true;
  }
  }
  return true;
}

void CoroExecutor::RunSystemCoros() {
  for (auto& sys_task : sys_tasks_) {
    RunCoroutine(sys_task.get());
  }
}

} // namespace leanstore