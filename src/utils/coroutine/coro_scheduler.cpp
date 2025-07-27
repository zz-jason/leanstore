#include "utils/coroutine/coro_scheduler.hpp"

#include "leanstore/concurrency/worker_context.hpp"
#include "utils/coroutine/coro_future.hpp"

#include <memory>
#include <utility>
#include <vector>

namespace leanstore {

void CoroScheduler::InitCoroExecutors() {
  ScopedTimer timer([this](double elapsed_ms) {
    Log::Info("CoroExecutors inited, num_threads={}, elapsed={}ms", num_threads_, elapsed_ms);
  });

  // Start all threads
  for (auto& executor : coro_executors_) {
    executor->Start();
  }

  // Wait for all threads to be ready
  for (auto& executor : coro_executors_) {
    while (!executor->IsReady()) {
    }
  }
}

void CoroScheduler::DeinitCoroExecutors() {
  ScopedTimer timer([this](double elapsed_ms) {
    Log::Info("CoroExecutors deinited, num_threads={}, elapsed={}ms", num_threads_, elapsed_ms);
  });

  // Stop all threads
  for (auto& executor : coro_executors_) {
    executor->Stop();
  }

  // Wait for all threads to finish
  for (auto& executor : coro_executors_) {
    executor->Join();
  }
}

void CoroScheduler::InitWorkerCtxs() {
  if (!store_->store_option_->enable_wal_) {
    Log::Info("Skipping worker context initialization, WAL is disabled");
    return;
  }

  ScopedTimer timer([this](double elapsed_ms) {
    Log::Info("WorkerContexts inited, num_threads={}, elapsed={}ms", num_threads_, elapsed_ms);
  });

  std::vector<std::shared_ptr<CoroFuture<void>>> futures;
  for (auto i = 0u; i < coro_executors_.size(); i++) {
    auto coro_ctx_init_job = [i, this]() {
      cr::WorkerContext::s_tls_worker_ctx =
          std::make_unique<cr::WorkerContext>(i, worker_ctxs_, store_);
      cr::WorkerContext::s_tls_worker_ctx_ptr = cr::WorkerContext::s_tls_worker_ctx.get();
      worker_ctxs_[i] = cr::WorkerContext::s_tls_worker_ctx_ptr;
    };
    futures.emplace_back(Submit(std::move(coro_ctx_init_job), i));
  }
  for (auto& future : futures) {
    future->Wait();
  }
}

} // namespace leanstore