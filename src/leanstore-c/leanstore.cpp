#include "leanstore-c/leanstore.h"

#include "leanstore-c/store_option.h"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/lean_store.hpp"
#include "telemetry/metrics_http_exposer.hpp"
#include "utils/coroutine/coro_session.hpp"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <mutex>
#include <utility>

#include <stdint.h>
#include <stdlib.h>

//------------------------------------------------------------------------------
// OwnedString API
//------------------------------------------------------------------------------

OwnedString* CreateOwnedString(const char* data, uint64_t size) {
  OwnedString* str = new OwnedString();

  if (data == nullptr || size == 0) {
    str->data_ = nullptr;
    str->size_ = 0;
    str->capacity_ = 0;
    return str;
  }

  // allocate memory, copy data
  str->size_ = size;
  str->capacity_ = size + 1;
  str->data_ = new char[size + 1];
  memcpy(str->data_, data, size);
  str->data_[size] = '\0';

  return str;
}

void DestroyOwnedString(OwnedString* str) {
  if (str != nullptr) {
    if (str->data_ != nullptr) {
      // release memory
      delete[] str->data_;
    }

    str->data_ = nullptr;
    str->size_ = 0;
    str->capacity_ = 0;

    // release the string object
    delete str;
  }
}

//------------------------------------------------------------------------------
// LeanStore API
//------------------------------------------------------------------------------

struct LeanStoreHandle {
  std::unique_ptr<leanstore::LeanStore> store_;
};

LeanStoreHandle* CreateLeanStore(StoreOption* option) {
  auto res = leanstore::LeanStore::Open(option);
  if (!res) {
    std::cerr << "open store failed: " << res.error().ToString() << std::endl;
    return nullptr;
  }
  LeanStoreHandle* handle = new LeanStoreHandle();
  handle->store_ = std::move(res.value());
  return handle;
}

void DestroyLeanStore(LeanStoreHandle* handle) {
  delete handle;
}

void* GetLeanStore(LeanStoreHandle* handle) {
  if (handle == nullptr) {
    return nullptr;
  }
  return handle->store_.get();
}

struct LeanStoreSessionHandle {
  leanstore::LeanStore* store_;
  leanstore::CoroSession* session_;
};

namespace {
static std::atomic<uint64_t> runs_on_counter{0};
} // namespace

LeanStoreSessionHandle* LeanStoreTryConnect(LeanStoreHandle* handle) {
  if (handle == nullptr || handle->store_ == nullptr) {
    return nullptr;
  }

  auto* store = handle->store_.get();
  auto runs_on = runs_on_counter++ % store->store_option_->worker_threads_;

  assert(store->GetCoroScheduler() != nullptr && "CoroScheduler should be initialized");
  auto* session = store->GetCoroScheduler()->TryReserveCoroSession(runs_on);
  if (session == nullptr) {
    return nullptr;
  }

  LeanStoreSessionHandle* session_handle = new LeanStoreSessionHandle();
  session_handle->store_ = store;
  session_handle->session_ = session;
  return session_handle;
}

LeanStoreSessionHandle* LeanStoreConnect(LeanStoreHandle* handle) {
  if (handle == nullptr || handle->store_ == nullptr) {
    return nullptr;
  }

  auto* store = handle->store_.get();
  auto runs_on = runs_on_counter++ % store->store_option_->worker_threads_;
  auto* session = store->GetCoroScheduler()->ReserveCoroSession(runs_on);
  assert(session != nullptr && "ReserveCoroSession should never return nullptr");

  LeanStoreSessionHandle* session_handle = new LeanStoreSessionHandle();
  session_handle->store_ = store;
  session_handle->session_ = session;
  return session_handle;
}

void LeanStoreDisconnect(LeanStoreSessionHandle* handle) {
  if (handle == nullptr) {
    return;
  }

  handle->store_->GetCoroScheduler()->ReleaseCoroSession(handle->session_);
  delete handle;
  handle = nullptr;
}

void* GetLeanStoreFromSession(LeanStoreSessionHandle* handle) {
  return handle->store_;
}

void* GetCoroSessionFromSession(LeanStoreSessionHandle* handle) {
  return handle->session_;
}

//------------------------------------------------------------------------------
// Interfaces for metrics
//------------------------------------------------------------------------------

namespace {
leanstore::telemetry::MetricsHttpExposer* global_metrics_http_exposer = nullptr;
std::mutex global_metrics_http_exposer_mutex;
} // namespace

void StartMetricsHttpExposer(int32_t port) {
  std::lock_guard guard{global_metrics_http_exposer_mutex};
  if (global_metrics_http_exposer == nullptr) {
    global_metrics_http_exposer = new leanstore::telemetry::MetricsHttpExposer(port);
    global_metrics_http_exposer->Start();
  }
}

void StopMetricsHttpExposer() {
  std::lock_guard guard{global_metrics_http_exposer_mutex};
  if (global_metrics_http_exposer != nullptr) {
    delete global_metrics_http_exposer;
    global_metrics_http_exposer = nullptr;
  }
}
