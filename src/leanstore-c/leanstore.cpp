#include "leanstore-c/leanstore.h"

#include "leanstore-c/store_option.h"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/lean_store.hpp"
#include "telemetry/metrics_http_exposer.hpp"

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

//------------------------------------------------------------------------------
// Interfaces for metrics
//------------------------------------------------------------------------------

static leanstore::telemetry::MetricsHttpExposer* global_metrics_http_exposer = nullptr;
static std::mutex global_metrics_http_exposer_mutex;

void StartMetricsHttpExposer(int32_t port) {
  std::unique_lock guard{global_metrics_http_exposer_mutex};
  global_metrics_http_exposer = new leanstore::telemetry::MetricsHttpExposer(port);
  global_metrics_http_exposer->Start();
}

void StopMetricsHttpExposer() {
  std::unique_lock guard{global_metrics_http_exposer_mutex};
  if (global_metrics_http_exposer != nullptr) {
    delete global_metrics_http_exposer;
    global_metrics_http_exposer = nullptr;
  }
}
