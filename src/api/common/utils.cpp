#include "leanstore/common/utils.h"

#include "telemetry/metrics_http_exposer.hpp"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <mutex>

#include <stdint.h>
#include <stdlib.h>

namespace {
leanstore::telemetry::MetricsHttpExposer* global_metrics_http_exposer = nullptr;
std::mutex global_metrics_http_exposer_mutex;
} // namespace

void lean_metrics_exposer_start(int32_t port) {
  std::lock_guard guard{global_metrics_http_exposer_mutex};
  if (global_metrics_http_exposer == nullptr) {
    global_metrics_http_exposer = new leanstore::telemetry::MetricsHttpExposer(port);
    global_metrics_http_exposer->Start();
  }
}

void lean_metrics_exposer_stop() {
  std::lock_guard guard{global_metrics_http_exposer_mutex};
  if (global_metrics_http_exposer != nullptr) {
    delete global_metrics_http_exposer;
    global_metrics_http_exposer = nullptr;
  }
}