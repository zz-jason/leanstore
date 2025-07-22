#include "telemetry/metrics_http_exposer.hpp"

namespace leanstore::telemetry {

MetricsHttpExposer::MetricsHttpExposer(int32_t port)
    : ManagedThread(nullptr, kThreadName),
      port_(port) {
  server_.new_task_queue = [] { return new httplib::ThreadPool(1); };

  server_.Get(kHeapEndpoint, [&](const httplib::Request& req, httplib::Response& res) {
    HandleHeapRequest(req, res);
  });

  server_.Get(kProfileEndpoint, [&](const httplib::Request& req, httplib::Response& res) {
    HandleProfileRequest(req, res);
  });
}

} // namespace leanstore::telemetry