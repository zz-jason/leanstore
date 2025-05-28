#include "telemetry/metrics_http_exposer.hpp"

namespace leanstore::telemetry {

MetricsHttpExposer::MetricsHttpExposer(int32_t port)
    : UserThread(nullptr, "MetricsExposer"),
      port_(port) {
  server_.new_task_queue = [] { return new httplib::ThreadPool(1); };

  server_.Get("/heap",
              [&](const httplib::Request& req, httplib::Response& res) { handle_heap(req, res); });

  server_.Get("/profile", [&](const httplib::Request& req, httplib::Response& res) {
    handle_profile(req, res);
  });
}

} // namespace leanstore::telemetry