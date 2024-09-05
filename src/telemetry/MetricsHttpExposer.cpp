#include "telemetry/MetricsHttpExposer.hpp"

namespace leanstore::telemetry {

MetricsHttpExposer::MetricsHttpExposer(int32_t port)
    : UserThread(nullptr, "MetricsExposer"),
      mPort(port) {
  mServer.new_task_queue = [] { return new httplib::ThreadPool(1); };

  mServer.Get("/heap",
              [&](const httplib::Request& req, httplib::Response& res) { handleHeap(req, res); });

  mServer.Get("/profile", [&](const httplib::Request& req, httplib::Response& res) {
    handleProfile(req, res);
  });
}

} // namespace leanstore::telemetry