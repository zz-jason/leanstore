#pragma once

#include "leanstore/Config.hpp"
#include "utils/UserThread.hpp"

#include <httplib.h>
#include <prometheus/collectable.h>
#include <prometheus/text_serializer.h>

#include <mutex>

namespace leanstore::telemetry {

const std::string kContentType("text/plain; version=0.0.4; charset=utf-8");

class MetricsHttpExposer : public utils::UserThread {
public:
  MetricsHttpExposer() : UserThread("MetricsExposer") {
    mServer.new_task_queue = [] { return new httplib::ThreadPool(1); };
    mServer.Get("/metrics",
                [&](const httplib::Request& req, httplib::Response& res) {
                  handleMetrics(req, res);
                });
  }

  ~MetricsHttpExposer() override {
    mServer.stop();
  }

  void SetCollectable(std::shared_ptr<prometheus::Collectable> collectable) {
    auto guard = std::unique_lock(mCollectableMutex);
    mCollectable = collectable;
  }

protected:
  void runImpl() override {
    while (mKeepRunning) {
      mServer.listen("0.0.0.0", FLAGS_metrics_port);
    }
  }

private:
  void handleMetrics(const httplib::Request&, httplib::Response& res) {
    auto guard = std::unique_lock(mCollectableMutex);
    if (mCollectable != nullptr) {
      auto metrics = mCollectable->Collect();
      guard.unlock();
      const prometheus::TextSerializer serializer;
      res.set_content(serializer.Serialize(metrics), kContentType);
      return;
    }

    // empty
    guard.unlock();
    const prometheus::TextSerializer serializer;
    std::vector<prometheus::MetricFamily> empty;
    res.set_content(serializer.Serialize(empty), kContentType);
  }

  /// The http server
  httplib::Server mServer;

  /// The mutex to protect mCollectable
  std::mutex mCollectableMutex;

  /// The Collectable to expose metrics
  std::shared_ptr<prometheus::Collectable> mCollectable;
};

} // namespace leanstore::telemetry