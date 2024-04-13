#pragma once

#include "utils/RandomGenerator.hpp"
#include "utils/UserThread.hpp"

#ifdef ENABLE_PROFILING
#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>
#endif
#include <httplib.h>
#include <prometheus/collectable.h>
#include <prometheus/text_serializer.h>

#include <mutex>

#include <fcntl.h>
#include <unistd.h>

namespace leanstore::telemetry {

const std::string kContentType("text/plain; version=0.0.4; charset=utf-8");

class MetricsHttpExposer : public utils::UserThread {
public:
  MetricsHttpExposer(LeanStore* store);

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
      mServer.listen("0.0.0.0", mPort);
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

  void handleHeap(const httplib::Request& req [[maybe_unused]],
                  httplib::Response& res) {
#ifdef ENABLE_PROFILING
    // get the profiling time in seconds from the query
    auto secondsStr = req.get_param_value("seconds");
    auto seconds = secondsStr.empty() ? 10 : std::stoi(secondsStr);

    // generate a random file name
    auto perfFile = createRandomFile();

    // profile for the given seconds
    HeapProfilerStart(perfFile.c_str());
    SCOPED_DEFER({
      HeapProfilerStop();
      std::remove(perfFile.c_str());
    });
    sleep(seconds);

    // dump the profile and return it
    res.set_content(GetHeapProfile(), kContentType);
    return;
#else
    res.set_content("not implemented", kContentType);
    return;
#endif
  }

  void handleProfile(const httplib::Request& req [[maybe_unused]],
                     httplib::Response& res) {

#ifdef ENABLE_PROFILING
    // get the profiling time in seconds from the query
    auto secondsStr = req.get_param_value("seconds");
    auto seconds = secondsStr.empty() ? 10 : std::stoi(secondsStr);

    // generate a random file name
    auto perfFile = createRandomFile();
    SCOPED_DEFER(std::remove(perfFile.c_str()));

    // profile for the given seconds
    ProfilerStart(perfFile.c_str());
    sleep(seconds);
    ProfilerStop();
    ProfilerFlush();

    // read the file and return it
    readProfile(perfFile, res);
    return;
#else
    res.set_content("not implemented", kContentType);
#endif
  }

  std::string createRandomFile() {
    auto perfFile = std::format("/tmp/leanstore-{}.prof",
                                utils::RandomGenerator::RandAlphString(8));
    std::ofstream file(perfFile);
    file.close();
    return perfFile;
  }

  void readProfile(const std::string& file, httplib::Response& res) {
    std::ifstream stream(file);
    std::stringstream buffer;
    buffer << stream.rdbuf();
    res.set_content(buffer.str(), kContentType);
  }

  /// The http server
  httplib::Server mServer;

  int32_t mPort;

  /// The mutex to protect mCollectable
  std::mutex mCollectableMutex;

  /// The Collectable to expose metrics
  std::shared_ptr<prometheus::Collectable> mCollectable;
};

} // namespace leanstore::telemetry