#pragma once

#include "leanstore/utils/managed_thread.hpp"
#include "leanstore/utils/random_generator.hpp"

#ifdef LEAN_ENABLE_PROFILING
#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>
#endif

#include <httplib.h>

#include <fstream>

#include <fcntl.h>
#include <unistd.h>

namespace leanstore::telemetry {

class MetricsHttpExposer : public utils::ManagedThread {
private:
  static constexpr char kListenAddress[] = "0.0.0.0";
  static constexpr char kThreadName[] = "metric_exposer";
  static constexpr char kContentType[] = "text/plain; version=0.0.4; charset=utf-8";
  static constexpr char kHeapEndpoint[] = "/heap";
  static constexpr char kProfileEndpoint[] = "/profile";
  static constexpr char kQuerySeconds[] = "seconds";
  static constexpr char kTempFilePattern[] = "/tmp/leanstore-{}.prof";
  static constexpr char kUnsupportedMessage[] = "Unsupported operation";
  static constexpr int32_t kDefaultSeconds = 10;

  /// The http server
  httplib::Server server_;

  /// The port to expose metrics
  int32_t port_;

public:
  explicit MetricsHttpExposer(int32_t port);

  ~MetricsHttpExposer() override {
    server_.stop();
  }

protected:
  void RunImpl() override {
    while (keep_running_) {
      server_.listen(kListenAddress, port_);
    }
  }

private:
  void HandleHeapRequest(const httplib::Request& req [[maybe_unused]], httplib::Response& res) {
#ifdef LEAN_ENABLE_PROFILING
    // get the profiling time in seconds from the query
    auto seconds_str = req.get_param_value(kQuerySeconds);
    auto seconds = seconds_str.empty() ? kDefaultSeconds : std::stoi(seconds_str);

    // generate a random file name
    auto perf_file = CreateRandomFile();

    // profile for the given seconds
    HeapProfilerStart(perf_file.c_str());
    LEAN_DEFER({
      HeapProfilerStop();
      std::remove(perf_file.c_str());
    });
    sleep(seconds);

    // dump the profile and return it
    res.set_content(GetHeapProfile(), kContentType);
    return;
#else
    res.set_content(kUnsupportedMessage, kContentType);
    return;
#endif
  }

  void HandleProfileRequest(const httplib::Request& req [[maybe_unused]], httplib::Response& res) {
#ifdef LEAN_ENABLE_PROFILING
    // get the profiling time in seconds from the query
    auto seconds_str = req.get_param_value(kQuerySeconds);
    auto seconds = seconds_str.empty() ? kDefaultSeconds : std::stoi(seconds_str);

    // generate a random file name
    auto perf_file = CreateRandomFile();
    LEAN_DEFER(std::remove(perf_file.c_str()));

    // profile for the given seconds
    ProfilerStart(perf_file.c_str());
    sleep(seconds);
    ProfilerStop();
    ProfilerFlush();

    // read the file and return it
    ReadProfile(perf_file, res);
    return;
#else
    res.set_content(kUnsupportedMessage, kContentType);
#endif
  }

  std::string CreateRandomFile() {
    auto perf_file = std::format(kTempFilePattern, utils::RandomGenerator::RandAlphString(8));
    std::ofstream file(perf_file);
    file.close();
    return perf_file;
  }

  void ReadProfile(const std::string& file, httplib::Response& res) {
    std::ifstream stream(file);
    std::stringstream buffer;
    buffer << stream.rdbuf();
    res.set_content(buffer.str(), kContentType);
  }
};

} // namespace leanstore::telemetry
