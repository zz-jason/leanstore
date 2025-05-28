#pragma once

#include "leanstore/utils/random_generator.hpp"
#include "leanstore/utils/user_thread.hpp"

#ifdef ENABLE_PROFILING
#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>
#endif

#include <httplib.h>

#include <fcntl.h>
#include <unistd.h>

namespace leanstore::telemetry {

const std::string kContentType("text/plain; version=0.0.4; charset=utf-8");

class MetricsHttpExposer : public utils::UserThread {
private:
  /// The http server
  httplib::Server server_;

  /// The port to expose metrics
  int32_t port_;

public:
  MetricsHttpExposer(int32_t port);

  ~MetricsHttpExposer() override {
    server_.stop();
  }

protected:
  void run_impl() override {
    while (keep_running_) {
      server_.listen("0.0.0.0", port_);
    }
  }

private:
  void handle_heap(const httplib::Request& req [[maybe_unused]], httplib::Response& res) {
#ifdef ENABLE_PROFILING
    // get the profiling time in seconds from the query
    auto seconds_str = req.get_param_value("seconds");
    auto seconds = seconds_str.empty() ? 10 : std::stoi(seconds_str);

    // generate a random file name
    auto perf_file = create_random_file();

    // profile for the given seconds
    HeapProfilerStart(perf_file.c_str());
    SCOPED_DEFER({
      HeapProfilerStop();
      std::remove(perf_file.c_str());
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

  void handle_profile(const httplib::Request& req [[maybe_unused]], httplib::Response& res) {

#ifdef ENABLE_PROFILING
    // get the profiling time in seconds from the query
    auto seconds_str = req.get_param_value("seconds");
    auto seconds = seconds_str.empty() ? 10 : std::stoi(seconds_str);

    // generate a random file name
    auto perf_file = create_random_file();
    SCOPED_DEFER(std::remove(perf_file.c_str()));

    // profile for the given seconds
    ProfilerStart(perf_file.c_str());
    sleep(seconds);
    ProfilerStop();
    ProfilerFlush();

    // read the file and return it
    read_profile(perf_file, res);
    return;
#else
    res.set_content("not implemented", kContentType);
#endif
  }

  std::string create_random_file() {
    auto perf_file =
        std::format("/tmp/leanstore-{}.prof", utils::RandomGenerator::RandAlphString(8));
    std::ofstream file(perf_file);
    file.close();
    return perf_file;
  }

  void read_profile(const std::string& file, httplib::Response& res) {
    std::ifstream stream(file);
    std::stringstream buffer;
    buffer << stream.rdbuf();
    res.set_content(buffer.str(), kContentType);
  }
};

} // namespace leanstore::telemetry