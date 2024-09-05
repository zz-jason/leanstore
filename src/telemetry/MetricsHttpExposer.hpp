#pragma once

#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/UserThread.hpp"

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
  //! The http server
  httplib::Server mServer;

  //! The port to expose metrics
  int32_t mPort;

public:
  MetricsHttpExposer(int32_t port);

  ~MetricsHttpExposer() override {
    mServer.stop();
  }

protected:
  void runImpl() override {
    while (mKeepRunning) {
      mServer.listen("0.0.0.0", mPort);
    }
  }

private:
  void handleHeap(const httplib::Request& req [[maybe_unused]], httplib::Response& res) {
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

  void handleProfile(const httplib::Request& req [[maybe_unused]], httplib::Response& res) {

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
    auto perfFile =
        std::format("/tmp/leanstore-{}.prof", utils::RandomGenerator::RandAlphString(8));
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
};

} // namespace leanstore::telemetry