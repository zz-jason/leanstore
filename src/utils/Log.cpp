#include "utils/Log.hpp"

#include "utils/Defer.hpp"

#include <spdlog/common.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include <cstdlib>
#include <iostream>
#include <string>

namespace leanstore {

void Log::Init(const StoreOption& option) {
  if (sInited) {
    return;
  }

  std::unique_lock writeLock(sInitMutex);

  auto logger = spdlog::basic_logger_mt("basic_logger", option.GetLogPath());
  SCOPED_DEFER({
    spdlog::set_default_logger(logger);
    spdlog::flush_every(std::chrono::seconds(3));
    sInited = true;
  });

  // set log pattern
  logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] %v");

  // set log level
  switch (option.mLogLevel) {
  case LogLevel::kDebug: {
    logger->set_level(spdlog::level::debug);
    break;
  }
  case LogLevel::kInfo: {
    logger->set_level(spdlog::level::info);
    break;
  }
  case LogLevel::kWarn: {
    logger->set_level(spdlog::level::warn);
    break;
  }
  case LogLevel::kError: {
    logger->set_level(spdlog::level::err);
    break;
  }
  default: {
    std::cerr << std::format("unsupported log level: {}",
                             static_cast<uint8_t>(option.mLogLevel));
    std::abort();
  }
  }
}

void Log::DebugCheck(bool condition, const std::string& msg) {
  if (!condition) {
    Fatal(msg);
  }
}

void Log::Debug(const std::string& msg) {
  spdlog::debug(msg);
}

void Log::Info(const std::string& msg) {
  spdlog::info(msg);
}

void Log::Warn(const std::string& msg) {
  spdlog::warn(msg);
}

void Log::Error(const std::string& msg) {
  spdlog::error(msg);
}

void Log::Fatal(const std::string& msg) {
  spdlog::critical(msg);
  std::abort();
}

} // namespace leanstore