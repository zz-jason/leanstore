#include "leanstore/utils/log.hpp"

#include "leanstore/utils/defer.hpp"

#include <spdlog/common.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include <cstdlib>
#include <format>
#include <iostream>
#include <string>

namespace leanstore {

static constexpr char kLoggerName[] = "leanstore_logger";
static constexpr char kLogFileName[] = "leanstore.log";
static constexpr char kLogFormat[] = "[%Y-%m-%d %H:%M:%S.%e] [%t] [%l] %v";
static constexpr int kFlushIntervalSeconds = 3;

void Log::Init(const StoreOption* option) {
  if (s_inited) {
    return;
  }

  std::unique_lock write_lock(s_init_mutex);

  auto log_path = std::format("{}/{}", option->store_dir_, kLogFileName);
  auto logger = spdlog::basic_logger_mt(kLoggerName, log_path.c_str());

  SCOPED_DEFER({
    spdlog::set_default_logger(logger);
    spdlog::flush_every(std::chrono::seconds(kFlushIntervalSeconds));
    s_inited = true;
    spdlog::info("Logger initialized, flushed every {} seconds, flushed on {}",
                 kFlushIntervalSeconds, SPDLOG_LEVEL_NAME_INFO);
  });

  // set log pattern
  logger->set_pattern(kLogFormat);

  // set flush strategy
  logger->flush_on(spdlog::level::info);

  // set log level
  switch (option->log_level_) {
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
    std::cerr << std::format("unsupported log level: {}", static_cast<uint8_t>(option->log_level_));
    std::abort();
  }
  }
}

void Log::Deinit() {
  spdlog::info("Logger deinited");
  spdlog::drop(kLoggerName);
  spdlog::set_default_logger(nullptr);
  spdlog::flush_every(std::chrono::seconds(0));
  Log::s_inited = false;
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