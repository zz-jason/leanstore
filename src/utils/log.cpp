#include "leanstore/utils/log.hpp"

#include <spdlog/common.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include <cstdlib>
#include <format>
#include <iostream>
#include <mutex>
#include <string>

namespace leanstore {

static constexpr char kLoggerName[] = "leanstore_logger";
static constexpr char kLogFileName[] = "leanstore.log";
static constexpr char kLogFormat[] = "[%Y-%m-%d %H:%M:%S.%e] [%t] [%l] %v";
static constexpr int kFlushIntervalSeconds = 3;

namespace {
std::mutex logger_mutex;
std::shared_ptr<spdlog::logger> logger = nullptr;
std::shared_ptr<spdlog::logger> origin_default_logger = spdlog::default_logger();

spdlog::level::level_enum LogLevelToSpdlogLevel(LogLevel level) {
  switch (level) {
  case LogLevel::kDebug:
    return spdlog::level::debug;
  case LogLevel::kInfo:
    return spdlog::level::info;
  case LogLevel::kWarn:
    return spdlog::level::warn;
  case LogLevel::kError:
    return spdlog::level::err;
  default:
    std::cerr << "Unsupported log level: " << static_cast<uint8_t>(level) << std::endl;
    std::abort();
  }
}

} // namespace

void Log::Init(const StoreOption* option) {
  std::lock_guard<std::mutex> lock(logger_mutex);
  if (logger != nullptr) {
    return;
  }

  auto log_path = std::format("{}/{}", option->store_dir_, kLogFileName);
  logger = spdlog::basic_logger_mt(kLoggerName, log_path.c_str());
  logger->set_pattern(kLogFormat);
  logger->flush_on(spdlog::level::info);
  logger->set_level(LogLevelToSpdlogLevel(option->log_level_));

  spdlog::set_default_logger(logger);
  spdlog::flush_every(std::chrono::seconds(kFlushIntervalSeconds));
  spdlog::info("Logger initialized, flushed every {} seconds, flushed on {}", kFlushIntervalSeconds,
               SPDLOG_LEVEL_NAME_INFO);
}

void Log::Deinit() {
  std::lock_guard<std::mutex> lock(logger_mutex);
  if (logger == nullptr) {
    return;
  }

  spdlog::info("Logger deinited");
  spdlog::drop(kLoggerName);
  spdlog::set_default_logger(origin_default_logger);
  spdlog::flush_every(std::chrono::seconds(0));
  logger = nullptr;
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