#include "leanstore/cpp/base/log.hpp"

#include "coroutine/coro_env.hpp"
#include "leanstore/cpp/config/store_paths.hpp"

#include <spdlog/common.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include <cassert>
#include <cstdlib>
#include <format>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>

namespace leanstore {

static constexpr char kLoggerName[] = "leanstore_logger";
static constexpr char kLogFormat[] = "[%Y-%m-%d %H:%M:%S.%e] [%t] [%l] %v";
static constexpr int kFlushIntervalSeconds = 3;

namespace {

std::mutex logger_mutex;
std::shared_ptr<spdlog::logger> logger = nullptr;
std::shared_ptr<spdlog::logger> origin_default_logger = spdlog::default_logger();

spdlog::level::level_enum LogLevelToSpdlogLevel(lean_log_level level) {
  switch (level) {
  case lean_log_level::LEAN_LOG_LEVEL_DEBUG:
    return spdlog::level::debug;
  case lean_log_level::LEAN_LOG_LEVEL_INFO:
    return spdlog::level::info;
  case lean_log_level::LEAN_LOG_LEVEL_WARN:
    return spdlog::level::warn;
  case lean_log_level::LEAN_LOG_LEVEL_ERROR:
    return spdlog::level::err;
  default:
    std::cerr << "Unsupported log level: " << static_cast<uint8_t>(level) << std::endl;
    std::abort();
  }
}

void LogWithCoroId(void (*logger)(const std::string& msg), const std::string& msg) {
  auto* cur_coro = CoroEnv::CurCoro();
  if (cur_coro != nullptr) {
    logger(std::format("[Coro-{}] {}", static_cast<void*>(cur_coro), msg));
  } else {
    logger(msg);
  }
}

} // namespace

void Log::Init(const lean_store_option* option) {
  std::lock_guard<std::mutex> lock(logger_mutex);
  if (logger != nullptr) {
    return;
  }

  auto log_path = StorePaths::LogFilePath(option->store_dir_);
  logger = spdlog::basic_logger_mt(kLoggerName, log_path);
  logger->set_pattern(kLogFormat);
  logger->flush_on(spdlog::level::info);
  logger->set_level(LogLevelToSpdlogLevel(option->log_level_));

  spdlog::set_default_logger(logger);
  spdlog::flush_every(std::chrono::seconds(kFlushIntervalSeconds));
  Log::Info("Logger initialized, flushed every {} seconds, flushed on {}", kFlushIntervalSeconds,
            std::string_view(SPDLOG_LEVEL_NAME_INFO));
}

void Log::Deinit() {
  std::lock_guard<std::mutex> lock(logger_mutex);
  if (logger == nullptr) {
    return;
  }

  Log::Info("Logger deinited");
  spdlog::drop(kLoggerName);
  spdlog::set_default_logger(origin_default_logger);
  spdlog::flush_every(std::chrono::seconds(0));
  logger = nullptr;
}

void Log::DebugCheck(bool condition, const std::string& msg) {
  if (!condition) {
    LogWithCoroId(spdlog::critical, msg);
    assert(false);
  }
}

void Log::Debug(const std::string& msg) {
  LogWithCoroId(spdlog::debug, msg);
}

void Log::Info(const std::string& msg) {
  LogWithCoroId(spdlog::info, msg);
}

void Log::Warn(const std::string& msg) {
  LogWithCoroId(spdlog::warn, msg);
}

void Log::Error(const std::string& msg) {
  LogWithCoroId(spdlog::error, msg);
}

void Log::Fatal(const std::string& msg) {
  LogWithCoroId(spdlog::critical, msg);
  std::abort();
}

} // namespace leanstore