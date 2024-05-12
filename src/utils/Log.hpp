#pragma once

#include "leanstore/StoreOption.hpp"
#include "utils/Defer.hpp"

#include <spdlog/common.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <format>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>

#ifndef NDEBUG
#define LS_DLOG(...) leanstore::Log::Debug(__VA_ARGS__);
#define LS_DCHECK(...) leanstore::Log::DebugCheck(__VA_ARGS__);
#else
#define LS_DLOG(...) (void)0;
#define LS_DCHECK(...) (void)0;
#endif

namespace leanstore {

class Log {
public:
  inline static bool sInited = false;
  inline static std::mutex sInitMutex;

  static void Init(const StoreOption& option) {
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

  template <typename... Args>
  static void DebugCheck(bool condition, std::format_string<Args...> fmt,
                         Args&&... args) {
    if (!condition) {
      spdlog::critical(std::format(fmt, std::forward<Args>(args)...));
      assert(false);
    }
  }

  static void DebugCheck(bool condition, std::string_view msg = "") {
    if (!condition) {
      spdlog::critical(msg);
      assert(false);
    }
  }

  static void Debug(std::string_view msg) {
    spdlog::debug(msg);
  }

  static void Info(std::string_view msg) {
    spdlog::info(msg);
  }

  static void Warn(std::string_view msg) {
    spdlog::warn(msg);
  }

  static void Error(std::string_view msg) {
    spdlog::error(msg);
  }

  static void Fatal(std::string_view msg) {
    spdlog::critical(msg);
    assert(false);
  }

  template <typename... Args>
  static void Debug(std::format_string<Args...> fmt, Args&&... args) {
    spdlog::debug(std::format(fmt, std::forward<Args>(args)...));
  }

  template <typename... Args>
  static void Info(std::format_string<Args...> fmt, Args&&... args) {
    spdlog::info(std::format(fmt, std::forward<Args>(args)...));
  }

  template <typename... Args>
  static void Warn(std::format_string<Args...> fmt, Args&&... args) {
    spdlog::warn(std::format(fmt, std::forward<Args>(args)...));
  }

  template <typename... Args>
  static void Error(std::format_string<Args...> fmt, Args&&... args) {
    spdlog::error(std::format(fmt, std::forward<Args>(args)...));
  }

  template <typename... Args>
  static void Fatal(std::format_string<Args...> fmt, Args&&... args) {
    spdlog::critical(std::format(fmt, std::forward<Args>(args)...));
    assert(false);
  }
};

} // namespace leanstore
