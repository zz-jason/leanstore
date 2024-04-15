#pragma once

#include <spdlog/spdlog.h>

#include <cassert>
#include <format>
#include <string_view>

namespace leanstore {

class Log {
public:
  static void Init() {
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] %v");
    spdlog::set_level(spdlog::level::debug);
  }

  template <typename... Args>
  static void DebugCheck(bool condition, std::format_string<Args...> fmt,
                         Args&&... args) {
#ifdef NDEBUG
#else
    if (!condition) {
      spdlog::critical(std::format(fmt, std::forward<Args>(args)...));
      assert(false);
    }
#endif
  }

  static void DebugCheck(bool condition, std::string_view msg = "") {
#ifdef NDEBUG
#else
    if (!condition) {
      spdlog::critical(msg);
      assert(false);
    }
#endif
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