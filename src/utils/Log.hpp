#pragma once

#include <glog/logging.h>
#include <spdlog/spdlog.h>

#include <format>
#include <string_view>

namespace leanstore {

class Log {
public:
  static void Init() {
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] %v");
    spdlog::set_level(spdlog::level::debug);
    spdlog::info("{}", "hahaa");
  }

  template <typename... Args>
  static void DebugCheck(bool condition, std::format_string<Args...> fmt,
                         Args&&... args) {
#ifdef NDEBUG
#else
    if (!condition) {
      spdlog::critical(std::format(fmt, std::forward<Args>(args)...));
    }
#endif
  }

  static void DebugCheck(bool condition, std::string_view msg = "") {
#ifdef NDEBUG
#else
    if (!condition) {
      spdlog::critical(msg);
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
  }

  static void DebugIf(bool condition, std::string_view msg) {
    if (condition) {
      spdlog::debug(msg);
    }
  }

  static void InfoIf(bool condition, std::string_view msg) {
    if (condition) {
      spdlog::info(msg);
    }
  }

  static void WarnIf(bool condition, std::string_view msg) {
    if (condition) {
      spdlog::warn(msg);
    }
  }

  static void ErrorIf(bool condition, std::string_view msg) {
    if (condition) {
      spdlog::error(msg);
    }
  }

  static void FatalIf(bool condition, std::string_view msg) {
    if (condition) {
      spdlog::critical(msg);
    }
  }

  template <typename... Args>
  static void DebugIf(bool condition, std::format_string<Args...> fmt,
                      Args&&... args) {
    if (condition) {
      spdlog::debug(std::format(fmt, std::forward<Args>(args)...));
    }
  }

  template <typename... Args>
  static void InfoIf(bool condition, std::format_string<Args...> fmt,
                     Args&&... args) {
    if (condition) {
      spdlog::info(std::format(fmt, std::forward<Args>(args)...));
    }
  }

  template <typename... Args>
  static void WarnIf(bool condition, std::format_string<Args...> fmt,
                     Args&&... args) {
    if (condition) {
      spdlog::warn(std::format(fmt, std::forward<Args>(args)...));
    }
  }

  template <typename... Args>
  static void ErrorIf(bool condition, std::format_string<Args...> fmt,
                      Args&&... args) {
    if (condition) {
      spdlog::error(std::format(fmt, std::forward<Args>(args)...));
    }
  }

  template <typename... Args>
  static void FatalIf(bool condition, std::format_string<Args...> fmt,
                      Args&&... args) {
    if (condition) {
      spdlog::critical(std::format(fmt, std::forward<Args>(args)...));
    }
  }
};

} // namespace leanstore