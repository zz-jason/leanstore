#pragma once

#include "leanstore/StoreOption.hpp"

#include <format>
#include <mutex>

#ifdef DEBUG
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

  static void Init(const StoreOption& option);

  static void DebugCheck(bool condition, const std::string& msg = "");

  static void Debug(const std::string& msg);

  static void Info(const std::string& msg);

  static void Warn(const std::string& msg);

  static void Error(const std::string& msg);

  static void Fatal(const std::string& msg);

  template <typename... Args>
  static void DebugCheck(bool condition, std::format_string<Args...> fmt, Args&&... args) {
    DebugCheck(condition, std::format(fmt, std::forward<Args>(args)...));
  }

  template <typename... Args>
  static void Debug(std::format_string<Args...> fmt, Args&&... args) {
    Debug(std::format(fmt, std::forward<Args>(args)...));
  }

  template <typename... Args>
  static void Info(std::format_string<Args...> fmt, Args&&... args) {
    Info(std::format(fmt, std::forward<Args>(args)...));
  }

  template <typename... Args>
  static void Warn(std::format_string<Args...> fmt, Args&&... args) {
    Warn(std::format(fmt, std::forward<Args>(args)...));
  }

  template <typename... Args>
  static void Error(std::format_string<Args...> fmt, Args&&... args) {
    Error(std::format(fmt, std::forward<Args>(args)...));
  }

  template <typename... Args>
  static void Fatal(std::format_string<Args...> fmt, Args&&... args) {
    Fatal(std::format(fmt, std::forward<Args>(args)...));
  }
};

} // namespace leanstore
