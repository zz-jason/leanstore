#pragma once

#include "leanstore/base/log.hpp"

#include <cassert>
#include <cstdio>
#include <exception>
#include <string>

#define Generic_Exception(name)                                                                    \
  struct name : public std::exception {                                                            \
    const std::string msg;                                                                         \
    explicit name() : msg(#name) {                                                                 \
      Log::Warn("Throwing exception: {}", #name);                                                  \
    }                                                                                              \
    explicit name(const std::string& msg) : msg(msg) {                                             \
      Log::Warn("Throwing exception: {}({})", #name, msg);                                         \
    }                                                                                              \
    ~name() = default;                                                                             \
    virtual const char* what() const noexcept override {                                           \
      return msg.c_str();                                                                          \
    }                                                                                              \
  };

namespace leanstore::ex {
Generic_Exception(GenericException);
Generic_Exception(EnsureFailed);
Generic_Exception(UnReachable);
Generic_Exception(TODO);
} // namespace leanstore::ex

#define UNREACHABLE()                                                                              \
  throw leanstore::ex::UnReachable(std::string(__FILE__) + ":" +                                   \
                                   std::string(std::to_string(__LINE__)));

#define always_check(e)                                                                            \
  (__builtin_expect(!(e), 0) ? throw leanstore::ex::EnsureFailed(                                  \
                                   std::string(__func__) + " in " + std::string(__FILE__) + "@" +  \
                                   std::to_string(__LINE__) + " msg: " + std::string(#e))          \
                             : (void)0)

#ifdef DEBUG
#define ENSURE(e) assert(e);
#else
#define ENSURE(e) always_check(e)
#endif

#define TODOException()                                                                            \
  throw leanstore::ex::TODO(std::string(__FILE__) + ":" + std::string(std::to_string(__LINE__)));

#ifdef LEAN_ENABLE_DEBUG_EXEC
#define LEAN_DEXEC() if (true)
#else
#define LEAN_DEXEC() if (false)
#endif

template <typename T>
inline void DoNotOptimize(const T& value) {
#if defined(__clang__)
  asm volatile("" : : "g"(value) : "memory");
#else
  asm volatile("" : : "i,r,m"(value) : "memory");
#endif
}
