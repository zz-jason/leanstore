#pragma once

#include <cassert>
#include <cstdio>
#include <exception>
#include <string>

#define Generic_Exception(name)                                                                    \
  struct name : public std::exception {                                                            \
    const std::string msg;                                                                         \
    explicit name() : msg(#name) {                                                                 \
      printf("Throwing exception: %s\n", #name);                                                   \
    }                                                                                              \
    explicit name(const std::string& msg) : msg(msg) {                                             \
      printf("Throwing exception: %s(%s)\n", #name, msg.c_str());                                  \
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

#ifdef MACRO_CHECK_DEBUG
#define DEBUG_BLOCK() if (true)
#define RELEASE_BLOCK() if (true)
#define BENCHMARK_BLOCK() if (true)
#else
#define DEBUG_BLOCK() if (false)
#ifdef MACRO_CHECK_RELEASE
#define RELEASE_BLOCK() if (true)
#define BENCHMARK_BLOCK() if (true)
#else
#define RELEASE_BLOCK() if (false)
#ifdef MACRO_CHECK_BENCHMARK
#define BENCHMARK_BLOCK() if (true)
#else
#define BENCHMARK_BLOCK() if (false)
#endif
#endif
#endif

template <typename T>
inline void DoNotOptimize(const T& value) {
#if defined(__clang__)
  asm volatile("" : : "g"(value) : "memory");
#else
  asm volatile("" : : "i,r,m"(value) : "memory");
#endif
}
