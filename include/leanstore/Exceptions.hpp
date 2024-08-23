#pragma once

#include <exception>
#include <string>

#include <assert.h>
#include <signal.h>
#include <stdio.h>

//--------------------------------------------------------------------------------------
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
  };                                                                                               \
//--------------------------------------------------------------------------------------
namespace leanstore {
namespace ex {
Generic_Exception(GenericException);
Generic_Exception(EnsureFailed);
Generic_Exception(UnReachable);
Generic_Exception(TODO);
} // namespace ex
} // namespace leanstore
// -------------------------------------------------------------------------------------
#define UNREACHABLE()                                                                              \
  throw leanstore::ex::UnReachable(std::string(__FILE__) + ":" +                                   \
                                   std::string(std::to_string(__LINE__)));
// -------------------------------------------------------------------------------------
#define always_check(e)                                                                            \
  (__builtin_expect(!(e), 0) ? throw leanstore::ex::EnsureFailed(                                  \
                                   std::string(__func__) + " in " + std::string(__FILE__) + "@" +  \
                                   std::to_string(__LINE__) + " msg: " + std::string(#e))          \
                             : (void)0)

#ifdef PARANOID
#define PARANOID(e) always_check(e);
#define PARANOID_BLOCK() if constexpr (true)
#else
#ifdef DEBUG
#define PARANOID_BLOCK() if constexpr (true)
#else
#define PARANOID_BLOCK() if constexpr (false)
#endif
#define PARANOID(e) assert(e)
#endif

#ifdef DEBUG
#define ENSURE(e) assert(e);
#else
#define ENSURE(e) always_check(e)
#endif

#define TODOException()                                                                            \
  throw leanstore::ex::TODO(std::string(__FILE__) + ":" + std::string(std::to_string(__LINE__)));

#define explainIfNot(e)                                                                            \
  if (!(e)) {                                                                                      \
    raise(SIGTRAP);                                                                                \
  };
#define RAISE_WHEN(e)                                                                              \
  if (e) {                                                                                         \
    raise(SIGTRAP);                                                                                \
  };
// -------------------------------------------------------------------------------------
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
// -------------------------------------------------------------------------------------
#ifdef MACRO_COUNTERS_ALL
#define COUNTERS_BLOCK() if constexpr (true)
#else
#define COUNTERS_BLOCK() if constexpr (false)
#endif
// -------------------------------------------------------------------------------------
template <typename T>
inline void DO_NOT_OPTIMIZE(const T& value) {
#if defined(__clang__)
  asm volatile("" : : "g"(value) : "memory");
#else
  asm volatile("" : : "i,r,m"(value) : "memory");
#endif
}

#define POSIX_CHECK(expr)                                                                          \
  if (!(expr)) {                                                                                   \
    perror(#expr);                                                                                 \
    raise(SIGTRAP);                                                                                \
  }
