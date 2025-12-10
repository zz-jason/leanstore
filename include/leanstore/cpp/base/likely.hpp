#pragma once

#if defined(__GNUC__) || defined(__clang__)
#define LEAN_LIKELY(x) (__builtin_expect(!!(x), 1))
#define LEAN_UNLIKELY(x) (__builtin_expect(!!(x), 0))
#else
#define LEAN_LIKELY(x) (x)
#define LEAN_UNLIKELY(x) (x)
#endif