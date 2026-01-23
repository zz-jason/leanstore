#pragma once

#ifdef LEAN_ENABLE_CORO

#include "leanstore/coro/coro_lock_guard.hpp"
#include "leanstore/coro/coro_mutex.hpp"

#else

#include <mutex>
#include <shared_mutex>

#endif

namespace leanstore {

#ifdef LEAN_ENABLE_CORO

using LeanMutex = leanstore::CoroMutex;
using LeanSharedMutex = leanstore::CoroSharedMutex;

template <typename T>
using LeanUniqueLock = leanstore::CoroUniqueLock<T>;
template <typename T>
using LeanSharedLock = leanstore::CoroSharedLock<T>;

#else

using LeanMutex = std::mutex;
using LeanSharedMutex = std::shared_mutex;

template <typename T>
using LeanUniqueLock = std::unique_lock<T>;
template <typename T>
using LeanSharedLock = std::shared_lock<T>;

#endif

} // namespace leanstore

//------------------------------------------------------------------------------
// Unique/shared lock macros
//------------------------------------------------------------------------------
#define LEAN_LOCK_DECL_IMPL_IMPL(mutex, LINE) guard_##LINE(mutex)
#define LEAN_LOCK_DECL_IMPL(mutex, LINE) LEAN_LOCK_DECL_IMPL_IMPL(mutex, LINE)
#define LEAN_LOCK_DECL(mutex) LEAN_LOCK_DECL_IMPL(mutex, __LINE__)

#define LEAN_UNIQUE_LOCK(mutex) LeanUniqueLock<std::decay_t<decltype(mutex)>> LEAN_LOCK_DECL(mutex)
#define LEAN_SHARED_LOCK(mutex) LeanSharedLock<std::decay_t<decltype(mutex)>> LEAN_LOCK_DECL(mutex)
