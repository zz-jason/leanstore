#pragma once

#include "leanstore/coro/coro_lock_guard.hpp"
#include "leanstore/coro/coro_mutex.hpp"

namespace leanstore {

using LeanMutex = leanstore::CoroMutex;
using LeanSharedMutex = leanstore::CoroSharedMutex;

template <typename T>
using LeanUniqueLock = leanstore::CoroUniqueLock<T>;
template <typename T>
using LeanSharedLock = leanstore::CoroSharedLock<T>;

} // namespace leanstore

//------------------------------------------------------------------------------
// Unique/shared lock macros
//------------------------------------------------------------------------------
#define LEAN_LOCK_DECL_IMPL_IMPL(mutex, LINE) guard_##LINE(mutex)
#define LEAN_LOCK_DECL_IMPL(mutex, LINE) LEAN_LOCK_DECL_IMPL_IMPL(mutex, LINE)
#define LEAN_LOCK_DECL(mutex) LEAN_LOCK_DECL_IMPL(mutex, __LINE__)

#define LEAN_UNIQUE_LOCK(mutex) LeanUniqueLock<std::decay_t<decltype(mutex)>> LEAN_LOCK_DECL(mutex)
#define LEAN_SHARED_LOCK(mutex) LeanSharedLock<std::decay_t<decltype(mutex)>> LEAN_LOCK_DECL(mutex)
