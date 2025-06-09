#pragma once

namespace leanstore {
class Thread;
class Coroutine;
class CoroFuture;
} // namespace leanstore

namespace leanstore::env {

extern void Init();
extern void Deinit();

extern Thread* CurrentThread();
extern Coroutine* CurrentCoro();

} // namespace leanstore::env