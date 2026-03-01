# KNOWLEDGE_BASE

Use this file for factual, project-specific knowledge that improves coding-agent performance.

## System Overview

- High-level architecture: foundation (`base/io/sync`) -> buffer and WAL (`buffer/wal`) -> B+Tree (`btree`) -> transaction (`tx`) -> API (`c`, `lean_store.hpp`, `table`).
- Core modules/services: coroutine runtime (`coro`), transaction manager (`tx_manager`), concurrency control and history storage (`tx/concurrency_control`, `tx/history_storage`), group commit + logging (`tx/group_committer`, WAL), B-tree generic engine.
- Critical data flows: write path goes through session/tx -> KV/B-tree update -> WAL/group commit -> visibility/commit tree update -> GC by watermarks.

## Invariants and Constraints

- LeanStore runtime target is Linux with `libaio`; treat `libaio` as required runtime/build dependency.
- Keep test parallelism at `ctest -j 4` by default to avoid AIO exhaustion (`io_setup EAGAIN`) in common dev environments.
- LeanStore now runs coroutine-only; `LEAN_ENABLE_CORO` build/runtime gating and thread-mode fallbacks were removed.
- Session/tree operations in C++ API must execute on the assigned worker thread via sync submit pattern (`SubmitAndWait`/`ExecSync`).

## Decisions and Rationale

| Date | Decision | Reason | Impact |
|------|----------|--------|--------|
| 2026-01-23 | Use `ctest -j 4` instead of `-j $(nproc)` | Avoid Linux AIO `io_setup EAGAIN` in parallel tests | Stable local and CI-like test runs |
| 2026-01-31 | Use `KVVariant` + `std::visit` in table paths | Remove legacy KV interface indirection | Cleaner dispatch and clearer type ownership |
| 2026-02-02 | Add coroutine-gated C++ API (`LeanStore/LeanSession/LeanBTree/LeanCursor`) | Provide high-level API aligned with C API style | New C++ entry points in `include/leanstore/lean_store.hpp` |
| 2026-02-04 | C++ API operations must execute on worker thread via `SubmitAndWait`/`ExecSync` | Preserve thread/coroutine affinity and correctness | Session/tree operations are worker-bound and safer |
| 2026-02-27 | Keep `lean_store.hpp` decoupled from `lean_session/lean_btree/lean_cursor` headers | Avoid circular include chains through buffer/btree headers | `Connect/TryConnect` implementations moved to `src/lean_store.cpp` |
| 2026-02-27 | `LeanSession::ExecSync` must support move-only return types | `LeanCursor` is move-only (owns `std::unique_ptr<BTreeIter>`) | `ExecSync` now captures non-void returns via `std::optional<T>` |
| 2026-02-27 | Compile new C++ API sources unconditionally | Runtime is coroutine-only; no dual-mode build remains | `src/CMakeLists.txt` always includes `lean_session/lean_btree/lean_cursor` |
| 2026-02-27 | Keep `LeanCursor` operations local instead of per-call `SubmitAndWait` hopping | Cursor/iterator lifetime can outlive a submitted coroutine frame and trip `JumpContext` assertions | `OpenCursor` and cursor navigation/access run directly on held iterator object |
| 2026-02-27 | `LeanSession` must be move-only | Session wrapper owns one reserved `CoroSession*`; copying can double-release into scheduler pool | Copy ctor/assign deleted, move ctor/assign implemented in `lean_session` |
| 2026-02-27 | `TryConnect` should return LeanStore `Optional<LeanSession>` | Keep public API aligned with project wrappers and avoid direct `std::optional` exposure | `LeanStore::TryConnect` now returns `Optional<LeanSession>` |
| 2026-02-27 | Compile tx tests unconditionally in coroutine mode | Refactored tx tests use coroutine-only C++ API wrappers (`LeanSession/LeanBTree`) | `tests/CMakeLists.txt` always includes `leanstore_add_test_in_dir(tx)` |
| 2026-02-27 | `LeanBTree` handles are session-bound in the new API | Each handle executes via its owning `LeanSession::ExecSync`; cross-session checks need per-session `GetBTree` handles | Refactored tx tests create/fetch one handle per session |
| 2026-02-27 | Remove `LEAN_ENABLE_CORO` option and thread-mode branches | Project direction is coroutine-only runtime and test matrix | Core source/tests now compile and run without coro/thread `#ifdef` forks; `release_thread` preset removed |
| 2026-02-28 | Remove `debug_tsan` preset and tsan CI lane | Thread-mode-only validation lane is no longer part of the project matrix | `CMakePresets.json`, CI workflow, and local CI helper removed tsan-based checks |
| 2026-02-28 | Remove `debug_cov` preset and duplicate coverage lane | `debug_cov` and `debug_coro` became identical in coroutine-only mode | CI/local checks now run on `debug_coro` only; duplicate `[CORO] UT-coverage` job removed |
| 2026-02-28 | Remove unused sanitizer CMake toggles | `ENABLE_ASAN`/`ENABLE_TSAN` had no active preset or workflow consumer | Sanitizer option branches removed from top-level `CMakeLists.txt` |
| 2026-02-28 | `LeanCursor::CurrentKey()` must call `AssembleKey()` before `Key()` | `BTreeIterPessistic::Key()` expects assembled key bytes in the internal buffer | C++ cursor API now assembles key eagerly before returning `Slice` |
| 2026-02-28 | Keep C API cursor bridges on iterator wrappers (`CursorImpl`/`CursorMvccImpl`) for now | Directly wrapping `LeanCursor` in C bridge regressed JumpContext/iterator lifecycle behavior in C cursor tests | C bridge uses existing per-call iterator wrapper logic while C store/session paths still use `LeanStore::TryConnect` + `LeanSession` |
| 2026-02-28 | `LeanSession::Close()` must tolerate store shutdown order | Recovery tests may close store before deferred session handle destruction | `Close()` releases CoroSession only when scheduler is still alive |
| 2026-02-28 | Avoid nested `LeanSession::ExecSync` in tests | Calling `session.CreateBTree()` inside an outer `session.ExecSync()` deadlocks because both wait on the same session future | `tests/table/column_store_test.cpp` now performs `CreateBTree` outside the `ExecSync` block and keeps only in-worker table operations inside |
| 2026-02-28 | Prefer C table/session API in column-store tests to avoid direct scheduler plumbing | `LeanSession::ExecSync` is an internal execution primitive; test logic should use stable public table/session interfaces | `tests/table/column_store_test.cpp` now uses `lean_open_store`/`lean_session`/`lean_table` APIs only |
| 2026-02-28 | Remove redundant C-bridge `SessionImpl::ExecSync` helper | Forwarding wrappers duplicate `LeanSession` execution primitive and obscure migration progress to direct high-level APIs | C bridge B-tree/cursor paths now call `Session().ExecSync(...)` directly and `SessionImpl::ExecSync` is deleted |
| 2026-02-28 | Remove single-use `LeanStore::HasCoroScheduler()` helper | The helper had one call site and duplicated a direct null check on the owned scheduler pointer | `LeanSession::Close()` now checks `store_->coro_scheduler_ != nullptr` (friend access) before releasing the session |

## Pitfalls and Failure Modes

- Running tests with high parallelism: keep `ctest` at `-j 4` unless resource limits are tuned.
- Assuming examples are part of main build: `examples/c` and `examples/cpp` are standalone consumer projects.
- Treating `libaio` as vcpkg-only: it is a system dependency on Linux and must be available.
- Using `debug_coro` for package distribution: this preset may include coverage flags that can break downstream linking.
- Using non-existent coroutine state `CoroState::kWaiting` in tests: valid yield states are `kReady`, `kWaitingMutex`, `kWaitingIo`, and `kDone`.
- In coroutine tests, waiting on producer futures from a coroutine sharing producer session can deadlock; wait from a host thread or a dedicated non-conflicting execution context.

## Operational Knowledge

- Local setup caveats: install `libaio` and configure with `cmake --preset debug_coro` for daily development.
- Build/test caveats: prefer `cmake --build build/debug_coro -j $(nproc)` then `ctest --test-dir build/debug_coro --output-on-failure -j 4`.
- Validation caveats: run format and tidy checks; if tidy failures are pre-existing, document them explicitly with scope.
- Hook caveats: pre-commit/pre-push tidy flow relies on `debug_coro` compile commands (`cmake --preset debug_coro`).
- Distribution caveats: prefer `release_coro` when validating installed-package consumption.
- Build-mode caveats: validate coroutine presets (`debug_coro`, `release_coro`); there is no non-coro preset.
- OpenCode CLI in this environment has no built-in scheduling subcommands; background task scheduling must invoke `opencode run` via OS schedulers (`cron`, `systemd --user` timers, or `at`).
- The local `.agents/skills/scheduled-task` workflow uses named scheduler entries (`opencode-scheduled-<task-name>` / `opencode-scheduled:<task-name>`) plus `~/.local/share/opencode-scheduled/registry.tsv` so tasks can be listed and canceled reliably.

## Glossary

- MVCC: Multi-Version Concurrency Control used for visibility and transactional reads/writes.
- WAL: Write-Ahead Logging used for durability and recovery.
- CommitTree: per-worker commit timeline used by visibility checks.
- LCB: last committed-before value used in cross-worker visibility lookup.
- KVVariant: `std::variant<BasicKV*, TransactionKV*>` dispatch carrier.
