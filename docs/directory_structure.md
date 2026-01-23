# LeanStore Directory Structure

This document outlines the directory structure of LeanStore, reflecting its 5-layer software architecture. The structure separates public APIs (in `include/`) from internal implementation details (in `src/`), and organizes tests (in `tests/`) to mirror the source structure.

## Architectural Layers

LeanStore follows a 5-layer architecture:

| Layer | Name | Components |
|-------|------|------------|
| 0 | I/O | Async I/O, File Operations |
| 1 | Storage | Buffer Pool, WAL, Recovery, Checkpoint |
| 2 | Index | B+ Tree (BTreeGeneric, BTreeNode) |
| 3 | Transaction | MVCC, Concurrency Control, Coroutines |
| 4 | API | C API, C++ KV/Table API |

## Directory Layout

### Public Headers (`include/leanstore/`)

All public C++ headers are located in `include/leanstore/`. The C API is located in `include/leanstore/c/`.

*   **`leanstore.hpp`**: Main public header/facade.
*   **`kv_interface.hpp`**: KV interface definitions.
*   **`exceptions.hpp`**: Exception types.
*   **`c/`**: Layer 4: C Language API (contains `leanstore.h`, `types.h`, `status.h`, `wal_record.h`).
*   **`base/`**: Layer -1: C++ Foundation (Error handling, Result, Slice, Logging, etc.).
*   **`sync/`**: Cross-cutting synchronization primitives (HybridMutex, Guards).
*   **`io/`**: Layer 0: I/O Backend (Async I/O, File Writer).
*   **`buffer/`**: Layer 1: Buffer Management (Buffer Manager, Frames, Page Eviction).
*   **`wal/`**: Layer 1: Write-Ahead Logging (WAL Entry, Payload).
*   **`recovery/`**: Layer 1: Recovery (Redo/Undo logic).
*   **`checkpoint/`**: Layer 1: Checkpointing.
*   **`config/`**: Configuration options.
*   **`btree/`**: Layer 2: B+ Tree Index (pure index, generic implementation).
*   **`tx/`**: Layer 3: Transaction & Concurrency (TxManager, MVCC, Concurrency Control, Recovery).
*   **`coro/`**: Layer 3: Coroutine Framework (Scheduler, Executor, Primitives).
*   **`table/`**: Layer 4: Logical Table API (Table abstraction, Registry, Schema).
*   **`utils/`**: Cross-cutting utilities (Random generators, JSON, Thread management).

### Source Code (`src/`)

Internal implementation details are located in `src/` and are not exposed via public headers.

*   **`lean_store.cpp`**: Main store implementation.
*   **`api/`**: API implementations (`c/`, `cpp/`).
*   **`buffer/`**: Buffer manager implementation.
*   **`wal/`**: WAL implementation.
*   **`recovery/`**: Recovery implementation.
*   **`checkpoint/`**: Checkpoint implementation.
*   **`btree/`**: B+ Tree core implementation.
*   **`tx/`**: Transaction and concurrency control implementation.
*   **`coro/`**: Coroutine framework implementation.
*   **`table/`**: Table API implementation.
*   **`utils/`**: Utility implementations.
*   **`telemetry/`**: Metrics and telemetry (Internal).
*   **`failpoint/`**: Failure injection for testing (Internal).

### Tests (`tests/`)

Tests are organized to mirror the source directory structure.

*   **`base/`**: Tests for foundation components (`error`, `result`, `zipfian`, etc.).
*   **`sync/`**: Synchronization tests.
*   **`buffer/`**: Buffer manager tests.
*   **`btree/`**: B+ Tree tests.
*   **`coro/`**: Coroutine and coroutine-integrated tests.
*   **`tx/`**: Transaction, MVCC, and Concurrency tests.
*   **`recovery/`**: Recovery tests.
*   **`table/`**: Table API tests.
*   **`c/`**: C API tests.
*   **`common/`**: Shared test utilities (`lean_test_suite.hpp`, `tx_kv.hpp`).

## Design Principles

1.  **Flat Hierarchy**: Prefer shallow directory structures (e.g., `include/leanstore/base/`) over deep nesting.
2.  **C++ by Default**: C++ headers are placed directly under module directories. C API headers are isolated in `c/`.
3.  **Snake_case**: All directory names use `snake_case`.
4.  **Separation of Concerns**: Clear separation between Public API (`include/`) and Internal Implementation (`src/`).
