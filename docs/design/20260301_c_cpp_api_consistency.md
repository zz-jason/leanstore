# C/C++ API Alignment (Scope: Issue 1 and 4)

- Co-Authored-By: OpenCode(@zz-jason), gpt-5.3-codex
- Date: 2026-03-01

## Summary

This proposal only addresses two confirmed inconsistencies between LeanStore C and C++ APIs: (1) transaction behavior mismatch, and (4) connect/worker scheduling mismatch. The goal is to make C++ API behavior align with C API behavior by default: auto-commit semantics for single operations when no explicit transaction is active, and round-robin worker selection for default connect paths.

## Background

From the API review, we intentionally narrow scope to two items:

1. **Transaction behavior mismatch (Issue 1)**
   - C API: many data operations are auto-wrapped by `TxGuard` when no explicit tx is active (`src/c/tx_guard.hpp`).
   - C++ API: callers are expected to explicitly call `StartTx/CommitTx/AbortTx` for MVCC-style flows (`include/leanstore/lean_session.hpp`).
   - Result: same insert/remove/lookup usage pattern can behave differently across languages.

2. **Connect scheduling mismatch (Issue 4)**
   - C API `connect()/try_connect()` chooses worker by internal round-robin (`src/c/store_impl.hpp`).
   - C++ API currently defaults to worker 0 via `Connect(uint64_t worker_id = 0)` / `TryConnect(uint64_t worker_id = 0)` (`include/leanstore/lean_store.hpp`).
   - Result: out-of-box C++ session distribution differs from C API.

Use cases supported by this design:
- Existing C users migrating a module to C++ without behavior changes.
- Mixed C/C++ integration tests expecting equivalent semantics.
- Multi-session workloads expecting default load spreading instead of worker-0 concentration.

Out of scope for this round:
- status-code redesign/mapping cleanup
- btree config parity expansion
- table-wrapper parity and broader surface refactors
- encapsulation cleanup not required by Issue 1/4

## Detailed Design

### 1) Align C++ transaction behavior to C API (Issue 1)

#### 1.1 Target semantics

For C++ high-level write/read key-value operations (`LeanBTree` paths):

- If explicit tx is active on session: operate inside current tx.
- If explicit tx is not active: run operation as implicit single-op tx (start/commit; abort on failure), matching C `TxGuard` behavior.

This makes C++ default behavior consistent with C API convenience semantics.

#### 1.2 Affected operations

Apply implicit-tx policy only to C++ `LeanBTree` operations that call KV operations:

- `LeanBTree::Insert`
- `LeanBTree::Remove`
- `LeanBTree::Update`
- `LeanBTree::Lookup`

Notes:
- Explicit transaction APIs remain unchanged: `StartTx`, `CommitTx`, `AbortTx` still available and authoritative.
- When explicit tx is active, implicit wrapper must be no-op (no nested tx lifecycle).

Cursor parity requirement (with C cursor wrappers):
- `LeanCursor` methods themselves do **not** create implicit tx guards.
- For MVCC trees, cursor operations require an active explicit tx (same as C cursor `assert(IsTxStarted())`).
- Cursor visibility/filtering and post-delete iteration behavior must match C cursor wrappers.

#### 1.3 Implementation approach

Do not create a separate C++ auto-tx helper. Reuse the existing `TxGuard` implementation directly for both C bridge and C++ high-level API.

Code organization change:
- Move `TxGuard` from `src/c/tx_guard.hpp` to a shared internal location used by both layers (for example `src/common/tx_guard.hpp`, final path decided during implementation).
- Keep one RAII implementation only.

Pseudo flow inside each `LeanBTree` op:

```cpp
session_->ExecSync([&] {
  lean_status tx_status = lean_status::LEAN_STATUS_OK;
  TxGuard guard(tx_status);
  OpCode code = ...;
  if (code != OpCode::kOK) {
    tx_status = lean_status::LEAN_ERR_CONFLICT; // any non-OK to force abort for implicit tx
    return Error(...);
  }
  return Success;
});
```

Corner cases:
- If user starts tx explicitly, `TxGuard` detects active tx and does not commit/abort.
- On exception/error path, implicit tx aborts.
- Behavior is per operation, consistent with C wrapper pattern.

Cursor-specific corner cases (parity with C):
- On MVCC cursor operations without active tx: fail-fast via debug assertion (same policy as C cursor).
- Cursor traversal in C++ must not expose tuples that C cursor would filter out in MVCC mode.
- `RemoveCurrent` then `Next/Prev` behavior must match C cursor state transitions.

### 2) Align C++ default connect scheduling to C API (Issue 4)

#### 2.1 Target semantics

Default C++ connect behavior should mirror C:
- `Connect()` -> blocking, internal round-robin worker selection.
- `TryConnect()` -> non-blocking, internal round-robin worker selection.

#### 2.2 API shape changes

Current C++ API uses default arg `worker_id = 0`. To align with C API behavior, simplify to default-only connect APIs:

```cpp
auto Connect() -> LeanSession;
auto TryConnect() -> Optional<LeanSession>;
```

Compatibility policy:
- Do **not** keep backward-compatibility overloads.
- Remove `Connect(uint64_t worker_id = 0)` and `TryConnect(uint64_t worker_id = 0)` directly.
- C++ public API keeps only default round-robin: `Connect()` / `TryConnect()`.
- Update all in-repo callsites (tests/examples/tools/C bridge internals if affected) in the same change.

#### 2.3 Worker selection algorithm

Use the same round-robin counter strategy as C implementation (`src/c/store_impl.hpp`):

```cpp
static std::atomic<uint64_t> runs_on_counter{0};
auto runs_on = runs_on_counter++ % store_option_->worker_threads_;
```

Apply in C++ `Connect()` and `TryConnect()` only.

Behavioral alignment target:
- initial worker selection sequence starts from worker 0
- monotonically increasing global counter
- modulo by `worker_threads_`
- no per-instance/per-callsite custom policy in this round

### 3) Backward compatibility and migration

Phase A (this round):
- Implement Issue 1 and Issue 4 behavior changes only.
- Keep explicit tx APIs available.

There is no compatibility shim for old C++ `Connect(worker_id)` / `TryConnect(worker_id)` signatures in this scope.

### 4) Validation plan

Add/adjust tests to prove C/C++ equivalence for scoped items:

1. **Implicit tx parity**
   - C++ insert/remove/lookup without explicit tx should succeed/fail with same semantics as C wrappers.
2. **Explicit tx precedence**
   - C++ with explicit `StartTx` should not auto-commit per op; commit visibility follows explicit boundaries.
3. **Round-robin connect parity**
   - repeated `Connect()`/`TryConnect()` in C++ should distribute sessions across workers similarly to C behavior.
4. **Regression**
   - existing explicit worker-id callsites are migrated to default connect semantics.
5. **Cursor parity**
   - C++ cursor seek/next/prev/current/remove behavior matches C cursor on both atomic and MVCC trees.
   - Add side-by-side C vs C++ cursor tests for visibility and post-remove iteration behavior.

## Alternative Designs Considered

1. **Keep C++ explicit-tx-only semantics**
   - Rejected because user requirement is to align C++ behavior with C API.

2. **Change C API to explicit-tx-only instead of changing C++**
   - Rejected for this round; opposite direction from requested alignment and likely higher break risk for C consumers.

3. **Keep `Connect(worker_id=0)` and document worker-0 default**
   - Rejected; documentation does not solve behavior mismatch or load skew.

4. **Silent semantic changes while keeping old `Connect(worker_id)` signature**
   - Rejected; removing worker-id overloads makes C++ surface unambiguous and C-aligned.

5. **Provide new explicit-worker C++ APIs (`ConnectOnWorker/TryConnectOnWorker`)**
   - Rejected by scope decision; we only keep C-aligned default round-robin entry points.

6. **Keep old C++ `Connect(worker_id)` overloads as deprecated wrappers**
   - Rejected by scope decision; direct API alignment and cleanup is preferred over temporary compatibility layer.

## Unresolved Questions

1. None for scoped changes.
