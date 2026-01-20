# LeanStore Knowledge Base

## B-tree Implementation
- **Core files**: `include/leanstore/btree/core/b_tree_generic.hpp`, `src/btree/core/b_tree_generic.cpp`
- **Node structure**: `BTreeNode` in `b_tree_node.hpp`
- **Iterators**: `BTreeIterMut` (mutating), `BTreeIterPessimistic` (pessimistic locking)
- **KV interfaces**: `BasicKV` (non‑transactional), `TransactionKV` (MVCC‑enabled)

## Transaction and MVCC
- **Transaction manager**: `TxManager` per worker thread (`include/leanstore/concurrency/tx_manager.hpp`)
- **Concurrency control**: `ConcurrencyControl` per TxManager, contains `HistoryStorage` and `CommitTree`
- **MVCC manager**: `MvccManager` global singleton (`src/coroutine/mvcc_manager.hpp`) allocates timestamps and maintains global watermarks

### Version Storage
- **HistoryStorage** (`include/leanstore/concurrency/history_storage.hpp`): per‑worker storage of update and remove versions.
- **Two B‑trees**: `update_index_` (update versions), `remove_index_` (remove versions/tombstones).
- **Version key**: `(tx_id, command_id)` where `command_id` includes a remove mark bit.
- **VersionMeta**: metadata containing `tree_id_` and payload.

### Visibility Check
- **VisibleForMe** (`concurrency_control.cpp:132`): checks if a tuple written by `worker_id` in `tx_id` is visible to the current transaction.
  - Same worker → always visible.
  - Uses global watermark `global_wmk_of_all_tx_` (copied at transaction start).
  - LCB cache (`lcb_cache_key_`, `lcb_cache_val_`) per worker.
  - If cache miss, queries `Other(worker_id).commit_tree_.Lcb(start_ts)`.
- **VisibleForAll** (`concurrency_control.cpp:181`): checks if `tx_id` is visible to all active transactions (i.e., below global watermark `wmk_of_all_tx_`).
- **CommitTree**: per‑worker log of `(commit_ts, start_ts)` pairs. `Lcb(start_ts)` returns the last transaction committed before `start_ts` on that worker.

### Commit Protocol
- **TxManager::CommitTx** (`tx_manager.cpp:90`):
  1. Allocates `commit_ts` if transaction has writes.
  2. Appends `(start_ts, commit_ts)` to local `CommitTree`.
  3. Updates `latest_commit_ts_`.
  4. Places transaction in `tx_to_commit_` (has remote dependencies) or `rfa_tx_to_commit_` (no remote dependencies).
  5. Calls `GarbageCollection()`.
  6. Waits for `commit_ts <= last_committed_usr_tx_` (log flush).
- **GroupCommitter** (`include/leanstore/concurrency/group_committer.hpp`): background thread that batches WAL flushes and determines committable transactions.
- **AutoCommitProtocol** (`src/coroutine/auto_commit_protocol.hpp`): used in coroutine mode for autonomous log flush and commit acknowledgment.

### Garbage Collection
- **ConcurrencyControl::GarbageCollection** (`concurrency_control.cpp:187`):
  1. `UpdateGlobalWmks()`: tries to acquire global watermark lock, computes oldest active transaction IDs, updates global watermarks, then updates each worker's `wmk_of_all_tx_` and `wmk_of_short_tx_`.
  2. `UpdateLocalWmks()`: reads local watermarks.
  3. Purges versions with tx_id ≤ `local_wmk_of_all_tx_`.
  4. Moves tombstones with tx_id in `(local_wmk_of_all_tx_, local_wmk_of_short_tx_]` to graveyard.

## Coroutine Integration
- **CoroExecutor**: each runs a thread with its own `TxManager` and `Logging`.
- **CoroScheduler::Submit** (`src/coroutine/coro_scheduler.hpp`): wraps user lambda into a `Coroutine` and enqueues it to a specific executor.
- **CoroEnv**: thread‑local environment providing `CurTxMgr()`, `CurStore()`, `CurLogging()`.

## Constraints for Moving Operations to User Threads
1. **Thread‑local version storage** – HistoryStorage is per worker thread.
2. **Visibility check dependencies** – LCB cache and CommitTree are indexed by worker ID.
3. **Commit log thread association** – CommitTree append requires a worker‑specific log.
4. **Transaction state publication** – `active_tx_id_` must be published per thread.
5. **GC coordination** – Watermark updates require global lock and per‑worker watermark propagation.
6. **WAL logging** – Each worker has its own Logging instance.
7. **Coroutine environment** – Many functions rely on `CoroEnv` thread‑local variables.
8. **Lock mechanisms** – Hybrid locks may assume coroutine context.
9. **Performance‑consistency trade‑off** – Reducing synchronization may increase cross‑thread coordination costs.

## Useful Commands
- Build with debug_coro preset: `cmake --preset debug_coro && cmake --build build/debug_coro -j $(nproc)`
- Run tests: `ctest --test-dir build/debug_coro --output-on-failure`
- Format code: `cmake --build build/debug_coro --target format`
- Clang‑tidy: `cmake --build build/debug_coro --target check-tidy`

## Key Code References
- `concurrency_control.cpp:132` – `VisibleForMe` implementation
- `concurrency_control.cpp:181` – `VisibleForAll` implementation
- `tx_manager.cpp:90` – `CommitTx` implementation
- `history_storage.cpp:21` – `PutVersion` implementation
- `mvcc_manager.hpp:69` – `AllocUsrTxTs` and `AllocSysTxTs`
- `chained_tuple.hpp:63` – `GetVisibleTuple` method
- `coro_scheduler.hpp` – `Submit` method for task submission

## CI and Development Scripts

### ci-local-check.sh Options
- `--mode staged`: Check only git-staged files (for pre-commit hooks)
- `--mode branch`: Check files changed in current branch vs main (for pre-push hooks)
- `--tidy-only`: Run only clang-tidy check, skip preset builds and tests
- `--no-cache`: Disable clang-tidy result caching

### Clang-tidy Cache
- Location: `.cache/clang-tidy/`
- Cache key: `file_content_sha256 + clang_tidy_config_sha256`
- Passed files are cached and skipped; failed files are always rechecked
- Clear cache: `rm -rf .cache/clang-tidy`

### Git Hooks
- Pre-commit: Uses `--mode staged --tidy-only` for fast feedback
- Pre-push: Uses `--mode branch` for comprehensive branch validation
- Skip pre-commit: `git commit --no-verify` or `LEANSTORE_SKIP_PRECOMMIT=1`
- Skip pre-push: `LEANSTORE_SKIP_PREPUSH=1 git push`

### Known Issues
- `tests/cpp/zipfian_generator_test.cpp` has clang-tidy error: `'lean_test_suite.hpp' file not found` - this is a pre-existing issue with include paths for test files