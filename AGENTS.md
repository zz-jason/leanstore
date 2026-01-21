# The LeanStore Project

LeanStore is a high-performance B-tree based key-value storage library optimized
for NVMe SSD and multi-core CPUs. Larger-than-memory database system with full
ACID transaction support.

## Quick Reference

| Action | Command |
|--------|---------|
| Configure | `cmake --preset debug_coro` |
| Build | `cmake --build build/debug_coro -j $(nproc)` |
| Test | `ctest --test-dir build/debug_coro --output-on-failure -j $(nproc)` |
| Format | `cmake --build build/debug_coro --target format` |
| Tidy | `cmake --build build/debug_coro --target check-tidy` |

## **MANDATORY** Agent Workflow

**For Every Task:**

- Write TODOs to `.AGENTS/${AGENT_NAME}_TODO.md`
- After task completion, write a brief summary to `.AGENTS/${AGENT_NAME}_SUMMARY.md`
- Write improvement suggestions for AGENTS.md to `.AGENTS/${AGENT_NAME}_SUGGESTION.md` (focus on efficiency, comfort, token reduction)
- Append newly gained knowledge to `.AGENTS/KNOWLEDGE_BASE.md`

**For Coding Tasks:**

- Run the `format` cmake target before commit
- Ensure all code builds and tests pass under the `debug_coro` preset
- Ensure `check-tidy` passes under the `debug_coro` preset (may require fixing existing files)
- Use conventional commit messages (see `.github/workflows/conventional-commits.yml`), and always append a `Co-Authored-By` trailer identifying the AI agent
- Use `file_path:line_number` format when referencing code (e.g., `concurrency_control.cpp:132`)

## Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `LEAN_ENABLE_CORO` | ON | Enable coroutine support |
| `LEAN_ENABLE_TESTS` | ON | Build tests |



## CI Local Check Script

The `scripts/ci-local-check.sh` script runs CI checks locally:

| Option | Description |
|--------|-------------|
| `--mode staged` | Check only staged files (for pre-commit) |
| `--mode branch` | Check files changed vs main (for pre-push) |
| `--tidy-only` | Run only clang-tidy, skip builds/tests |
| `--no-cache` | Disable clang-tidy result caching |

**Skipping Hooks:**
- Pre-commit: `git commit --no-verify` or `LEANSTORE_SKIP_PRECOMMIT=1`
- Pre-push: `LEANSTORE_SKIP_PREPUSH=1 git push`

## Shared Build Scripts

| Script | Purpose |
|--------|---------|
| `scripts/build-wiredtiger.sh` | WiredTiger build (used by Dockerfile and cmake) |

When modifying external dependency builds, update the shared script to maintain consistency.

## Codebase Navigation

**For transaction/MVCC analysis, read in this order:**
1. `include/leanstore/tx/concurrency_control.hpp` - visibility checks
2. `include/leanstore/tx/tx_manager.hpp` - transaction lifecycle
3. `include/leanstore/tx/history_storage.hpp` - version storage
4. `include/leanstore/coro/mvcc_manager.hpp` - timestamp allocation

**Common search patterns:**
| Component | Search Pattern |
|-----------|---------------|
| Visibility check | `VisibleForMe`, `VisibleForAll` |
| Commit protocol | `CommitTx`, `GroupCommitter` |
| Version storage | `HistoryStorage`, `PutVersion` |
| B-tree operations | `BTreeGeneric`, `BTreeNode` |

## Known Issues

- `tests/cpp/zipfian_generator_test.cpp`: clang-tidy reports `'lean_test_suite.hpp' file not found` - pre-existing include path issue

## Knowledge Base

Learn from previously accumulated knowledge in `.AGENTS/KNOWLEDGE_BASE.md`, which contains:
- B-tree implementation details
- Transaction and MVCC architecture
- Coroutine integration patterns
- Key code references with line numbers
