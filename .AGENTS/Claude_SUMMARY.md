# Claude Summary - Optimize clang-tidy in ci-local-check.sh

## Problem
The `run_clang_tidy_check` function in `scripts/ci-local-check.sh` was checking all C++ files on every commit/push, causing long wait times during pre-commit and pre-push hooks.

## Solution
Implemented a layered checking strategy with caching and parallel execution:

### 1. Layered Checking Modes
- `--mode staged`: Check only staged files (for pre-commit) - instant feedback
- `--mode branch`: Check files changed vs main branch (for pre-push) - comprehensive but scoped

### 2. Incremental Cache
- Cache location: `.cache/clang-tidy/`
- Cache key: `file_content_hash + clang_tidy_config_hash`
- Passed files are cached and skipped on subsequent runs
- Failed files are always rechecked

### 3. Parallel Execution
- Uses `nproc` to determine parallel job count
- Implemented via `xargs -P` with a helper script
- Results collected and processed after all jobs complete

### 4. `--tidy-only` Option
- Skip preset builds/tests when only clang-tidy check is needed
- Useful for quick pre-commit validation

## Files Modified
- `scripts/ci-local-check.sh` - Main implementation
- `scripts/git-hooks/pre-commit` - Use `--mode staged --tidy-only`
- `scripts/git-hooks/pre-push` - Use `--mode branch`
- `.git/hooks/pre-commit` - Synced with source
- `.git/hooks/pre-push` - Synced with source

## Performance Results
| Scenario | Before | After |
|----------|--------|-------|
| First run (40 files) | ~5-10 min (serial) | 32s (parallel) |
| Cached run | N/A | 4.4s |
| Pre-commit (staged) | Full check | Only staged files |

## Known Issues
- `tests/cpp/zipfian_generator_test.cpp` has a pre-existing clang-tidy error (header not found) - not caused by this change
