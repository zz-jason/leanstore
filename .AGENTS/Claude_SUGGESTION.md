# Claude Suggestions for AGENTS.md

## Efficiency Improvements

### 1. Add CI Script Documentation
Consider adding a section about `scripts/ci-local-check.sh` options:
```markdown
## CI Local Check Options
- `--mode staged`: Check only staged files (fast, for pre-commit)
- `--mode branch`: Check files changed vs main (for pre-push)
- `--tidy-only`: Run only clang-tidy, skip builds/tests
- `--no-cache`: Disable clang-tidy cache
```

### 2. Cache Directory in .gitignore
Ensure `.cache/` is in `.gitignore` to prevent committing clang-tidy cache files.

### 3. Mention Skip Options
Add documentation about skipping hooks when needed:
```markdown
## Skipping Hooks
- Pre-commit: `git commit --no-verify` or `LEANSTORE_SKIP_PRECOMMIT=1`
- Pre-push: `LEANSTORE_SKIP_PREPUSH=1 git push`
```

## Token Reduction

### 1. Consolidate Duplicate Sections
The "For Coding Task" and "For Coding Tasks" headers seem duplicated - consider merging.

### 2. Link to Detailed Docs
Instead of inline instructions, consider linking to a separate `docs/development.md` file for verbose build/test instructions.

## Comfort Improvements

### 1. Quick Reference Card
Add a quick reference section at the top:
```markdown
## Quick Reference
| Action | Command |
|--------|---------|
| Build | `cmake --build build/debug_coro -j $(nproc)` |
| Test | `ctest --test-dir build/debug_coro -j $(nproc)` |
| Format | `cmake --build build/debug_coro --target format` |
| Tidy | `cmake --build build/debug_coro --target check-tidy` |
```

### 2. Pre-existing Issues Documentation
Document known pre-existing issues (like the `zipfian_generator_test.cpp` header issue) so agents don't waste time debugging them.
