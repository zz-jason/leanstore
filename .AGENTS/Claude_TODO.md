# Claude TODO - Optimize clang-tidy in ci-local-check.sh

## Task
Optimize clang-tidy checks in `scripts/ci-local-check.sh` to reduce time spent during pre-commit and pre-push hooks.

## Completed Items
- [x] Modify `ci-local-check.sh` to support layered checking modes (`--mode staged|branch`)
- [x] Add `--tidy-only` option to skip preset builds/tests
- [x] Implement incremental cache mechanism (file hash + config hash based)
- [x] Add parallel execution using `nproc` jobs
- [x] Update pre-commit hook to use `--mode staged --tidy-only`
- [x] Update pre-push hook to use `--mode branch`
- [x] Update `scripts/git-hooks/pre-commit` and `scripts/git-hooks/pre-push` source files
- [x] Verify script correctness and performance

## Results
| Scenario | Time |
|----------|------|
| 40 files parallel check (first run) | 32s |
| Cache hit (39 cached, 1 recheck) | 4.4s |
| Staged mode (no staged files) | <1s |
