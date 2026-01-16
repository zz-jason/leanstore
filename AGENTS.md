# AGENTS.md

Quick reference for AI-assisted development in LeanStore.

## Project Overview

LeanStore is a high-performance B-tree based key-value storage library optimized for NVMe SSD and multi-core CPUs. Larger-than-memory database system with full ACID transaction support.

**Key Features**: B-tree index, MVCC, NVMe SSD optimization, multi-core parallel processing.

## Essential Commands

```bash
# Configure and build
cmake --preset debug_coro
cmake --build build/debug_coro -j $(nproc)

# Run tests
ctest --test-dir build/debug_coro --output-on-failure

# Code quality checks
cmake --build build/debug_coro --target format      # Apply formatting
cmake --build build/debug_coro --target check-format # Check formatting
cmake --build build/debug_coro --target check-tidy   # Static analysis

# Local CI validation (run before pushing)
./scripts/ci-local-check.sh

# Git hooks (install once for automated checks)
./scripts/install-git-hooks.sh

# Clean rebuild
rm -rf build/debug_coro && cmake --preset debug_coro
```

## Key Configuration Files

- `.clang-format`: Code formatting rules
- `.clang-tidy`: Static analysis and naming rules  
- `CMakePresets.json`: Build configurations
- `.github/workflows/ci.yml`: CI/CD pipeline
- `vcpkg.json`: Dependency management

## Common File Locations

- Source code: `src/`
- Public headers: `include/leanstore/`
- Tests: `tests/`
- Examples: `examples/`
- Benchmarks: `tools/ycsb/`

### Local CI Validation

Run `./scripts/ci-local-check.sh` before pushing to verify changes will pass CI. This runs the same checks as GitHub Actions across multiple presets (debug_tsan, debug_cov, debug_coro).

## CI Workflow Structure

| Job | CMake Preset | Purpose |
|-----|-------------|---------|
| Clang-Tidy | `debug_tsan` | Static analysis with thread sanitizer |
| [CORO] Clang-Tidy | `debug_coro` | Static analysis with coroutine support |
| UT-coverage | `debug_cov` | Unit tests with code coverage |
| UT-tsan | `debug_tsan` | Thread-sanitizer tests |
| Build Release | `release` | Release build validation |

## Critical Code Conventions

### Clang-Tidy Naming Rules

1. **Constants**: `k` prefix with CamelCase
   ```cpp
   const int kMin = 0;    // Good
   const int min = 0;     // Bad
   ```

2. **Struct/Class Members**: Underscore suffix
   ```cpp
   struct Config {
     int min_;           // Good
     double theta_;      // Good
     int min;            // Bad
   };
   ```

3. **Test Macros**: Add `// NOLINT`
   ```cpp
   TEST(ScrambledZipfGeneratorTest, BasicTest) {  // NOLINT
     // test body
   }
   ```

4. **Designated Initializers**: C++20 requires field names
   ```cpp
   Config config = {.min_ = 0, .max_ = 10, .theta_ = 0.5};  // Good
   Config config = {0, 10, 0.5};                           // Bad
   ```

### Compiler Toolchain

- **Compilation**: GCC for all builds (`CMAKE_CXX_COMPILER: "g++"`)
- **Static Analysis**: Clang-tidy and clang-format
- **Hybrid approach**: GCC optimization + Clang static analysis

GCC-specific flags cause issues with clang-tidy. Already handled:
- `.clang-tidy`: `-Wno-unknown-warning-option`
- `run-clang-tidy-pr.sh`: `-extra-arg=-Wno-error=clobbered`
- `CMakeLists.txt`: Conditional `-Wno-unknown-warning-option`

## AI Assistant Guidelines

### Heuristic Workflow

1. **Validate before commit**: Run `./scripts/ci-local-check.sh`
2. **Fix issues sequentially**: formatting → naming → compilation → tests
3. **Follow naming rules**:
   - Constants: `kCamelCase`
   - Struct members: `suffix_`
   - TEST macros: add `// NOLINT`
   - Initializers: C++20 designated syntax (`.field = value`)
4. **Compiler toolchain**: GCC for compilation, Clang-tidy for static analysis
5. **Test across presets**: debug_tsan, debug_cov, debug_coro
6. **Git hooks auto-run**: Fix errors they report; use `--no-verify` only for debugging

## Quick Troubleshooting

### Clang-format Issues
```bash
cmake --build build/debug_coro --target format      # Fix
cmake --build build/debug_coro --target check-format # Check
```

### Clang-tidy Issues
Check `.clang-tidy` for rules. Common fixes:
- Add `k` prefix to constants
- Add `_` suffix to struct members  
- Add `// NOLINT` to TEST() macros
- Use designated initializers with field names

### Build Failures
- Verify GCC is compiler: `CMAKE_CXX_COMPILER: "g++"`
- Check preset configuration in `CMakePresets.json`
- Rebuild clean: `rm -rf build/<preset> && cmake --preset <preset>`

### Test Failures
```bash
ctest --test-dir build/debug_coro -R <TestName> --output-on-failure
```

### Git Hooks (Automated Quality Enforcement)

Git hooks automatically run quality checks during `git commit` and `git push`.

**Key hooks:**
- `commit-msg`: Validates conventional commit format
- `pre-commit`: Quick formatting and compilation check  
- `pre-push`: Full CI validation (`./scripts/ci-local-check.sh`)

**For AI assistants:**
1. Hooks run automatically - if they fail, fix the reported issues
2. Use `--no-verify` only for debugging hook issues
3. Pre-commit is fast, pre-push is comprehensive (same as CI)
4. Errors provide clear guidance - follow them

**Installation:** Already installed in dev container. See `scripts/install-git-hooks.sh`.