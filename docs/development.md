# LeanStore Development Guide

This document provides a comprehensive guide for developers contributing to LeanStore. It covers environment setup, build processes, testing, code quality, and project conventions.

## Development Environment

### Docker-Based Development (Recommended)

LeanStore requires libaio and C++23 support. The easiest way to set up a consistent development environment is using Docker:

```bash
# Build the development container
docker build -f docker/Dockerfile -t leanstore-dev .

# Run with volume mount for source code
docker run -it --rm -v $(pwd):/root/code/leanstore leanstore-dev
```

### VS Code Setup

If using VS Code, install these extensions:
- **C/C++ Extension Pack** – for C++ development support
- **clangd** – for intelligent code completion and navigation

Open the project in the container using VS Code's Remote-Containers extension.

### Toolchain Requirements

- **Compiler**: GCC ≥13, Clang ≥16, or MSVC ≥19.34 (C++23 support)
- **CMake**: ≥3.25
- **vcpkg**: For dependency management (automatically bootstrapped)
- **libaio**: System library for asynchronous I/O (`libaio-dev` on Ubuntu)

## Build Process

### Using CMake Presets

LeanStore provides CMake presets for common configurations:

| Preset | Description | Use Case |
|--------|-------------|----------|
| `debug_coro` | Debug build with coroutine support | **Default for development** |
| `release_coro` | Release build with coroutine support | Performance testing, distribution |
| `debug_cov` | Debug build with coverage instrumentation | Coverage-enabled local validation |

### Basic Build Commands

```bash
# Configure with debug_coro preset (recommended for development)
cmake --preset debug_coro

# Build the project
cmake --build build/debug_coro -j $(nproc)

# Run unit tests
ctest --test-dir build/debug_coro --output-on-failure -j 4

# Install locally (for testing downstream integration)
cmake --install build/debug_coro --prefix ./dist
```

### Building Specific Components

```bash
# Build only the library (no tests/tools)
cmake --build build/debug_coro --target leanstore

# Build and run YCSB benchmark tool
cmake --build build/debug_coro --target ycsb
./build/debug_coro/tools/ycsb/ycsb --ycsb_threads=8 --ycsb_target=basickv
```

## Testing

### Unit Tests

The test suite is organized by component:

```bash
# Run all tests
ctest --test-dir build/debug_coro --output-on-failure

# Run specific test category
ctest --test-dir build/debug_coro --tests-regex "buffer"

# Run single test with verbose output
ctest --test-dir build/debug_coro -R "btree_test" -V
```

### Test Structure

- **Base layer**: `tests/base/` – foundational utilities
- **Buffer manager**: `tests/buffer/` – buffer pool and page management
- **B-tree**: `tests/btree/` – B-tree operations and concurrency
- **Transactions**: `tests/tx/` – MVCC and concurrency control
- **Coroutines**: `tests/coro/` – coroutine-based execution
- **Recovery**: `tests/recovery/` – WAL and crash recovery

### Coverage Reports

To generate code coverage reports (requires `lcov`):

```bash
cmake --preset debug_coro -DLEAN_ENABLE_COVERAGE=ON
cmake --build build/debug_coro -j $(nproc)
ctest --test-dir build/debug_coro
lcov --capture --directory build/debug_coro --output-file coverage.info
genhtml coverage.info --output-directory coverage_report
```

## Code Quality

### Formatting

LeanStore uses `clang-format-18` with a custom `.clang-format` configuration.

```bash
# Check formatting
cmake --build build/debug_coro --target check-format

# Fix formatting automatically
cmake --build build/debug_coro --target format
```

### Static Analysis

`clang-tidy` is integrated into the build system with two approaches:

1. **CMake target** (`check-tidy`): Runs clang-tidy on all source files without caching.
2. **CI script** (`scripts/ci-local-check.sh`): Includes a file-based caching mechanism that skips unchanged files.

```bash
# Run clang-tidy via CMake target (no caching)
cmake --build build/debug_coro --target check-tidy

# Use the CI script with caching (recommended for incremental checks)
scripts/ci-local-check.sh --mode branch --tidy-only
```

The caching mechanism in `ci-local-check.sh` works by:
- Computing a hash of each file's content and the `.clang-tidy` configuration
- Storing results in `.cache/clang-tidy/` directory
- Skipping files where both the file content and config haven't changed
- Cache can be disabled with `--no-cache` flag

### CI Local Checks

The `scripts/ci-local-check.sh` script runs CI checks locally:

```bash
# Check staged files (pre-commit hook)
scripts/ci-local-check.sh --mode staged

# Check files changed vs main (pre-push hook)
scripts/ci-local-check.sh --mode branch

# Run only clang-tidy, skip builds/tests
scripts/ci-local-check.sh --tidy-only

# Disable clang-tidy cache
scripts/ci-local-check.sh --no-cache
```

### Git Hooks

Pre-commit and pre-push hooks are automatically installed:

- **Skip pre-commit**: `git commit --no-verify` or `LEANSTORE_SKIP_PRECOMMIT=1`
- **Skip pre-push**: `LEANSTORE_SKIP_PREPUSH=1 git push`

## Commit Guidelines

### Conventional Commits

LeanStore uses [Conventional Commits](https://www.conventionalcommits.org/) for commit messages. Allowed types are defined in `.github/workflows/conventional-commits.yml`:

```
<type>: <description>

[optional body]

[optional footer(s)]
```

Common types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Test-related changes
- `chore`: Maintenance tasks

### AI-Assisted Development

When using AI assistants for development:

1. **Write TODOs** to `.AGENTS/${AGENT_NAME}_TODO.md`
2. **After completion**, write a brief summary to `.AGENTS/${AGENT_NAME}_SUMMARY.md`
3. **Provide suggestions** for improving AGENTS.md to `.AGENTS/${AGENT_NAME}_SUGGESTION.md`
4. **Append new knowledge** to `.AGENTS/KNOWLEDGE_BASE.md`
5. **Include Co-Authored-By trailer** in commit messages:
   ```
   Co-Authored-By: Assistant Name <assistant@example.com>
   ```

## Project Structure

### Key Directories

```
leanstore/
├── include/leanstore/          # Public headers
│   ├── base/                   # Foundation utilities
│   ├── buffer/                 # Buffer manager
│   ├── btree/                  # B-tree implementation
│   ├── tx/                     # Transactions and MVCC
│   ├── coro/                   # Coroutine support
│   └── c/                      # C API headers
├── src/                        # Implementation
│   ├── buffer/                 # Buffer manager
│   ├── btree/                  # B-tree
│   ├── tx/                     # Transaction system
│   ├── coro/                   # Coroutine runtime
│   └── utils/                  # Utilities
├── tests/                      # Test suite (mirrors src/)
├── examples/                   # Standalone examples
│   ├── c/                      # C API examples
│   └── cpp/                    # C++ API examples
└── docs/                       # Documentation
```

### Important Files

| File | Purpose |
|------|---------|
| `CMakeLists.txt` | Root CMake configuration |
| `CMakePresets.json` | Build presets for common configurations |
| `vcpkg.json` | Dependency manifest for vcpkg |
| `.clang-format` | Code formatting rules |
| `.clang-tidy` | Static analysis configuration |
| `CONTRIBUTING.md` | Contribution guidelines |
| `AGENTS.md` | AI assistant workflow |

### Architecture Layers

1. **IO layer** – Async Disk I/O
2. **Buffer & logging (WAL)** – Page buffer management and write-ahead logging
3. **B+ tree** – Core index structure implementation
4. **Transaction** – Transaction management and concurrency control
5. **API layer** – Key-value and table interfaces
6. **Foundation libraries** – Infrastructure shared across all layers

## Dependency Management

### vcpkg Integration

LeanStore uses vcpkg for dependency management. The `vcpkg.json` manifest specifies all dependencies:

```json
{
  "dependencies": [
    "spdlog",
    "crc32c",
    "rapidjson",
    "cpptrace",
    "cpp-httplib",
    "boost-context",
    "benchmark",
    "gperftools"
  ]
}
```

### Public vs Private Dependencies

- **Public dependencies** (appear in headers):
  - `Threads::Threads` – C++ threading support
  - `libaio::libaio` – Asynchronous I/O (system library)

- **Private dependencies** (implementation only):
  - `spdlog`, `Crc32c`, `rapidjson`, `cpptrace`, `httplib`, `Boost::context`
  - Automatically resolved via `find_package(leanstore)`

### Adding New Dependencies

1. **Public dependency** (appears in headers):
   ```cmake
   target_link_libraries(leanstore PUBLIC <dependency>)
   # Add to leanstoreConfig.cmake.in
   ```

2. **Private dependency** (implementation only):
   ```cmake
   target_link_libraries(leanstore PRIVATE <dependency>)
   # Add to vcpkg.json if not already present
   ```

3. **Header-only dependency**:
   ```cmake
   target_link_libraries(leanstore PRIVATE <dependency>)
   # Add find_dependency in leanstoreConfig.cmake.in if the dependency
   # contributes usage requirements (include directories, compile definitions)
   # or appears in public headers.
   ```

   Header-only dependencies (e.g., `spdlog`, `RapidJSON`) do not produce linkable symbols and are instantiated into LeanStore's object files only where used. However, they may still need to be exposed via CMake exports if they contribute usage requirements such as include directories, compile definitions, or appear in LeanStore's public headers. In such cases, downstream projects must have these dependencies available at configure/compile time, even though no additional linking is required.

## Troubleshooting

### Common Build Issues

| Issue | Solution |
|-------|----------|
| "libaio not found" | Install `libaio-dev` (Ubuntu) or use vcpkg's `libaio` port |
| C++23 compilation errors | Use GCC ≥13, Clang ≥16, or MSVC ≥19.34 |
| Undefined references to libaio | Ensure `find_package(leanstore)` succeeded and `CMAKE_PREFIX_PATH` includes vcpkg |
| Coverage flags causing linking errors | Use `release_coro` preset for distribution (debug_coro adds `--coverage`) |
| Profiling doesn't work with shared libraries | Build with `BUILD_SHARED_LIBS=OFF` for profiling support |

### Debugging Tips

1. **Enable debug logging**:
   ```cpp
   #include <leanstore/base/log.hpp>
   leanstore::Log::Info("Debug message: {}", value);
   ```

2. **Run debug validation quickly** (debug_coro preset):
   ```bash
   cmake --preset debug_coro
   cmake --build build/debug_coro
   ctest --test-dir build/debug_coro
   ```

3. **Generate compile_commands.json**:
   ```bash
   cmake --preset debug_coro -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
   ```

### Performance Profiling

1. **Build with profiling support**:
   ```bash
   cmake --preset release_coro -DLEAN_ENABLE_PROFILING=ON
   cmake --build build/release_coro --target leanstore
   ```

2. **Run with gperftools**:
   ```bash
   CPUPROFILE=/tmp/prof.out ./build/release_coro/tools/ycsb/ycsb
   pprof --text ./build/release_coro/tools/ycsb/ycsb /tmp/prof.out
   ```

## Further Reading

- [CMake Integration Guide](cmake_integration_guide.md) – Detailed CMake usage and dependency management
- [Design Documents](./design/) – Architecture and design decisions
- [API Reference](../include/leanstore/) – Public header documentation

## Getting Help

- **Slack workspace**: [Join Slack](https://join.slack.com/t/leanstoreworkspace/shared_invite/zt-2o69igywh-yTheoWxjYnD5j3bAFN34Qg)
- **GitHub Issues**: [Report bugs or request features](https://github.com/zz-jason/leanstore/issues)
- **Pull Requests**: [Contribute code](https://github.com/zz-jason/leanstore/pulls)

---

*Last updated: January 2026*
