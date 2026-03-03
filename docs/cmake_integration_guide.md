# LeanStore CMake Integration Guide

This document summarizes the CMake refactoring work done for LeanStore, detailing the design principles, decisions made, problems encountered, and solutions implemented. It serves as a reference for maintainers and downstream users.

## Table of Contents

- [Using LeanStore in Downstream Projects](#using-leanstore-in-downstream-projects)
- [Design Principles](#design-principles)
- [Key Changes and Rationale](#key-changes-and-rationale)
- [Library Type and Dependencies](#library-type-and-dependencies)
- [C++ Version Requirements](#c-version-requirements)
- [Integration Strategies by C++ Standard](#integration-strategies-by-c-standard)
- [Static Library Dependency Exposure](#static-library-dependency-exposure)
- [Downstream Dependency Management Strategies](#downstream-dependency-management-strategies)
- [Common Issues and Solutions](#common-issues-and-solutions)
- [Migration Guide](#migration-guide)
- [Future Considerations](#future-considerations)
- [References](#references)


## Using LeanStore in Downstream Projects

### Method 1: find_package (Recommended for Installed Libraries)

1. Install LeanStore:
   ```bash
   cmake --preset debug_coro
   cmake --build build/debug_coro -j $(nproc)
   cmake --install build/debug_coro --prefix /path/to/install
   ```

2. In your CMakeLists.txt:
   ```cmake
   cmake_minimum_required(VERSION 3.25)
   project(my_project CXX)
   
   set(CMAKE_CXX_STANDARD 23)
   
   # Set CMAKE_PREFIX_PATH to include LeanStore installation
   list(APPEND CMAKE_PREFIX_PATH "/path/to/install")
   # If using vcpkg, also include vcpkg installation directory
   list(APPEND CMAKE_PREFIX_PATH "/path/to/vcpkg/installed/x64-linux")
   
   find_package(leanstore CONFIG REQUIRED)
   
   add_executable(my_target main.cpp)
   target_link_libraries(my_target PRIVATE leanstore::leanstore)
   ```

3. Configure your project:
   ```bash
   cmake -B build -DCMAKE_PREFIX_PATH="/path/to/install;/path/to/vcpkg/installed/x64-linux"
   ```

### Method 2: FetchContent (For Direct Source Integration)

```cmake
include(FetchContent)

FetchContent_Declare(
  leanstore
  GIT_REPOSITORY https://github.com/zz-jason/leanstore.git
  GIT_TAG main
)

FetchContent_MakeAvailable(leanstore)

add_executable(my_target main.cpp)
target_link_libraries(my_target PRIVATE leanstore::leanstore)
```

### Method 3: pkg-config (Legacy Support)

```bash
# Build and install LeanStore first
pkg-config --cflags --libs leanstore
```

```cmake
find_package(PkgConfig REQUIRED)
pkg_check_modules(LEANSTORE REQUIRED leanstore)

add_executable(my_target main.cpp)
target_include_directories(my_target PRIVATE ${LEANSTORE_INCLUDE_DIRS})
target_link_libraries(my_target PRIVATE ${LEANSTORE_LIBRARIES})
```


## Design Principles

The CMake refactoring follows Modern CMake best practices with these core principles:

1. **Separation of Build and Install Interfaces**
   - Compiler warnings, sanitizers, and optimization flags (`-Werror`, `-Wall`, `-fsanitize`) are wrapped in `$<BUILD_INTERFACE:...>` to prevent pollution of downstream projects.
   - Installation exports only the minimal required dependencies and definitions.

2. **Explicit Source Management**
   - Replaced `file(GLOB_RECURSE)` with explicit source file lists for deterministic builds and automatic dependency tracking.

3. **Proper Dependency Scoping**
   - Public dependencies (required by library headers) are declared `PUBLIC`.
   - Private dependencies (used only in implementation) are declared `PRIVATE`.
   - Header-only libraries that don't appear in public headers are kept private.

4. **Portable Binary Generation**
   - Added `LEAN_PORTABLE` option (default OFF) to control `-march=native` usage.
   - When `LEAN_PORTABLE=OFF`, architecture-specific optimizations (AVX2, CX16) are enabled for x86_64.

5. **Complete Package Configuration**
   - Generated `leanstoreConfig.cmake`, `leanstoreConfigVersion.cmake`, and `leanstoreTargets.cmake` for easy integration via `find_package()`.
   - Provided both CMake config-file and pkg-config (`leanstore.pc`) support.

## Key Changes and Rationale

### 1. Project Versioning
- Added `VERSION 1.0.0` to the root `project()` call.
- Enables semantic versioning and compatibility checks via `find_package(leanstore 1.0 COMPATIBILITY SameMajorVersion)`.

### 2. Build Interface Isolation
- **Problem**: Strict compiler flags (`-Werror`, `-Wall`) were leaking to consumers, causing unnecessary build failures.
- **Solution**: Wrapped all warning/error flags in `$<BUILD_INTERFACE:...>` using the `lean_project_options` interface library.

### 3. Dependency Analysis and Cleanup
- **Identified header-only libraries**: `rapidjson`, `spdlog`, `Crc32c`, `cpptrace`, `httplib`.
  - These are private dependencies because they don't appear in public headers.
  - Removed `rapidjson` from package configuration (no `find_dependency` needed).
  - Kept others in build dependencies but not exported.


### 4. Public Dependency: libaio
- **Problem**: Public headers (`include/leanstore/io/async_io.hpp`) include `<libaio.h>`, making `libaio` a public dependency.
- **Previous state**: Linked as `PRIVATE`, causing downstream linking errors.
- **Solution**: Moved to `PUBLIC` in `target_link_libraries` and added detection logic in `leanstoreConfig.cmake.in`.

### 5. Package Configuration Generation
- Generated complete CMake package files that:
  - Export `leanstore::leanstore` target with namespace.
  - Automatically find public dependencies (`Threads` and `libaio`).
  - Handle both pkg-config and fallback detection for `libaio`.
  - Include version compatibility checking.

### 6. Examples Modernization
- Updated `examples/cpp` and `examples/c` to use `find_package(leanstore CONFIG)`.
- Replaced manual `-DLEANSTORE_INCLUDE_DIR` and `-DLEANSTORE_LIBRARY_DIR` with `CMAKE_PREFIX_PATH`.
- Added proper C++ standard library linking for C examples.

## Library Type and Dependencies

### Static vs. Shared Library
- **Default**: Static library (`BUILD_SHARED_LIBS=OFF`)
- **Rationale**:
  - Static linking simplifies distribution and avoids runtime dependency issues.
  - Profiling with gperftools (`LEAN_ENABLE_PROFILING`) only works with static libraries.
  - Shared libraries can be built by setting `BUILD_SHARED_LIBS=ON`, but profiling will be disabled.

### Public Dependencies (Required by Downstream Users)
These dependencies must be available in the consumer's environment:

| Dependency | Purpose | How to Provide |
|------------|---------|----------------|
| `Threads::Threads` | C++ threading support (`std::thread`) | CMake's built-in `find_package(Threads)` |
| `libaio::libaio` | Asynchronous I/O for NVMe optimization | System package (`libaio-dev` on Ubuntu) or vcpkg |

### Private Dependencies (Internal Only)
These are handled automatically during LeanStore build:

- `spdlog` (header-only) - Logging
- `Crc32c` - CRC32 calculations
- `rapidjson` (header-only) - JSON parsing
- `cpptrace` (header-only) - Stack traces
- `httplib` (header-only) - HTTP server for metrics
- `Boost::context` - Coroutine stack management (hidden by pimpl)
- `gperftools` - Profiling (optional, only when `LEAN_ENABLE_PROFILING=ON`)

## C++ Version Requirements

- **Minimum C++ Standard**: C++23
- **Set via**: `target_compile_features(leanstore PUBLIC cxx_std_23)`
- **Rationale**:
  - Modern C++ features (coroutines, ranges, concepts) are used extensively.
  - C++23 provides better coroutine support and performance optimizations.
- **Consumer Requirements**: Downstream projects must use at least C++23 when linking with LeanStore.


## Integration Strategies by C++ Standard

| C++ Standard | Recommended Integration Strategy | Key Considerations | Section |
|--------------|----------------------------------|---------------------|---------|
| C++11/C++17  | Use C API with C++23 library linkage | Stable C interface, no direct C++ dependency, requires C++ standard library for linking | [Older C++ Standards](#older-c-standards-c11c17) |
| C++23        | Use C++ API directly | Full feature access, but API may evolve, better performance and integration | [Newer C++ Standards](#newer-c-standards-c23) |

### Older C++ Standards (C++11/C++17)

#### When to Use This Strategy

Choose this approach if your project:
- Uses C++11 or C++17 due to legacy constraints
- Requires stable API guarantees (C interface is stable)
- Needs to avoid direct dependency on evolving C++ API
- Can accept the overhead of C wrapper calls

#### Step-by-Step Integration

1. **Install LeanStore** following the [Using LeanStore in Downstream Projects](#using-leanstore-in-downstream-projects) guide.

2. **Configure Your CMakeLists.txt**:
   ```cmake
   cmake_minimum_required(VERSION 3.25)
   project(my_project C)
   
   # Set C standard (optional)
   set(CMAKE_C_STANDARD 11)
   
   # Enable C++ language support for linking to LeanStore's C++23 library
   enable_language(CXX)
   set(CMAKE_CXX_STANDARD 23)
   
   # Find leanstore package
   find_package(leanstore CONFIG REQUIRED)
   
   add_executable(my_target main.c)
   target_link_libraries(my_target PRIVATE
       leanstore::leanstore
       stdc++        # C++ standard library
       m             # Math library (if needed)
   )
   ```

3. **Use C Headers** in your code:
   ```c
   #include "leanstore/c/leanstore.h"
   #include "leanstore/c/types.h"
   ```

4. **Link with C++ Standard Library**: Since LeanStore is built with C++23, your executable must link against the C++ standard library (`stdc++` on GCC/Clang, `c++` on macOS).

#### Key Considerations

- **ABI Compatibility**: While the C interface is stable, linking against a C++23 library requires compiler ABI compatibility. Use the same compiler family (GCC, Clang) and similar versions.
- **Dependency Management**: All LeanStore dependencies (Boost, spdlog, etc.) are handled automatically through `find_package`.
- **Performance**: C API calls have minimal overhead compared to direct C++ usage.
- **Example**: See `examples/c/kv_basic_example.c` for a complete working example.

### Newer C++ Standards (C++23)

#### When to Use This Strategy

Choose this approach if your project:
- Already uses C++23 or can upgrade to it
- Needs access to the full C++ API (subject to evolution)
- Wants optimal performance with direct method calls
- Can tolerate API changes in future LeanStore versions

#### Step-by-Step Integration

1. **Install LeanStore** following the [Using LeanStore in Downstream Projects](#using-leanstore-in-downstream-projects) guide.

2. **Configure Your CMakeLists.txt**:
   ```cmake
   cmake_minimum_required(VERSION 3.25)
   project(my_project CXX)
   
   set(CMAKE_CXX_STANDARD 23)
   
   # Find leanstore package
   find_package(leanstore CONFIG REQUIRED)
   
   add_executable(my_target main.cpp)
   target_link_libraries(my_target PRIVATE leanstore::leanstore)
   ```

3. **Use C++ Headers** in your code:
   ```cpp
   #include <leanstore/leanstore.hpp>
   // Or specific headers as needed
   ```

4. **Link Dependencies**: The `leanstore::leanstore` target automatically propagates all necessary dependencies.

#### Key Considerations

- **API Stability**: The C++ API is still evolving. Expect breaking changes between major versions.
- **Feature Access**: Full access to LeanStore's C++ features including coroutines, ranges, and advanced configuration.
- **Compiler Requirements**: Requires C++23 compatible compiler (GCC ≥13, Clang ≥16, MSVC ≥19.34).
- **Example**: See `examples/cpp` directory for C++ examples.

## Static Library Dependency Exposure

When building LeanStore as a static library (`BUILD_SHARED_LIBS=OFF`), downstream projects need to link against LeanStore's private dependencies. This is because:

1. **Static Linking Semantics**: Static libraries are archives of object files that don't embed their dependencies' code. The linker must resolve all symbols when creating the final executable, requiring access to dependency libraries.
2. **CMake Export Behavior**: When CMake exports a static library target, it includes all link dependencies (both `PUBLIC` and `PRIVATE`) in the exported target's `INTERFACE_LINK_LIBRARIES` to ensure correct linking downstream.
3. **Private Dependencies with Linkable Symbols**: Dependencies like `Boost::context` provide symbols needed at link time. Even though they're marked `PRIVATE` (not exposed in headers), they must be available during final linking.
4. **Header-only Dependencies**: Header-only dependencies (e.g., `spdlog`, `rapidjson`, `cpptrace`, `httplib`) do not produce linkable symbols and are instantiated into LeanStore's object files only where used. However, they may still need to be exposed via CMake exports if they contribute usage requirements such as include directories, compile definitions, or appear in LeanStore's public headers. In such cases, downstream projects must have these dependencies available at configure/compile time, even though no additional linking is required.

### Solution Implemented

The `leanstoreConfig.cmake.in` file now includes `find_dependency()` calls for all private dependencies. This ensures:

- Downstream projects automatically get required include directories and link libraries
- No manual dependency management is needed by consumers
- Consistent behavior whether using vcpkg or system packages
- Private dependencies remain conceptually private—consumers don't need to know about them, but CMake handles them transparently

### Alternative Approaches Considered

1. **Bundle dependencies into the static library**: Could use `-Wl,--whole-archive` or similar to embed dependency code, but this increases library size and may cause symbol conflicts.
2. **Use `$<BUILD_INTERFACE>` and `$<INSTALL_INTERFACE>`**: Could conditionally include dependencies only during build, but this breaks downstream linking for static libraries.
3. **Make all dependencies PUBLIC**: Would expose implementation details and force unnecessary constraints on consumers.

The current approach provides the best balance of encapsulation and usability for a high-performance database library.

## Downstream Dependency Management Strategies

When distributing LeanStore as a static library, downstream users need access to the library's dependencies for linking. There are several strategies to handle this:

### Strategy 1: Use vcpkg Manifest Mode (Recommended)

This is the simplest approach that ensures version compatibility:

1. **Copy the vcpkg.json manifest**: Downstream users should copy the `vcpkg.json` from LeanStore's root directory into their project.

2. **Configure with vcpkg toolchain**:
   ```bash
   cmake -B build -DCMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
   ```

3. **Vcpkg automatically handles dependencies**: vcpkg will install all required dependencies (Boost, spdlog, Crc32c, etc.) in a local directory and make them available to CMake.

4. **Install LeanStore**: After building LeanStore, install it to a directory and add that directory to `CMAKE_PREFIX_PATH`.

**Advantages**:
- Automatic dependency resolution
- Version locking via `vcpkg.json`
- Consistent builds across environments
- No manual dependency management

**Example downstream CMakeLists.txt**:
```cmake
cmake_minimum_required(VERSION 3.25)
project(my_app CXX)

set(CMAKE_CXX_STANDARD 23)

# Use vcpkg toolchain (set via command line or preset)
# -DCMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake

find_package(leanstore CONFIG REQUIRED)

add_executable(my_app main.cpp)
target_link_libraries(my_app PRIVATE leanstore::leanstore)
```

### Strategy 2: Manual Dependency Installation with CMAKE_PREFIX_PATH

If dependencies are already installed on the system (via system package manager or manual build):

1. **Install dependencies manually**:
   ```bash
   # Ubuntu example
   sudo apt install libboost-context-dev libspdlog-dev libcrc32c-dev rapidjson-dev
   # Or install via vcpkg without manifest mode
   vcpkg install boost-context spdlog crc32c rapidjson cpptrace cpp-httplib
   ```

2. **Set CMAKE_PREFIX_PATH to include both LeanStore installation and dependency locations**:
   ```bash
   cmake -B build -DCMAKE_PREFIX_PATH="/path/to/leanstore/install;/path/to/vcpkg/installed/x64-linux"
   ```

3. **The `find_dependency()` calls in `leanstoreConfig.cmake` will locate dependencies** if their `*Config.cmake` files are in directories covered by `CMAKE_PREFIX_PATH`.

**Advantages**:
- More control over dependency versions
- Can use system packages where available
- Works with existing vcpkg installations

**Disadvantages**:
- Manual dependency management
- Version mismatches possible

### Strategy 3: Building LeanStore as a Shared Library

For scenarios where dependency management is challenging:

1. **Build LeanStore as a shared library**:
   ```bash
   cmake --preset release_coro -DBUILD_SHARED_LIBS=ON
   cmake --build build/release_coro
   cmake --install build/release_coro --prefix /path/to/install
   ```

2. **Shared libraries embed their dependencies** at build time, so downstream users only need runtime dependencies (`libaio`, `libstdc++`, etc.).

3. **Note**: Profiling with gperftools (`LEAN_ENABLE_PROFILING`) does not work with shared libraries.

**Advantages**:
- Simplified downstream linking (only runtime dependencies needed)
- Smaller downstream binary size (shared library code not duplicated)

**Disadvantages**:
- Runtime dependency on `libleanstore.so`
- Profiling disabled
- Potential version conflicts if multiple applications use different LeanStore versions

### Comparison Summary

| Strategy | Dependency Management | Version Control | Complexity | Recommended For |
|----------|----------------------|----------------|------------|-----------------|
| vcpkg Manifest | Automatic via vcpkg | Locked by `vcpkg.json` | Low | **Most users**, CI/CD pipelines |
| Manual with CMAKE_PREFIX_PATH | Manual installation | Manual control | Medium | Advanced users, system packaging |
| Shared Library | Runtime only | Linked at build time | Low | Quick prototyping, distribution |

### Best Practice Recommendations

1. **For most users**: Use Strategy 1 (vcpkg manifest mode). This provides the best balance of ease-of-use and reproducibility.

2. **For package maintainers**: Use Strategy 2 with system packages where possible, ensuring to document all dependencies in package metadata.

3. **For embedded/distribution scenarios**: Consider Strategy 3 (shared libraries) if profiling is not required and you want to minimize downstream build complexity.

4. **Always document** which strategy your project uses to help downstream users configure their environment correctly.

## Common Issues and Solutions

### 1. "libaio not found" During Configuration
- **Cause**: The system lacks libaio development packages.
- **Solution**: Install `libaio-dev` (Ubuntu) or `libaio-devel` (Fedora), or use vcpkg.

### 2. Compilation Errors About Missing C++23 Features
- **Cause**: Compiler doesn't support C++23 or project not configured for C++23.
- **Solution**: Use GCC ≥13, Clang ≥16, or MSVC ≥19.34. Ensure `set(CMAKE_CXX_STANDARD 23)`.

### 3. Linker Errors with Undefined References to libaio
- **Cause**: Consumer project not linking to libaio.
- **Solution**: The `leanstore::leanstore` target should automatically propagate libaio dependency. Verify `find_package(leanstore)` succeeded.

### 4. Werror Causing Build Failures in Consumer Projects
- **Cause**: Previous versions leaked `-Werror` to consumers.
- **Solution**: After refactoring, `-Werror` is confined to `BUILD_INTERFACE`. Update to latest LeanStore.

### 5. Architecture-Specific Binaries Fail on Different Machines
- **Cause**: Default `-march=native` creates non-portable binaries.
- **Solution**: Build with `-DLEAN_PORTABLE=ON` for distribution.

### 6. Profiling Doesn't Work with Shared Libraries
- **Cause**: gperftools has known issues with shared libraries.
- **Solution**: Use static libraries (`BUILD_SHARED_LIBS=OFF`) when profiling needed.

## Migration Guide

### For Existing Users of LeanStore

If you previously used manual include/library paths or pkg-config:

1. **Update Integration**:
   - Replace manual `include_directories()` and `link_directories()` with `find_package(leanstore)`.
   - Replace `target_link_libraries(my_target leanstore)` with `target_link_libraries(my_target PRIVATE leanstore::leanstore)`.

2. **Update Build Configuration**:
   - Remove `-DLEANSTORE_INCLUDE_DIR` and `-DLEANSTORE_LIBRARY_DIR`.
   - Use `-DCMAKE_PREFIX_PATH` instead.

3. **Ensure Dependencies**:
   - Install `libaio` development package.
   - Use C++23 compiler.

### For Contributors to LeanStore

1. **Adding New Source Files**:
   - Add to explicit list in `src/CMakeLists.txt` (`LEAN_SOURCES`).
   - Do not use `file(GLOB_RECURSE)`.

2. **Adding New Dependencies**:
   - **Public dependency** (appears in headers): Add to `PUBLIC` in `target_link_libraries` and `leanstoreConfig.cmake.in`.
   - **Private dependency** (implementation only): Add to `PRIVATE` in `target_link_libraries`.
   - **Header-only dependency**: Keep in `PRIVATE`; no need for `find_dependency` in config.

3. **Modifying Compiler Flags**:
   - Use `lean_project_options` interface library.
   - Wrap flags in `$<BUILD_INTERFACE:...>` if they shouldn't propagate to consumers.

## Future Considerations

1. **Vcpkg Manifest Mode**: Ensure compatibility with vcpkg's manifest mode for easier dependency resolution.

2. **Cross-Compilation Support**: Test and document cross-compilation scenarios.

3. **Windows Support**: Currently UNIX-only; potential Windows port would require alternative to libaio.

4. **Package Managers**: Consider adding support for Conan, Spack, or system package managers.

5. **Binary Distribution**: Provide pre-built binaries for common configurations via GitHub Releases.

## References

- [Modern CMake Best Practices](https://cliutils.gitlab.io/modern-cmake/)
- [CMake Packaging Guide](https://cmake.org/cmake/help/latest/guide/importing-exporting/)
- [vcpkg Documentation](https://vcpkg.io/en/docs/)
- [Design Document](./design/20260122_cmake_refactoring.md)