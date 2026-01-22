# CMake Refactoring for Library Best Practices

- Co-Authored-By: Gemini

## Summary

This proposal outlines the refactoring of the `leanstore` CMake build system to align with Modern CMake best practices. The primary goal is to transform `leanstore` from a "build-from-source" local project into a proper library that can be easily integrated by downstream users via `find_package(leanstore)` or `FetchContent`.

## Background

The current build system works well for local development but presents significant hurdles for integration:

1.  **Integration Difficulty**: Downstream projects cannot use `find_package(leanstore CONFIG)`. They must manually manage include paths and dependencies, or rely on fragile `pkg-config` files.
2.  **Interface Pollution**: Strict compiler flags (e.g., `-Werror`, `-Wall`) and sanitizer options are leaked to consumers via the `INTERFACE` of `lean_project_options`. This causes downstream builds to fail if they don't adhere to `leanstore`'s internal strictness.
3.  **Portability**: The build scripts hardcode `-march=native`, making the resulting binaries non-portable across different machines (even with the same architecture).
4.  **Non-Standard Practices**: Usage of `GLOB_RECURSE` prevents CMake from detecting source file additions without a manual re-configure.

## Detailed Design

### 1. Project Versioning and Standard Layout
Update the root `project()` call to include version information (e.g., `VERSION 1.0.0`). This enables `find_package(leanstore 1.0 COMPATIBILITY SameMajorVersion)`.

### 2. Isolation of Build Options (`BUILD_INTERFACE` vs `INSTALL_INTERFACE`)
Refactor `lean_project_options` to strictly separate flags meant for building `leanstore` itself vs. flags required by consumers.
*   **Compiler Warnings/Errors**: Wrap in `$<BUILD_INTERFACE: ... >`. Consumers should not inherit `-Werror`.
*   **Sanitizers/Profilers**: Ensure these are not forced upon consumers unless explicitly requested via specific build types.

### 3. Portable Hardware Optimization
Replace the unconditional `-march=native` with an option `LEAN_PORTABLE`.
*   **Default**: `OFF` (current behavior, optimized for host).
*   **ON**: Generates generic binaries suitable for distribution.

### 4. Robust Source Management
Replace `file(GLOB_RECURSE ...)` in `src/CMakeLists.txt` with an explicit list of source files.
*   **Rationale**: Ensures deterministic builds and allows CMake to automatically detect when a file is added/removed during incremental builds.

### 5. Package Configuration Generation
Implement the standard CMake package workflow:
*   **Targets Export**: Export `leanstore` targets to `leanstoreTargets.cmake`.
*   **Config File**: Generate `leanstoreConfig.cmake` (and `Version.cmake`) using `CMakePackageConfigHelpers`.
*   **Dependency Management**: `leanstoreConfig.cmake` will use `find_dependency()` to transparently look up required public dependencies (e.g., `Threads`) so users don't have to.

### 6. Namespace Safety
Ensure the library is exported as `leanstore::leanstore` to prevent target name collisions in large projects.

### 7. Refactored Directory Structure
*   **`src/CMakeLists.txt`**: Focus purely on library definition.
*   **`CMakeLists.txt` (Root)**: Handle global options, testing, and tool configuration.
*   **`cmake/`**: Directory for helper scripts and `.in` templates.

## Implementation Plan

1.  **Create Helper Templates**: Add `cmake/leanstoreConfig.cmake.in`.
2.  **Refactor Root CMake**:
    *   Add `VERSION`.
    *   Add `LEAN_PORTABLE` option.
    *   Move warning flags to `PRIVATE` or `BUILD_INTERFACE` scope.
3.  **Refactor `src/CMakeLists.txt`**:
    *   List sources explicitly.
    *   Add `install(EXPORT ...)` directives.
    *   Generate and install `leanstoreConfig.cmake` and `leanstoreConfigVersion.cmake`.
4.  **Verify**:
    *   Build locally (existing workflow).
    *   Simulate downstream usage by trying to build `examples/cpp` using `find_package(leanstore)` against the installed directory.

## Alternative Designs

*   **Keep `GLOB_RECURSE`**: We could keep globbing but use `CONFIGURE_DEPENDS` (CMake 3.12+). However, explicit listing is still preferred in the C++ community to avoid "phantom file" issues and to clearly delineate what is part of the library.
*   **ExternalProject for dependencies**: We currently rely on system/vcpkg libraries. We could bundle dependencies (vendoring), but strictly assuming `find_package` works (standard CMake practice) gives users more control.

## Unresolved Questions

*   **Profiling Tools**: The current build disables profiling for shared libraries due to `gperftools` issues. The refactoring will preserve this logic but clarify the warning messages.

## Review Comments

<!--
Review by Crush AI Assistant (2025-01-22)

### Overall Assessment
The design document correctly identifies key CMake best‑practice violations in the current leanstore build system. The proposed refactoring aligns with Modern CMake principles and will significantly improve the library’s usability as a dependency. Below are specific observations and additional considerations.

### 1. Project Versioning (√)
Adding a `VERSION` field to `project()` is essential for package‑manager compatibility and semantic versioning. Should be implemented as described.

### 2. Build‑Interface Isolation (√)
The use of `$<BUILD_INTERFACE:...>` for compiler warnings, sanitizers, and coverage flags is mandatory. Currently `lean_project_options` leaks `-Werror`, `-Wall`, `-Wextra`, and sanitizer flags to consumers, causing unnecessary build failures. Recommend wrapping all warning/error flags and sanitizer options with `$<BUILD_INTERFACE:...>`.

### 3. Portable Hardware Optimization (√)
Introducing `LEAN_PORTABLE` (default OFF) is a pragmatic choice. The current unconditional `-march=native` breaks portability of distributed binaries. Consider also adding a CMake check for AVX2/CX16 support when `LEAN_PORTABLE=OFF` to avoid runtime crashes on older CPUs.

### 4. Explicit Source Lists (√)
Replacing `file(GLOB_RECURSE)` with explicit source lists improves determinism and ensures CMake tracks file additions/deletions automatically. If maintainability is a concern, `CONFIGURE_DEPENDS` (CMake ≥3.12) could be a middle ground, but explicit listing is the community‑preferred standard.

### 5. Package Configuration Generation (⚠)
The existing `cmake/leanstoreConfig.cmake.in` only finds `Threads`. However, public headers (`include/leanstore/...`) include `<libaio.h>`, making `libaio` a **public dependency**. The current `src/CMakeLists.txt` links `aio` as PRIVATE, which will cause downstream linking errors. Two options:
   - Move `aio` to `PUBLIC` in `target_link_libraries` and add `find_dependency(libaio)` in the config file.
   - Keep `aio` PRIVATE but ensure the exported target automatically propagates the `-laio` link flag (requires `INTERFACE_LINK_LIBRARIES`).

Also verify that no other private dependencies (e.g., `spdlog`, `crc32c`, `rapidjson`) are exposed through public headers; if they are, they must be moved to PUBLIC or hidden via pimpl.

### 6. Namespace Safety (√)
`add_library(leanstore::leanstore ALIAS leanstore)` already exists – good.

### 7. Missing Export Logic
The design mentions `install(EXPORT ...)` but the current `src/CMakeLists.txt` lacks it. Ensure the export set includes the `leanstore` target and its public dependencies (`Threads::Threads` and possibly `aio`). Use `install(TARGETS ... EXPORT)` and `install(EXPORT ...)`.

### 8. Additional Recommendations
- **Compiler Feature Detection**: Consider using `target_compile_features(leanstore PUBLIC cxx_std_23)` instead of relying solely on `lean_project_options`.
- **Separate Development Flags**: Create a `leanstore_dev_options` interface library for developer‑only flags (e.g., `-Werror`, sanitizers) that is never exported.
- **Test the Package**: After implementing the config file, test with `examples/cpp` using `find_package(leanstore)` instead of manual include/library paths.
- **vcpkg Integration**: Since the project already uses vcpkg, ensure the generated config file works both with and without vcpkg (i.e., handle `find_dependency` gracefully).

### 9. Implementation Order
1. Add `VERSION` and `LEAN_PORTABLE`.
2. Wrap warning/sanitizer flags with `BUILD_INTERFACE`.
3. Replace `GLOB_RECURSE` with explicit source lists.
4. Fix public dependency handling (`aio`).
5. Add `install(EXPORT ...)` and complete the package‑config generation.
6. Test locally and with the examples.

The refactoring is well‑scoped and should be undertaken as a single cohesive effort to avoid leaving the build in a partially‑exported state.
-->

## Response to Review Comments

I agree with the review assessment. Specifically:

1.  **Project Versioning**: Will be implemented as planned.
2.  **Build-Interface Isolation**: Will wrap all strict flags in `$<BUILD_INTERFACE:...>`.
3.  **Portable Hardware Optimization**: Will implement `LEAN_PORTABLE`.
4.  **Explicit Source Lists**: Will replace `GLOB_RECURSE` with explicit listing.
5.  **Package Configuration & Dependencies**: 
    *   **Ack**: `libaio` is indeed a public dependency. 
    *   **Action**: I will move `aio` from `PRIVATE` to `PUBLIC` in `src/CMakeLists.txt`. This ensures downstream targets inherit the link dependency.
    *   **Config**: I will add a check for `libaio` in `leanstoreConfig.cmake.in` (likely via PkgConfig or simple check) to ensure it is present on the consumer's system.
6.  **Missing Export Logic**: Will add `install(EXPORT leanstoreTargets ...)` to `src/CMakeLists.txt` and include it in the package config.
7.  **Implementation Order**: Will follow the suggested order.

The implementation will now proceed with these refinements.
