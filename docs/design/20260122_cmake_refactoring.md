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
