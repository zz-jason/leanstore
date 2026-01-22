# C Examples

This directory contains a C example that demonstrates how to use the LeanStore library via its C API.

## Prerequisites

- LeanStore built with `release_coro` preset (recommended) or `debug_coro` (development)
- Same vcpkg toolchain used to build LeanStore

## 1. Build and Install LeanStore

```bash
cd $LEANSTORE_HOME
cmake --preset release_coro
cmake --build build/release_coro -j $(nproc)
cmake --install build/release_coro --prefix ./dist/release_coro
```

The library will be installed to `$LEANSTORE_HOME/dist/release_coro`.

## 2. Build the C Example

```bash
cd $LEANSTORE_HOME/examples/c

# Clean previous build
rm -rf build

# Configure with CMAKE_PREFIX_PATH pointing to LeanStore installation and vcpkg
CMAKE_PREFIX_PATH="../../dist/release_coro:../../build/release_coro/vcpkg_installed/x64-linux" \
cmake -B build -S .

# Build the example
cmake --build build

# Run the example
./build/kv_basic_example
```

### Using a Custom Installation Directory

If LeanStore is installed to a different location (e.g., system directory or custom prefix):

```bash
cd $LEANSTORE_HOME/examples/c
rm -rf build

# Set CMAKE_PREFIX_PATH to include both LeanStore and vcpkg installation directories
cmake -B build -S . \
  -DCMAKE_PREFIX_PATH="/path/to/leanstore/install;/path/to/vcpkg/installed/x64-linux"

cmake --build build
```

## How It Works

- The `CMakeLists.txt` declares a pure C project (`project(leanstore-examples C)`) and enables C++ language support only for linking (`enable_language(CXX)`).
- It uses `find_package(leanstore CONFIG REQUIRED)` to locate the installed LeanStore package.
- The C example links to `leanstore::leanstore` and the C++ standard library (`stdc++`).
- All transitive dependencies (Boost, spdlog, rapidjson, etc.) are resolved automatically through LeanStore's CMake package configuration.

## Notes

- For development builds, you can use the `debug_coro` preset, but note that it adds `--coverage` flags that may affect downstream linking.
- The C example uses the C API defined in `include/leanstore/c/leanstore.h`.
- Ensure your compiler supports C11 (for the example) and C++23 (for the LeanStore library).

