[![CI][9]][10]
[![codecov][3]][4]
[![Join Slack][11]][12]

# LeanStore

LeanStore is a larger-than-memory database, optimized for NVMe SSD and
multi-core CPU, achieves performance close to in-memory systems without having
to keep all data in memory.

<div align='center'>
  <img align="center" width=80% src="./docs/images/Architecture.jpg" />
</div>

## Getting started

[vcpkg][13] is used to manage third-party libraries, please install it before
building the project. It's highly recommended to develop the project inside a
docker container, which can be built from this [Dockerfile][5]:

```sh
cmake --preset debug_coro
cmake --build build/debug_coro -j `nproc`
ctest --test-dir build/debug_coro
```

## Integration Guide

LeanStore is built as a **static library** by default, providing better performance and avoiding runtime dependency issues. The library requires **C++23** and has both public and private dependencies.

### Library Type
- **Default**: Static library (`BUILD_SHARED_LIBS=OFF`)
- **Shared libraries**: Can be built with `BUILD_SHARED_LIBS=ON`, but profiling support will be disabled (gperftools compatibility).

### Public Dependencies (Required by Downstream Users)
These must be available in the consumer's environment:

| Dependency | Purpose | How to Provide |
|------------|---------|----------------|
| `Threads::Threads` | C++ threading support | CMake's built-in `find_package(Threads)` |
| `libaio::libaio` | Asynchronous I/O for NVMe optimization | System package (`libaio-dev` on Ubuntu) or vcpkg |

### Private Dependencies (Handled Automatically)
- `spdlog` (header-only) - Logging
- `Crc32c` - CRC32 calculations
- `rapidjson` (header-only) - JSON parsing
- `cpptrace` (header-only) - Stack traces
- `httplib` (header-only) - HTTP server for metrics
- `Boost::context` - Coroutine stack management

### C++ Version Requirements
- **Minimum**: C++23
- **Set via**: `target_compile_features(leanstore PUBLIC cxx_std_23)`
- Downstream projects must use at least C++23 when linking with LeanStore.

### Using LeanStore in Your Project

#### Method 1: find_package (Recommended for Installed Libraries)
```cmake
cmake_minimum_required(VERSION 3.25)
project(my_project CXX)

set(CMAKE_CXX_STANDARD 23)

# Set CMAKE_PREFIX_PATH to include LeanStore installation
list(APPEND CMAKE_PREFIX_PATH "/path/to/leanstore/install")

find_package(leanstore CONFIG REQUIRED)

add_executable(my_target main.cpp)
target_link_libraries(my_target PRIVATE leanstore::leanstore)
```

#### Method 2: FetchContent (For Direct Source Integration)
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

### Build Presets
LeanStore provides CMake presets for common configurations:

| Preset | Description | Library Type |
|--------|-------------|--------------|
| `debug_coro` | Debug build with coroutine support | Static |
| `release_coro` | Release build with coroutine support | Static |
| `debug_tsan` | Debug build with thread sanitizer | Static |
| `release_thread` | Release build without coroutines | Static |

All presets now build **static libraries** by default. Set `BUILD_SHARED_LIBS=ON` explicitly if shared libraries are needed.

### Examples
See `examples/cpp` and `examples/c` for complete usage examples.

## Contributing

Contributions are welcomed and greatly appreciated! See [CONTRIBUTING.md][6] for
setting up development environment and contributing.

You can also join the [slack workspace][12] to discuss any questions or ideas.

## License

LeanStore is under the [MIT License][7].

## Acknowledgments

Thanks for the LeanStore authors and the [leanstore/leanstore][8] project.

[3]: https://codecov.io/github/zz-jason/leanstore/graph/badge.svg?token=MBS1H361JJ
[4]: https://codecov.io/github/zz-jason/leanstore
[5]: ./docker/Dockerfile
[6]: ./CONTRIBUTING.md
[7]: ./LICENSE
[8]: http://github.com/leanstore/leanstore
[9]: https://github.com/zz-jason/leanstore/actions/workflows/ci.yml/badge.svg
[10]: https://github.com/zz-jason/leanstore/actions/workflows/ci.yml
[11]: https://img.shields.io/badge/Join-Slack-blue.svg?logo=slack
[12]: https://join.slack.com/t/leanstoreworkspace/shared_invite/zt-2o69igywh-yTheoWxjYnD5j3bAFN34Qg
[13]: https://github.com/microsoft/vcpkg
